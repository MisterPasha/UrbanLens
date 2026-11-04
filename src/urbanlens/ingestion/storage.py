"""Immutable local/S3 artifacts; hashes describe stored (possibly compressed) bytes."""

from __future__ import annotations

import base64
import hashlib
import mimetypes
import os
import tempfile
import zlib
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit
from urllib.request import url2pathname

from urbanlens.ingestion.contracts import Artifact, FailureClass
from urbanlens.ingestion.errors import Conflict, IngestionError

CHUNK = 64 * 1024
MAX_ARTIFACT = 128 * 1024 * 1024


def resolved_path(path: Path) -> Path:
    """Normalize Windows extended-path spelling after resolving links, including races."""
    value = str(path.resolve())
    if os.name == "nt":
        if value.startswith("\\\\?\\UNC\\"):
            value = "\\\\" + value[8:]
        elif value.startswith("\\\\?\\"):
            value = value[4:]
    return Path(value)


def io_path(path: Path) -> Path:
    """Use Windows extended paths only for I/O; metadata retains ordinary file URIs."""
    if os.name != "nt":
        return path
    value = str(path)
    if value.startswith("\\\\?\\"):
        return path
    if value.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + value[2:])
    return Path("\\\\?\\" + value)


def safe_key(key: str) -> str:
    if (
        not key
        or "\\" in key
        or ":" in key
        or key.startswith("/")
        or any(p in {"", ".", ".."} for p in key.split("/"))
        or str(PurePosixPath(key)) != key
    ):
        raise ValueError("Unsafe relative storage key")
    return key


class ByteReader(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


def chunks(stream: ByteReader) -> Iterator[bytes]:
    while data := stream.read(CHUNK):
        yield data


def gzip_chunks(data: Iterator[bytes]) -> Iterator[bytes]:
    compressor = zlib.compressobj(level=6, wbits=31)
    for block in data:
        output = compressor.compress(block)
        if output:
            yield output
    yield compressor.flush()


def checked(data: Iterator[bytes], artifact: Artifact) -> Iterator[bytes]:
    digest, size = hashlib.sha256(), 0
    for block in data:
        size += len(block)
        if size > artifact.byte_count:
            raise IngestionError(FailureClass.INTEGRITY, "artifact-size-mismatch")
        digest.update(block)
        yield block
    if size != artifact.byte_count or digest.hexdigest() != artifact.sha256:
        raise IngestionError(FailureClass.INTEGRITY, "artifact-checksum-mismatch")


class ByteWriter(Protocol):
    def write(self, data: bytes, /) -> int: ...


def copy_hash(data: Iterator[bytes], stream: ByteWriter, limit: int) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    for block in data:
        size += len(block)
        if size > limit:
            raise IngestionError(FailureClass.BUDGET, "artifact-byte-budget")
        digest.update(block)
        stream.write(block)
    return digest.hexdigest(), size


def describe(uri: str, sha: str, size: int, media: str, compression: str) -> Artifact:
    return Artifact.model_validate(
        dict(uri=uri, sha256=sha, byte_count=size, media_type=media, compression=compression)
    )


class LocalArtifactStore:
    def __init__(self, root: Path, *, max_bytes: int = MAX_ARTIFACT) -> None:
        self.root = resolved_path(root)
        io_path(self.root).mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes

    def path(self, key: str) -> Path:
        path = resolved_path(self.root / safe_key(key))
        if not path.is_relative_to(self.root):
            raise ValueError("Storage key escapes configured root")
        return path

    def stat(self, key: str) -> Artifact | None:
        canonical = self.path(key)
        path = io_path(canonical)
        if not path.exists():
            return None
        if path.stat().st_size > self.max_bytes:
            raise IngestionError(FailureClass.BUDGET, "artifact-byte-budget")
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        media, encoding = mimetypes.guess_type(path.name)
        compression = "gzip" if encoding == "gzip" else "zip" if path.suffix == ".zip" else "none"
        return describe(
            canonical.as_uri(),
            digest,
            path.stat().st_size,
            media or "application/octet-stream",
            compression,
        )

    def put_immutable(
        self, key: str, chunks: Iterator[bytes], *, media_type: str, compression: str
    ) -> Artifact:
        canonical = self.path(key)
        path = io_path(canonical)
        describe(canonical.as_uri(), "0" * 64, 0, media_type, compression)
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                digest, size = copy_hash(chunks, stream, self.max_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            result = describe(canonical.as_uri(), digest, size, media_type, compression)
            try:
                os.link(temporary, path)  # Atomic create; never replace another writer's object.
            except FileExistsError:
                try:
                    for _ in self.read_verified(result):
                        pass
                except IngestionError:
                    raise Conflict() from None
            return result
        finally:
            temporary.unlink(missing_ok=True)

    def read_verified(self, artifact: Artifact) -> Iterator[bytes]:
        uri = urlsplit(artifact.uri)
        if uri.scheme != "file":
            raise ValueError("Local store requires a file URI")
        path = resolved_path(Path(url2pathname(uri.path)))
        if not path.is_relative_to(self.root):
            raise ValueError("Artifact is outside the configured root")
        with io_path(path).open("rb") as source:
            yield from checked(chunks(source), artifact)


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


def error_code(error: Exception) -> str:
    response = getattr(error, "response", {})
    return str(response.get("Error", {}).get("Code", ""))


class S3ArtifactStore:
    """Injected SDK client must use bounded timeouts/retries; no credentials in metadata."""

    def __init__(
        self, client: S3Client, bucket: str, prefix: str = "", *, max_bytes: int = MAX_ARTIFACT
    ) -> None:
        self.client, self.bucket, self.max_bytes = client, bucket, max_bytes
        self.prefix = safe_key(prefix) + "/" if prefix else ""
        if not bucket or any(c in bucket for c in "/:@?#"):
            raise ValueError("Invalid bucket")

    def key(self, relative: str) -> str:
        return self.prefix + safe_key(relative)

    def uri(self, key: str) -> str:
        return "s3://" + self.bucket + "/" + quote(key, safe="/-_.~")

    def stat(self, key: str) -> Artifact | None:
        absolute = self.key(key)
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=absolute)
        except Exception as error:
            if error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise IngestionError(FailureClass.NETWORK, "s3-head-failed", retryable=True) from None
        metadata = head.get("Metadata", {})
        return describe(
            self.uri(absolute),
            metadata.get("sha256", ""),
            head["ContentLength"],
            head.get("ContentType", "application/octet-stream"),
            metadata.get("compression", "none"),
        )

    def put_immutable(
        self, key: str, chunks: Iterator[bytes], *, media_type: str, compression: str
    ) -> Artifact:
        absolute = self.key(key)
        describe(self.uri(absolute), "0" * 64, 0, media_type, compression)
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as spool:
            digest, size = copy_hash(chunks, spool, self.max_bytes)
            artifact = describe(self.uri(absolute), digest, size, media_type, compression)
            spool.seek(0)
            try:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=absolute,
                    Body=spool,
                    ContentLength=size,
                    ContentType=media_type,
                    Metadata={"sha256": digest, "compression": compression},
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
                    IfNoneMatch="*",
                )
            except Exception as error:
                if error_code(error) in {"412", "PreconditionFailed"}:
                    try:
                        for _ in self.read_verified(artifact):
                            pass
                    except IngestionError:
                        raise Conflict() from None
                else:
                    raise IngestionError(
                        FailureClass.NETWORK, "s3-put-failed", retryable=True
                    ) from None
            return artifact

    def read_verified(self, artifact: Artifact) -> Iterator[bytes]:
        uri = urlsplit(artifact.uri)
        key = unquote(uri.path.lstrip("/"))
        if uri.scheme != "s3" or uri.netloc != self.bucket or not key.startswith(self.prefix):
            raise ValueError("Artifact is outside the configured bucket/prefix")
        safe_key(key)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception:
            raise IngestionError(FailureClass.NETWORK, "s3-read-failed", retryable=True) from None
        body = response["Body"]
        try:
            yield from checked(iter(lambda: body.read(CHUNK), b""), artifact)
        finally:
            body.close()
