"""Verified, size-bounded access to retained provider payloads."""

import gzip
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO

from urbanlens.ingestion.contracts import Artifact, FailureClass
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.interfaces import ArtifactStore


@contextmanager
def payload(
    store: ArtifactStore, artifact: Artifact, *, max_bytes: int = 64 * 1024 * 1024
) -> Iterator[IO[bytes]]:
    """Verify all stored bytes, then bound decompression; never parse an unchecked prefix."""
    with tempfile.TemporaryFile() as stored, tempfile.TemporaryFile() as decoded:
        for block in store.read_verified(artifact):
            stored.write(block)
        stored.seek(0)
        source = gzip.GzipFile(fileobj=stored) if artifact.compression == "gzip" else stored
        try:
            while block := source.read(64 * 1024):
                decoded.write(block)
                if decoded.tell() > max_bytes:
                    raise IngestionError(FailureClass.BUDGET, "expanded-payload-byte-budget")
        finally:
            if source is not stored:
                source.close()
        decoded.seek(0)
        yield decoded
