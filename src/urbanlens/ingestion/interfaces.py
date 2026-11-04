"""Local/S3-neutral boundaries; concrete clients and durable writes belong to step 11."""

from collections.abc import Iterator, Mapping
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from urbanlens.ingestion.contracts import Artifact, CollectionCycle, Partition, Provider, Run, Token


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    MANIFESTS = "manifests"
    EVIDENCE = "evidence"
    BACKUPS = "backups"


def artifact_key(
    layer: Layer,
    cycle: CollectionCycle,
    run_id: UUID,
    filename: str,
    partition_id: str | None = None,
) -> str:
    """Run-specific keys prevent a retry overwriting an earlier attempt's evidence."""
    import re

    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", filename) or ".." in filename:
        raise ValueError("Artifact filename must be a safe relative name")
    if partition_id is not None and partition_id not in cycle.expected_partitions:
        raise ValueError("Artifact partition is outside this cycle")
    pieces = [
        layer.value,
        cycle.provider.value,
        cycle.scope,
        cycle.window_start.date().isoformat(),
        str(cycle.cycle_id),
        str(run_id),
    ]
    if partition_id is not None:
        pieces.append(partition_id)
    return "/".join([*pieces, filename])


class ArtifactStore(Protocol):
    """Keys are relative to a configured local root or private S3 bucket/prefix."""

    def stat(self, key: str) -> Artifact | None:
        """Describe an existing finalized object, or return None only for absence."""
        ...

    def put_immutable(
        self, key: str, chunks: Iterator[bytes], *, media_type: str, compression: str
    ) -> Artifact:
        """Stream stored bytes; identical key/content is idempotent, other content conflicts.

        Return only after finalization/checksum. Partial uploads must not look committed.
        """
        ...

    def read_verified(self, artifact: Artifact) -> Iterator[bytes]:
        """Read retained stored bytes; verify byte count/hash before parse can be complete."""
        ...


class ProviderAdapter(Protocol):
    """Fetch and parse are separate, so replay never needs live provider access."""

    @property
    def provider(self) -> Provider:
        """Provider handled by this adapter."""
        ...

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: Token, store: ArtifactStore
    ) -> Partition:
        """Preserve the raw response and return fetched metadata, or an explicit failed result.

        Provider errors must never return a successful empty partition.
        """
        ...

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        """Parse retained artifacts only. Raise on malformed/truncated/error-bearing input.

        Iterator exhaustion alone is not completeness; finalize counts and validation first.
        """
        ...
