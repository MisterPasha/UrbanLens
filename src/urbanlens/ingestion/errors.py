"""Safe failure codes: never expose upstream exceptions, bodies or credential-bearing URLs."""

from urbanlens.ingestion.contracts import Failure, FailureClass


class IngestionError(RuntimeError):
    def __init__(self, category: FailureClass, code: str, *, retryable: bool = False) -> None:
        self.failure = Failure(category=category, code=code, retryable=retryable)
        super().__init__(code)


class Conflict(IngestionError):
    def __init__(self) -> None:
        super().__init__(FailureClass.INTEGRITY, "immutable-write-conflict")


class LeaseLost(IngestionError):
    def __init__(self) -> None:
        super().__init__(FailureClass.INTERNAL, "scope-lease-lost", retryable=True)
