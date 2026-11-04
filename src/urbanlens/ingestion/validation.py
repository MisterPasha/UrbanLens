"""Streaming validation hooks with disk-backed duplicate detection across partitions."""

import sqlite3
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from urbanlens.ingestion.contracts import ValidationCheck, ValidationReport

Record = Mapping[str, object]


def valid_record(record: Record) -> bool:
    return True


def no_reasons(record: Record) -> tuple[str, ...]:
    return ()


@dataclass(frozen=True)
class ValidationRules:
    version: str
    identity_field: str
    required_fields: tuple[str, ...]
    schema: Callable[[Record], bool] = valid_record
    scope: Callable[[Record], bool] = valid_record
    eligible: Callable[[Record], bool] = valid_record
    rejection_reasons: Callable[[Record], tuple[str, ...]] = no_reasons
    min_records: int = 0
    max_records: int = 1_000_000
    max_drop_fraction: float = 0.5
    multiplicity_field: str | None = None

    def __post_init__(self) -> None:
        if not self.version or not self.identity_field or self.min_records < 0:
            raise ValueError("Invalid validation rules")
        if self.max_records < self.min_records or not 0 <= self.max_drop_fraction <= 1:
            raise ValueError("Invalid count limits")


class Validator:
    def __init__(self, rules: ValidationRules, path: Path) -> None:
        self.rules, self.count = rules, 0
        self.reasons: Counter[str] = Counter()
        self.ok = {name: True for name in ("schema", "scope", "duplicates")}
        self.connection = sqlite3.connect(path)
        self.connection.execute("CREATE TABLE identities (id TEXT PRIMARY KEY)")

    def weight(self, row: Record) -> int:
        value = row.get(self.rules.multiplicity_field, 1) if self.rules.multiplicity_field else 1
        if type(value) is not int or not 1 <= value <= 10_000_000:
            raise ValueError("Invalid source record multiplicity")
        if value != 1 and self.rules.eligible(row):
            raise ValueError("Accepted records cannot be aggregated")
        return value

    def consume(self, row: Record) -> bool:
        weight = self.weight(row)
        self.count += weight
        identity = row.get(self.rules.identity_field)
        schema = (
            isinstance(identity, str)
            and bool(identity)
            and all(field in row for field in self.rules.required_fields)
            and self.rules.schema(row)
        )
        scope = self.rules.scope(row)
        duplicate = False
        if schema:
            try:
                self.connection.execute("INSERT INTO identities VALUES (?)", (identity,))
            except sqlite3.IntegrityError:
                duplicate = True
        self.ok["schema"] &= schema
        self.ok["scope"] &= scope
        self.ok["duplicates"] &= not duplicate
        eligible = self.rules.eligible(row)
        accepted = schema and scope and not duplicate and eligible
        if not accepted:
            reasons = self.rules.rejection_reasons(row)
            codes = reasons or tuple(
                name
                for name, ok in (
                    ("schema", schema),
                    ("scope", scope),
                    ("duplicate", not duplicate),
                    ("eligibility", eligible),
                )
                if not ok
            )
            self.reasons.update({code: weight for code in codes})
        return accepted

    def report(self, *, complete: bool, previous_count: int | None) -> ValidationReport:
        count_ok = self.rules.min_records <= self.count <= self.rules.max_records
        if previous_count is not None and previous_count > 0:
            count_ok &= self.count >= previous_count * (1 - self.rules.max_drop_fraction)
        results = {**self.ok, "completeness": complete, "counts": count_ok}
        return ValidationReport(
            rule_version=self.rules.version,
            required_checks=tuple(results),
            checks=tuple(
                ValidationCheck(code=name, outcome="passed" if ok else "failed")
                for name, ok in results.items()
            ),
        )

    def close(self) -> None:
        self.connection.close()
