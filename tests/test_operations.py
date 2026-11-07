"""Retention must reclaim expired history while preserving still-needed evidence."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self
from uuid import uuid4

import pytest
from test_silver import MemoryStore, portable_bundle
from test_silver import silver_roles as silver_roles

from scripts.maintenance import garbage, lifecycle
from urbanlens.database.migrations import connect
from urbanlens.silver.history import required
from urbanlens.silver.load import load
from urbanlens.silver.operations import cutoff, prune


@pytest.mark.database
def test_expired_history_is_removed_but_current_and_retained_event_support_survives(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    store = MemoryStore()
    bundle = portable_bundle(tmp_path, store)
    with connect(silver_roles["pipeline"]) as c:
        load(c, store, bundle)
        current, identity, snapshot = required(
            c.execute(
                "SELECT observation_id,identity_id,last_snapshot_id FROM"
                " ul_silver.observation_version LIMIT 1"
            ).fetchone()
        )
        old = [uuid4() for _ in range(3)]
        for index, oid in enumerate(old, 1):
            c.execute(
                "INSERT INTO ul_silver.observation_version(observation_id,"
                "identity_id,provider,record_hash,"
                "normalisation_version,first_snapshot_id,last_snapshot_id,"
                "first_observed_at,last_observed_at,"
                "valid_from,valid_to,source_artifact_id,raw_record_locator,"
                "address_state,location_quality,scope_id) "
                "SELECT %s,identity_id,provider,%s,%s,first_snapshot_id,"
                "last_snapshot_id,'2024-01-01','2024-02-01',"
                "'2024-01-01','2024-03-01',source_artifact_id,'old synthetic"
                " evidence','missing','missing',scope_id "
                "FROM ul_silver.observation_version WHERE observation_id=%s",
                (oid, str(index) * 64, "synthetic-old-" + str(index), current),
            )
        c.execute(
            "INSERT INTO ul_silver.company(company_number,legal_name,legal_status,company_type,"
            "first_observed_at,last_observed_at,source_observation_id) VALUES "
            "('SC000001','Synthetic','active','ltd',now(),now(),%s)",
            (old[1],),
        )
        for oid, year in ((old[0], 2024), (old[2], 2026)):
            event = uuid4()
            c.execute(
                "INSERT INTO ul_silver.change_event(event_id,event_key,entity_type,identity_id,"
                "event_type,snapshot_id,observed_at,evidence_fields,"
                "retained_evidence_uri,rule_version) "
                "VALUES (%s,%s,'source',%s,'source_changed',%s,%s,'{}',"
                "'s3://synthetic/evidence/event','fixture')",
                (event, str(year % 10) * 64, identity, snapshot, datetime(year, 1, 1, tzinfo=UTC)),
            )
            c.execute(
                "INSERT INTO ul_silver.event_observation_evidence VALUES (%s,%s)", (event, oid)
            )
        before = required(
            c.execute("SELECT count(*) FROM ul_silver.observation_version").fetchone()
        )[0]
        planned = prune(c)
        assert planned["expired_events"] == 1 and planned["removable_changed_observations"] == 1
        assert (
            required(c.execute("SELECT count(*) FROM ul_silver.observation_version").fetchone())[0]
            == before
        )
        applied = prune(c, apply=True)
        assert applied["removable_changed_observations"] == 1
        retained = {
            r[0] for r in c.execute("SELECT observation_id FROM ul_silver.observation_version")
        }
        assert (
            current in retained
            and old[0] not in retained
            and old[1] in retained
            and old[2] in retained
        )
        assert prune(c, apply=True)["removable_changed_observations"] == 0
        assert cutoff(datetime(2024, 2, 29, tzinfo=UTC)) == datetime(2023, 2, 28, tzinfo=UTC)


class RemoteMemory(MemoryStore):
    prefix = ""
    bucket = "synthetic"

    @property
    def client(self) -> Self:
        return self

    def get_paginator(self, name: str) -> Self:
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs: Any) -> Iterator[dict[str, Any]]:
        yield dict(
            Contents=[
                dict(Key=key, Size=len(body), LastModified=datetime(2024, 1, 1, tzinfo=UTC))
                for key, (_, body) in self.objects.items()
                if key.startswith(kwargs["Prefix"])
            ]
        )

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert Bucket == self.bucket
        del self.objects[Key]
        return {}


@pytest.mark.database
def test_object_cleanup_protects_current_silver_and_fragment_review_evidence(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    store = RemoteMemory()
    bundle = portable_bundle(tmp_path, store)
    proof = store.put_immutable(
        "silver/current review.json",
        iter([b"{}"]),
        media_type="application/json",
        compression="none",
    )
    store.put_immutable(
        "silver/expired-orphan.json",
        iter([b"{}"]),
        media_type="application/json",
        compression="none",
    )
    with connect(silver_roles["pipeline"]) as c:
        load(c, store, bundle)
        c.execute(
            "INSERT INTO ul_control.review(review_type,decision,reason,"
            "evidence_uri,rule_version,reviewer,reviewed_on) "
            "VALUES ('eligibility','approved','Synthetic current review',%s,"
            "'fixture','fixture',current_date)",
            (proof.uri + "#item",),
        )
        plan = garbage(c, store, apply=False)
        assert [x["key"] for x in plan["unreferenced_old_objects"]] == [
            "silver/expired-orphan.json"
        ]
        garbage(c, store, apply=True)
        assert store.stat("silver/current review.json") is not None
        assert store.stat("silver/expired-orphan.json") is None
        assert all(
            store.stat(b.records.uri.removeprefix("s3://synthetic/")) for b in bundle.batches
        )
    rules = {r.get("Prefix"): r.get("Expiration", {}).get("Days") for r in lifecycle()["Rules"]}
    assert (
        rules["bronze/"] == 90
        and rules["backups/neon/daily/"] == 7
        and rules["backups/neon/weekly/"] == 28
    )
