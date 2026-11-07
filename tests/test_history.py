"""Monthly sequence checks; all businesses, cycles and events here are synthetic."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from test_silver import MemoryStore, fsa_source
from test_silver import silver_roles as silver_roles

from urbanlens.database.migrations import connect
from urbanlens.ingestion.contracts import Snapshot
from urbanlens.silver import normalise
from urbanlens.silver.classification import build as classify
from urbanlens.silver.classification import load as load_classification
from urbanlens.silver.classification import save_json
from urbanlens.silver.companies import Register
from urbanlens.silver.companies import run as companies
from urbanlens.silver.eligibility import Reviews, Taxonomy, digest
from urbanlens.silver.history import interrupt, required
from urbanlens.silver.identity import Reviews as IdentityReviews
from urbanlens.silver.identity import Rules
from urbanlens.silver.load import load
from urbanlens.silver.location_processing import run as locate
from urbanlens.silver.models import Evidence
from urbanlens.silver.pipeline import Bundle, write_batch
from urbanlens.silver.resolution import run as resolve


class Sequence:
    def __init__(self, path: Path) -> None:
        self.store = MemoryStore()
        self.base = fsa_source(path)[1]

    def month(
        self,
        month: int,
        provider: str,
        records: list[dict[str, Any]],
        *,
        scope: str = "history-city",
    ) -> Bundle:
        start = datetime(2026 + (month - 1) // 12, (month - 1) % 12 + 1, 1, tzinfo=UTC)
        end = datetime(start.year + start.month // 12, start.month % 12 + 1, 1, tzinfo=UTC)
        cycle = {
            **self.base.cycle.model_dump(),
            "cycle_id": uuid4(),
            "provider": provider,
            "scope": scope,
            "window_start": start,
            "window_end": end,
        }
        if provider == "companies-house":
            cycle["query_version"] = "companies-monthly-v1"
            cycle["query_sha256"] = digest(dict(company_edition=month))
        run = {
            **self.base.run.model_dump(),
            "run_id": uuid4(),
            "cycle_id": cycle["cycle_id"],
            "started_at": start + timedelta(hours=1),
            "ended_at": start + timedelta(hours=2),
        }
        raw = self.store.put_immutable(
            f"bronze/{provider}/{scope}/{cycle['cycle_id']}/{run['run_id']}/full/raw.json",
            iter([json.dumps(records).encode()]),
            media_type="application/json",
            compression="none",
        )
        part = {
            **self.base.partitions[0].model_dump(),
            "cycle_id": cycle["cycle_id"],
            "run_id": run["run_id"],
            "fetched_at": start + timedelta(hours=1),
            "artifacts": [raw],
            "counts": dict(
                raw=len(records), parsed=len(records), accepted=len(records), rejected=0
            ),
        }
        snapshot = Snapshot.model_validate(
            {
                **self.base.model_dump(),
                "snapshot_id": uuid4(),
                "cycle": cycle,
                "run": run,
                "partitions": [part],
            }
        )
        manifest = save_json(self.store, "manifests/sequence", snapshot.model_dump(mode="json"))
        ctx = dict(
            scope=scope,
            snapshot_id=snapshot.snapshot_id,
            cycle_id=snapshot.cycle.cycle_id,
            observed_at=part["fetched_at"],
            evidence=(Evidence(artifact=raw, locator="$"),),
        )
        values = [
            (
                normalise.company(r, ctx, profile=True)
                if provider == "companies-house"
                else normalise.fsa(r, ctx)
                if provider == "fsa"
                else normalise.osm(r, ctx)
            )
            for r in records
        ]
        batch = write_batch(
            self.store,
            f"silver/{snapshot.snapshot_id}.jsonl.gz",
            iter(values),
            provider=provider,
            scope=scope,
            partition="full",
            snapshot=snapshot,
            source_manifest=manifest,
        )
        return Bundle(batches=(batch,))


def pub(number: int = 1) -> dict[str, Any]:
    return dict(
        FHRSID=str(number),
        BusinessName="History Pub",
        BusinessTypeID="7843",
        BusinessType="Pub/bar/nightclub",
        AddressLine1="1 Test Street",
        PostCode="BN1 1AA",
        Longitude="-0.14",
        Latitude="50.82",
    )


@pytest.mark.database
def test_complete_absence_failure_fresh_identical_payload_return_and_new_scope(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    seq = Sequence(tmp_path)
    with connect(silver_roles["pipeline"]) as c:
        load(c, seq.store, seq.month(1, "fsa", [pub()]))
        assert c.execute("SELECT count(*) FROM ul_silver.change_event").fetchone() == (0,)
        absence = seq.month(2, "fsa", [])
        load(c, seq.store, absence)
        load(c, seq.store, absence)
        assert c.execute(
            "SELECT observation_state FROM ul_silver.identity_scope_state"
        ).fetchone() == ("pending_absence",)
        failed_bundle = seq.month(3, "fsa", [])
        good = required(failed_bundle.batches[0].snapshot)
        failure = dict(category="timeout", code="fixture-timeout", retryable=True)
        failed = Snapshot.model_validate(
            {
                **good.model_dump(),
                "completeness": "failed",
                "run": {**good.run.model_dump(), "status": "failed", "failure": failure},
                "partitions": [
                    {**good.partitions[0].model_dump(), "status": "failed", "failure": failure}
                ],
            }
        )
        evidence = save_json(seq.store, "manifests/failed", failed.model_dump(mode="json"))
        interrupt(c, seq.store, failed, evidence)
        assert c.execute(
            "SELECT absence_interrupted FROM ul_silver.identity_scope_state"
        ).fetchone() == (True,)
        load(c, seq.store, seq.month(4, "fsa", []))
        assert c.execute(
            "SELECT observation_state FROM ul_silver.identity_scope_state"
        ).fetchone() == ("pending_absence",)
        # Same empty payload hash, genuinely acquired in the next monthly window.
        load(c, seq.store, seq.month(5, "fsa", []))
        assert c.execute(
            "SELECT observation_state FROM ul_silver.identity_scope_state"
        ).fetchone() == ("no_longer_observed",)
        load(c, seq.store, seq.month(6, "fsa", [pub()]))
        assert c.execute(
            "SELECT observation_state FROM ul_silver.identity_scope_state"
        ).fetchone() == ("observed",)
        load(c, seq.store, seq.month(7, "fsa", []))
        latest = seq.month(8, "fsa", [pub(), pub(2)])
        load(c, seq.store, latest)
        load(c, seq.store, seq.month(8, "fsa", [pub(3)], scope="newly-introduced-scope"))
        assert dict(
            c.execute(
                "SELECT event_type,count(*) FROM ul_silver.change_event GROUP BY event_type"
            ).fetchall()
        ) == {"no_longer_observed": 1, "reappeared": 1, "newly_observed": 1}
        copied = seq.month(9, "fsa", [])
        snap = required(copied.batches[0].snapshot)
        old = required(absence.batches[0].snapshot)
        fake = Snapshot.model_validate(
            {
                **snap.model_dump(),
                "partitions": [
                    {**snap.partitions[0].model_dump(), "artifacts": old.partitions[0].artifacts}
                ],
            }
        )
        manifest = save_json(seq.store, "manifests/copied", fake.model_dump(mode="json"))
        bad = Bundle(
            batches=(
                copied.batches[0].model_copy(
                    update={"snapshot": fake, "source_manifest": manifest}
                ),
            )
        )
        c.commit()
        with pytest.raises(ValueError, match="old run"):
            load(c, seq.store, bad)
        assert c.execute(
            "SELECT count(*) FROM ul_silver.change_event WHERE retained_evidence_uri IS NULL"
        ).fetchone() == (0,)
        # Raw expiry leaves normalised event evidence readable and replayable.
        for key in list(seq.store.objects):
            if key.startswith("bronze/"):
                del seq.store.objects[key]
        load(c, seq.store, latest)
        for (uri,) in c.execute("SELECT retained_evidence_uri FROM ul_silver.change_event"):
            proof = required(seq.store.stat(uri.removeprefix("s3://synthetic/")))
            retained = json.loads(b"".join(seq.store.read_verified(proof)))
            assert retained["after"] is not None and retained["evidence"]["snapshot_id"]


@pytest.mark.database
def test_canonical_multisource_support_company_dissolution_and_processing_changes(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    seq = Sequence(tmp_path)
    root = Path(__file__).parents[1]
    taxonomy = Taxonomy.model_validate_json((root / "config/taxonomy.json").read_text())
    rules = Rules.model_validate_json((root / "config/identity-rules.json").read_text())
    osm = dict(
        element_type="node",
        osm_id=100,
        tags={
            "name": "History Pub",
            "amenity": "pub",
            "fhrs:id": "1",
            "addr:full": "1 Test Street",
            "addr:postcode": "BN1 1AA",
        },
        geometry_state="source-node",
        longitude=-0.14,
        latitude=50.82,
        geometry=None,
        original_geometry=None,
        fhrs_ids=["1"],
        invalid_fhrs_tokens=[],
        in_city=True,
        crosses_city_boundary=False,
    )
    legal = dict(
        company_number="00000001",
        company_name="History Ltd",
        company_status="active",
        type="ltd",
        sic_codes=["56302"],
    )
    with connect(silver_roles["pipeline"]) as c:

        def refresh(bundle: Bundle, month: int) -> dict[str, Any]:
            load(c, seq.store, bundle)
            manifest = save_json(
                seq.store, "manifests/classification", bundle.model_dump(mode="json")
            )
            decision = classify(seq.store, bundle, manifest, taxonomy, Reviews(version="fixture"))[
                0
            ]
            load_classification(c, seq.store, decision)
            locate(c, seq.store, datetime(2026, month, 3, tzinfo=UTC), [])
            return resolve(c, seq.store, rules, IdentityReviews(version="fixture"))

        fsa = seq.month(1, "fsa", [pub()])
        om = seq.month(1, "osm", [osm])
        baseline = Bundle(batches=fsa.batches + om.batches)
        load(c, seq.store, baseline)
        aid = required(c.execute("SELECT artifact_id FROM ul_control.artifact LIMIT 1").fetchone())[
            0
        ]
        c.execute(
            "INSERT INTO ul_silver.area SELECT 'LSOA-2021','E01'||lpad(n::text,6,'0'),"
            "'Synthetic','E06000043',2021,public.ST_Multi(public.ST_MakeEnvelope("
            "-0.15,50.81,-0.12,50.83,4326)),1,%s FROM generate_series(1,165) n",
            (aid,),
        )
        assert refresh(baseline, 1)["establishments"] == 1
        load(
            c,
            seq.store,
            seq.month(1, "companies-house", [legal], scope="tracked-companies-profiles"),
        )
        companies(c, seq.store, Register(version="company-links-v1", note="Synthetic baseline"))
        assert refresh(seq.month(2, "osm", []), 2)["history_events"] == {}
        assert refresh(seq.month(3, "osm", []), 3)["history_events"] == {}
        assert c.execute(
            "SELECT observation_state,primary_spatial_eligible FROM ul_silver.establishment"
        ).fetchone() == ("observed", True)
        load(
            c,
            seq.store,
            seq.month(
                2,
                "companies-house",
                [{**legal, "company_status": "dissolved", "date_of_cessation": "2026-02-01"}],
                scope="tracked-companies-profiles",
            ),
        )
        companies(c, seq.store, Register(version="company-links-v1", note="Synthetic baseline"))
        assert c.execute(
            "SELECT count(*) FROM ul_silver.change_event "
            "WHERE entity_type='company' AND event_type='company_dissolved'"
        ).fetchone() == (1,)
        assert c.execute("SELECT observation_state FROM ul_silver.establishment").fetchone() == (
            "observed",
        )
        assert refresh(seq.month(2, "fsa", []), 3)["history_events"] == {}
        assert c.execute(
            "SELECT primary_spatial_eligible FROM ul_silver.establishment"
        ).fetchone() == (True,)
        assert refresh(seq.month(3, "fsa", []), 3)["history_events"] == {"no_longer_observed": 1}
        returned = seq.month(4, "fsa", [pub()])
        assert refresh(returned, 4)["history_events"] == {"reappeared": 1}
        assert refresh(returned, 4)["history_events"] == {}
        # Same accepted source with a new rule version is a processing correction.
        revised = rules.model_copy(update={"version": "synthetic-rule-correction"})
        assert (
            resolve(c, seq.store, revised, IdentityReviews(version="fixture"))["history_events"]
            == {}
        )
        c.execute("UPDATE ul_silver.establishment SET display_name='Reviewed correction'")
        from urbanlens.silver.history import project

        assert project(c, seq.store, "establishment") == {"processing_correction": 1}
        assert c.execute(
            "SELECT count(*) FROM ul_silver.change_event "
            "WHERE entity_type='establishment' AND event_type='newly_observed'"
        ).fetchone() == (0,)
        new_pub = {
            **pub(2),
            "BusinessName": "Fresh Pub",
            "AddressLine1": "99 New Street",
            "Longitude": "-0.13",
            "BusinessTypeID": "7844",
            "BusinessType": "Takeaway/sandwich shop",
        }
        result = refresh(seq.month(5, "fsa", [pub(), new_pub]), 5)
        assert result["history_events"].get("newly_observed") == 1
