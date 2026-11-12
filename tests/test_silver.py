"""Source meaning, deterministic hashes and traceable immutable Silver artifacts."""

import gzip
import hashlib
import io
import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Self
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from urbanlens.ingestion.contracts import Artifact, Snapshot
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.fsa import FsaAdapter, FsaConfig
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.postcodes import Lookup
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore
from urbanlens.silver import normalise as n
from urbanlens.silver.models import Evidence, Observation
from urbanlens.silver.pipeline import Bundle, build, retained_gzip, verified_observations


def test_compressed_content_replay_across_windows_and_linux(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    content = b'{"source_key":"unchanged"}\n'
    windows = bytearray(gzip.compress(content, mtime=0))
    windows[9] = 10
    linux = bytearray(windows)
    linux[9] = 3
    old = store.put_immutable(
        "silver/content.jsonl.gz",
        iter([bytes(windows)]),
        media_type="application/x-ndjson",
        compression="gzip",
    )
    replay = retained_gzip(
        store, "silver/content.jsonl.gz", io.BytesIO(linux), hashlib.sha256(content).hexdigest()
    )
    assert replay == old
    assert b"".join(store.read_verified(replay)) == windows
    with pytest.raises(ValueError, match="different decoded content"):
        retained_gzip(store, "silver/content.jsonl.gz", io.BytesIO(linux), "0" * 64)


def context() -> dict[str, Any]:
    artifact = Artifact(
        uri="s3://synthetic/bronze/source.json",
        sha256="a" * 64,
        byte_count=1,
        media_type="application/json",
    )
    return dict(
        scope="synthetic-scope",
        snapshot_id=uuid4(),
        cycle_id=uuid4(),
        observed_at=datetime(2026, 11, 1, tzinfo=UTC),
        evidence=(Evidence(artifact=artifact, locator="row/1"),),
    )


def fsa_source(
    tmp_path: Path, *, month: int = 11, renamed: bool = False
) -> tuple[LocalArtifactStore, Snapshot]:
    fixtures = Path(__file__).parent / "fixtures/fsa"
    config = FsaConfig(min_records=0)

    def request(request: httpx.Request) -> httpx.Response:
        content = (
            fixtures / ("authority.json" if "/Authorities/" in str(request.url) else "valid.xml")
        ).read_bytes()
        if renamed:
            content = content.replace(b"Example Caf", b"Renamed Caf")
        return httpx.Response(200, stream=httpx.ByteStream(content))

    downloader = HttpDownloader(
        config.cycle(date(2026, 11, 4)).provider,
        HttpPolicy(max_requests=2, backoff_seconds=0),
        transport=httpx.MockTransport(request),
    )
    store = LocalArtifactStore(tmp_path / "source")

    class CollectionClock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Self:
            return cls(2026, month, 4, 12, tzinfo=UTC)

    with (
        patch("urbanlens.ingestion.fsa.datetime", CollectionClock),
        patch("urbanlens.ingestion.engine.datetime", CollectionClock),
    ):
        snapshot = Collector(
            store, Registry(SQLiteStateBackend(tmp_path / "state.sqlite"))
        ).collect(
            config.cycle(date(2026, month, 4)),
            FsaAdapter(config, downloader),
            config.rules(),
            pipeline_version="fixture",
        )
    return store, snapshot


class MemoryStore:
    """Private S3-shaped fixture store, with no AWS connection."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[Artifact, bytes]] = {}

    def put_immutable(
        self, key: str, chunks: Iterator[bytes], *, media_type: str, compression: str
    ) -> Artifact:
        data = b"".join(chunks)
        artifact = Artifact.model_validate(
            dict(
                uri="s3://synthetic/" + key,
                sha256=hashlib.sha256(data).hexdigest(),
                byte_count=len(data),
                media_type=media_type,
                compression=compression,
            )
        )
        if key in self.objects and self.objects[key][0] != artifact:
            raise ValueError("Immutable fixture object conflict")
        self.objects[key] = (artifact, data)
        return artifact

    def stat(self, key: str) -> Artifact | None:
        return self.objects[key][0] if key in self.objects else None

    def read_verified(self, artifact: Artifact) -> Iterator[bytes]:
        key = artifact.uri.removeprefix("s3://synthetic/")
        actual, data = self.objects[key]
        assert actual == artifact and hashlib.sha256(data).hexdigest() == artifact.sha256
        yield data


def portable_bundle(
    tmp_path: Path, output: MemoryStore, *, month: int = 11, renamed: bool = False
) -> Bundle:
    source, snapshot = fsa_source(tmp_path, month=month, renamed=renamed)

    def resolve(artifact: Artifact) -> Artifact:
        return output.put_immutable(
            "bronze/retained/" + artifact.sha256,
            source.read_verified(artifact),
            media_type=artifact.media_type,
            compression=artifact.compression,
        )

    return build([snapshot], source, output, resolve=resolve)[0]


@pytest.fixture
def silver_roles() -> Iterator[dict[str, str]]:
    from scripts.database import credential, scratch_database
    from urbanlens.database.migrations import ROLES, connect, migrate

    with scratch_database("local") as name:
        dsns = {purpose: credential("local", purpose, database=name) for purpose in ROLES}
        with connect(dsns["migration"]) as connection:
            migrate(connection)
        yield dsns


@pytest.mark.database
def test_bounded_load_replay_and_changed_month_keep_only_changed_versions(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.load import load

    output = MemoryStore()
    first = portable_bundle(tmp_path / "first", output)
    next_month = portable_bundle(tmp_path / "next", output, month=12, renamed=True)
    with connect(silver_roles["pipeline"]) as connection:
        assert load(connection, output, first)["counts"] == {"inserted": 4}
        assert load(connection, output, first)["counts"] == {"unchanged": 4}
        assert load(connection, output, next_month)["counts"] == {"changed": 1, "unchanged": 3}
        assert connection.execute(
            "SELECT count(*),count(*) FILTER(WHERE valid_to IS NULL) "
            "FROM ul_silver.observation_version"
        ).fetchone() == (5, 4)
        with pytest.raises(ValueError, match="newer/different"):
            load(connection, output, first)
    with connect(silver_roles["api"]) as api:
        assert api.execute("SELECT count(*) FROM ul_api.establishment").fetchone() == (0,)


@pytest.mark.database
def test_bad_later_partition_rolls_back_entire_load(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.load import load

    output = MemoryStore()
    bundle = portable_bundle(tmp_path, output)
    original = bundle.batches[0]
    bad = original.model_copy(update={"decoded_sha256": "0" * 64})
    with connect(silver_roles["pipeline"]) as connection:
        with pytest.raises(ValueError, match="count/hash mismatch"):
            load(connection, output, Bundle(batches=(original, bad)))
        assert connection.execute("SELECT count(*) FROM ul_control.source_head").fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM ul_silver.observation_version"
        ).fetchone() == (0,)


@pytest.mark.database
def test_same_company_keeps_bulk_and_profile_streams_independent(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.classification import save_json
    from urbanlens.silver.companies import Link, Register, venue_hash
    from urbanlens.silver.companies import run as company_context
    from urbanlens.silver.load import load
    from urbanlens.silver.pipeline import write_batch

    _, base = fsa_source(tmp_path)
    output = MemoryStore()
    batches = []
    for scope in ("registered-office-bulk", "tracked-profiles"):
        cycle = {
            **base.cycle.model_dump(),
            "cycle_id": uuid4(),
            "provider": "companies-house",
            "scope": scope,
        }
        run = {**base.run.model_dump(), "run_id": uuid4(), "cycle_id": cycle["cycle_id"]}
        part = {
            **base.partitions[0].model_dump(),
            "cycle_id": cycle["cycle_id"],
            "run_id": run["run_id"],
            "counts": dict(raw=1, parsed=1, accepted=1, rejected=0),
        }
        snapshot = Snapshot.model_validate(
            {
                **base.model_dump(),
                "snapshot_id": uuid4(),
                "cycle": cycle,
                "run": run,
                "partitions": [part],
            }
        )
        manifest = output.put_immutable(
            f"manifests/{scope}.json",
            iter([snapshot.model_dump_json().encode()]),
            media_type="application/json",
            compression="none",
        )
        evidence = output.put_immutable(
            f"bronze/{scope}.json", iter([b"{}"]), media_type="application/json", compression="none"
        )
        ctx = {
            **context(),
            "scope": scope,
            "snapshot_id": snapshot.snapshot_id,
            "cycle_id": snapshot.cycle.cycle_id,
            "evidence": (Evidence(artifact=evidence, locator="$"),),
        }
        record = n.company(
            dict(
                company_number="SC000001",
                company_name="Example Ltd",
                company_status="active",
                type="ltd",
                sic_codes=["56101"],
            ),
            ctx,
            profile=True,
        )
        batches.append(
            write_batch(
                output,
                f"silver/{scope}.jsonl.gz",
                iter([record]),
                provider="companies-house",
                scope=scope,
                partition="full",
                snapshot=snapshot,
                source_manifest=manifest,
            )
        )
    bundle = Bundle(batches=tuple(batches))
    with connect(silver_roles["pipeline"]) as connection:
        assert load(connection, output, bundle)["counts"] == {"inserted": 2}
        assert load(connection, output, bundle)["counts"] == {"unchanged": 2}
        assert connection.execute("SELECT count(*) FROM ul_silver.source_identity").fetchone() == (
            1,
        )
        assert connection.execute(
            "SELECT count(*) FROM ul_silver.observation_version WHERE valid_to IS NULL"
        ).fetchone() == (2,)
        # The same outside-office company may operate multiple independently counted venues.
        from psycopg.rows import dict_row

        load(connection, output, portable_bundle(tmp_path / "venues", output))
        for (identity,) in connection.execute(
            "SELECT identity_id FROM ul_silver.source_identity WHERE provider='fsa' LIMIT 2"
        ).fetchall():
            venue = uuid4()
            connection.execute(
                "INSERT INTO ul_silver.establishment(establishment_id,display_name,eligibility,"
                "primary_category,taxonomy_version,identity_rule_version,location_quality,"
                "observation_state,first_observed_at,last_observed_at,duplicate_status) "
                "VALUES (%s,'Synthetic branch','eligible','cafe','fixture','fixture','missing',"
                "'observed',now(),now(),'resolved')",
                (venue,),
            )
            connection.execute(
                "INSERT INTO ul_silver.establishment_source_link(establishment_id,identity_id,"
                "method,evidence_uri,rule_version,decision,valid_from) "
                "VALUES (%s,%s,'fixture','s3://synthetic/evidence','fixture','accepted',now())",
                (venue, identity),
            )
        empty = Register(version="company-links-v1", note="Synthetic legal context")
        first = company_context(connection, output, empty)
        assert first["cohorts"] == {"registered_office": 0, "verified_operator": 0}
        assert first["coverage"]["denominator"] == 2
        assert company_context(connection, output, empty)["changed_companies"] == 0
        retained = save_json(
            output,
            "evidence/operator",
            {"statement": "Synthetic Ltd operates both synthetic branches"},
        )
        company_row = connection.execute(
            "SELECT context->>'record_hash' FROM ul_silver.company"
        ).fetchone()
        assert company_row is not None
        company_hash = company_row[0]
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT * FROM ul_silver.establishment ORDER BY establishment_id")
            links = tuple(
                Link(
                    company_number="SC000001",
                    establishment_id=row["establishment_id"],
                    company_record_hash=company_hash,
                    establishment_hash=venue_hash(row),
                    decision="approved",
                    reviewer="synthetic reviewer",
                    reviewed_on=date(2026, 11, 1),
                    evidence_url="https://example.invalid/operator",
                    evidence=retained,
                    operating_statement="Synthetic Ltd operates this specific synthetic branch",
                    reason="Synthetic explicit operating statement; no name matching",
                )
                for row in cursor.fetchall()
            )
        reviewed = Register(version="company-links-v2", links=links, note="Synthetic approvals")
        approved = company_context(connection, output, reviewed)
        assert approved["cohorts"] == {"registered_office": 0, "verified_operator": 1}
        assert approved["coverage"]["verified_establishments"] == 2
        assert company_context(connection, output, reviewed)["artifact"] == approved["artifact"]
        assert connection.execute(
            "SELECT count(*) FROM ul_silver.company_establishment_link"
        ).fetchone() == (2,)
        connection.commit()
        with pytest.raises(ValueError, match="Stale"):
            company_context(
                connection,
                output,
                Register(
                    version="company-links-v3",
                    links=(links[0].model_copy(update={"company_record_hash": "f" * 64}),),
                    note="Synthetic stale review",
                ),
            )
        with pytest.raises(ValueError, match="new version"):
            company_context(
                connection,
                output,
                reviewed.model_copy(update={"note": "Edited without version bump"}),
            )
        revoked = company_context(
            connection, output, Register(version="company-links-v3", note="Retired synthetic links")
        )
        assert revoked["coverage"]["verified_establishments"] == 0
        assert connection.execute(
            "SELECT count(*) FROM ul_silver.company_establishment_link WHERE valid_to IS NOT NULL"
        ).fetchone() == (2,)
        assert connection.execute("SELECT count(*) FROM ul_silver.change_event").fetchone() == (0,)


@pytest.mark.database
@pytest.mark.parametrize("change", ["population", "geography"])
def test_changed_reference_values_cannot_reuse_a_pinned_edition(
    tmp_path: Path, silver_roles: dict[str, str], change: str
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.load import load
    from urbanlens.silver.pipeline import write_batch

    _, base = fsa_source(tmp_path)
    output = MemoryStore()

    def reference_bundle(month: int, changed: bool) -> Bundle:
        cycle = {
            **base.cycle.model_dump(),
            "cycle_id": uuid4(),
            "provider": "ons",
            "scope": "synthetic-reference",
            "window_start": datetime(2026, month, 1, tzinfo=UTC),
            "window_end": datetime(2026, month + 1, 1, tzinfo=UTC),
        }
        run = {**base.run.model_dump(), "run_id": uuid4(), "cycle_id": cycle["cycle_id"]}
        part = {
            **base.partitions[0].model_dump(),
            "cycle_id": cycle["cycle_id"],
            "run_id": run["run_id"],
            "counts": dict(raw=1, parsed=1, accepted=1, rejected=0),
        }
        snapshot = Snapshot.model_validate(
            {
                **base.model_dump(),
                "snapshot_id": uuid4(),
                "cycle": cycle,
                "run": run,
                "partitions": [part],
            }
        )
        manifest = output.put_immutable(
            f"manifests/reference-{month}.json",
            iter([snapshot.model_dump_json().encode()]),
            media_type="application/json",
            compression="none",
        )
        evidence = output.put_immutable(
            "bronze/reference.json",
            iter([b"{}"]),
            media_type="application/json",
            compression="none",
        )
        population = 101 if changed and change == "population" else 100
        east = -0.12 if changed and change == "geography" else -0.13
        row = dict(
            lsoa21cd="E01000001",
            name="Example area",
            authority="E06000043",
            geography_vintage="LSOA-2021",
            reference_period="2024-06-30",
            population=population,
            age_0_15=20,
            age_16_64=population - 40,
            age_65_plus=20,
            land_km2="0.125",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [[-0.14, 50.82], [east, 50.82], [east, 50.83], [-0.14, 50.83], [-0.14, 50.82]]
                ],
            },
            product_editions={"population": "Mid-2024"},
        )
        record = n.area(
            row,
            {
                **context(),
                "scope": snapshot.cycle.scope,
                "snapshot_id": snapshot.snapshot_id,
                "cycle_id": snapshot.cycle.cycle_id,
                "evidence": (Evidence(artifact=evidence, locator="LSOA21CD=E01000001"),),
            },
        )
        batch = write_batch(
            output,
            f"silver/reference-{month}.jsonl.gz",
            iter([record]),
            provider="ons",
            scope=snapshot.cycle.scope,
            partition="full",
            snapshot=snapshot,
            source_manifest=manifest,
        )
        return Bundle(batches=(batch,))

    first = reference_bundle(9, False)
    later = reference_bundle(10, True)
    with connect(silver_roles["pipeline"]) as connection:
        load(connection, output, first)
        assert load(connection, output, first)["counts"] == {"unchanged": 1}
        with pytest.raises(ValueError, match="requires a new pinned"):
            load(connection, output, later)
        assert connection.execute("SELECT population FROM ul_silver.population").fetchone() == (
            100,
        )
        assert connection.execute(
            "SELECT count(*) FROM ul_silver.observation_version"
        ).fetchone() == (1,)


@pytest.mark.database
def test_classification_replay_version_binding_and_private_access(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    import psycopg

    from urbanlens.database.migrations import connect
    from urbanlens.silver.classification import build, load, save_json
    from urbanlens.silver.eligibility import Reviews, Taxonomy
    from urbanlens.silver.load import load as load_source

    store = MemoryStore()
    source = portable_bundle(tmp_path, store)
    manifest = save_json(store, "manifests/source", source.model_dump(mode="json"))
    taxonomy = Taxonomy.model_validate_json(
        (Path(__file__).parents[1] / "config/taxonomy.json").read_text()
    )
    reviews = Reviews(version="fixture")
    bundle, _, summary, _ = build(store, source, manifest, taxonomy, reviews)
    assert summary["fine_category_coverage"]["fraction"] is None
    with connect(silver_roles["pipeline"]) as c:
        load_source(c, store, source)
        assert load(c, store, bundle)["inserted_or_changed"] == 4
        stamps = c.execute(
            "SELECT processed_at FROM ul_silver.observation_classification ORDER BY observation_id"
        ).fetchall()
        c.commit()
        assert load(c, store, bundle)["inserted_or_changed"] == 0
        assert (
            c.execute(
                "SELECT processed_at FROM ul_silver.observation_classification "
                "ORDER BY observation_id"
            ).fetchall()
            == stamps
        )
        c.commit()
        changed = {**taxonomy.model_dump(), "osm_broad": (*taxonomy.osm_broad, "synthetic_new_tag")}
        changed_bundle = build(store, source, manifest, Taxonomy.model_validate(changed), reviews)[
            0
        ]
        with pytest.raises(ValueError, match="requires a new version"):
            load(c, store, changed_bundle)
        versioned = Taxonomy.model_validate(
            {**changed, "version": "hospitality-taxonomy-fixture-v2"}
        )
        new_bundle = build(store, source, manifest, versioned, reviews)[0]
        assert load(c, store, new_bundle)["inserted_or_changed"] == 4
        assert c.execute("SELECT count(*) FROM ul_silver.observation_version").fetchone() == (4,)
        assert c.execute("SELECT count(*) FROM ul_silver.change_event").fetchone() == (0,)
    with connect(silver_roles["api"]) as api:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            api.execute("SELECT * FROM ul_silver.observation_classification")


@pytest.mark.database
def test_stale_classification_rolls_back_without_advancing_sources(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.classification import build, load, save_json
    from urbanlens.silver.eligibility import Reviews, Taxonomy
    from urbanlens.silver.load import load as load_source

    store = MemoryStore()
    source = portable_bundle(tmp_path, store)
    manifest = save_json(store, "manifests/source", source.model_dump(mode="json"))
    taxonomy = Taxonomy.model_validate_json(
        (Path(__file__).parents[1] / "config/taxonomy.json").read_text()
    )
    bundle = build(store, source, manifest, taxonomy, Reviews(version="fixture"))[0]
    with connect(silver_roles["pipeline"]) as c:
        load_source(c, store, source)
        c.execute(
            "UPDATE ul_silver.observation_version SET valid_to=clock_timestamp() "
            "WHERE name_normalised='Example Home Caterer'"
        )
        c.commit()
        with pytest.raises(ValueError, match="exact current"):
            load(c, store, bundle)
        assert c.execute(
            "SELECT count(*) FROM ul_silver.observation_classification"
        ).fetchone() == (0,)
        assert c.execute(
            "SELECT count(*) FROM ul_control.classification_definition"
        ).fetchone() == (0,)


@pytest.mark.database
def test_location_assessment_boundary_fallback_review_and_replay(
    tmp_path: Path, silver_roles: dict[str, str]
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.classification import build, load, save_json
    from urbanlens.silver.eligibility import Reviews, Taxonomy
    from urbanlens.silver.load import load as load_source
    from urbanlens.silver.location_processing import assess, run

    store = MemoryStore()
    source = portable_bundle(tmp_path, store)
    manifest = save_json(store, "manifests/source", source.model_dump(mode="json"))
    taxonomy = Taxonomy.model_validate_json(
        (Path(__file__).parents[1] / "config/taxonomy.json").read_text()
    )
    classified = build(store, source, manifest, taxonomy, Reviews(version="fixture"))[0]
    stamp = datetime(2026, 11, 5, tzinfo=UTC)
    with connect(silver_roles["pipeline"]) as c:
        load_source(c, store, source)
        load(c, store, classified)
        artifact_row = c.execute("SELECT artifact_id FROM ul_control.artifact LIMIT 1").fetchone()
        assert artifact_row is not None
        aid = artifact_row[0]
        # Deliberate overlap: all covering codes retained, exactly one lowest code assigned.
        c.execute(
            "INSERT INTO ul_silver.area SELECT 'LSOA-2021','E01'||lpad(n::text,6,'0'),"
            "'Synthetic','E06000043',2021,public.ST_Multi(public.ST_MakeEnvelope("
            "-0.14,50.81,-0.12,50.83,4326)),1,%s FROM generate_series(1,165) n",
            (aid,),
        )
        c.execute(
            "INSERT INTO ul_silver.postcode_cache VALUES ('BN11AA',%s,%s,%s,'resolved',"
            "-0.13,50.82,'E06000043','E01000002',NULL,'unknown vintage',%s,%s)",
            (
                "b" * 64,
                datetime(2026, 11, 1, tzinfo=UTC),
                datetime(2026, 12, 1, tzinfo=UTC),
                aid,
                aid,
            ),
        )
        c.commit()
        first = run(c, store, stamp, [])
        assert first["inserted_or_changed"] == 4
        assert first["counts"]["approximate"] == 2
        assert first["counts"]["withheld"] == 1
        assert first["counts"]["spatial_usable"] == 1
        assert run(c, store, stamp, [])["inserted_or_changed"] == 0
        rows, _ = assess(c, stamp, [])
        precise = next(r for r in rows if r["quality"] == "source")
        assert precise["area_code"] == "E01000001"
        assert len(precise["covering_area_codes"]) == 165
        assert all(r["area_code"] is None for r in rows if r["quality"] == "approximate")
        assert c.execute("SELECT * FROM ul_silver.search_coverage(-0.14,50.82,500)").fetchone() == (
            True,
            True,
        )
        assert c.execute("SELECT * FROM ul_silver.search_coverage(-0.16,50.82,500)").fetchone() == (
            False,
            False,
        )
        with pytest.raises(Exception, match="invalid centre or radius"), c.transaction():
            c.execute("SELECT * FROM ul_silver.search_coverage(-0.14,50.82,800)")
        with pytest.raises(Exception, match="check constraint"), c.transaction():
            c.execute(
                "UPDATE ul_silver.observation_location SET quality='approximate' "
                "WHERE spatial_usable"
            )
        expired, _ = assess(c, datetime(2026, 12, 2, tzinfo=UTC), [])
        assert not any(r["quality"] == "approximate" for r in expired)
        review = dict(
            provider=precise["provider"],
            scope=precise["scope"],
            source_key=precise["source_key"],
            expected_record_hash="0" * 64,
            longitude=-0.13,
            latitude=50.82,
            reviewed_on="2026-11-05",
            evidence="synthetic review",
            reason="fixture only",
        )
        with pytest.raises(ValueError, match="Stale location review"):
            assess(c, stamp, [review])
        review["expected_record_hash"] = precise["record_hash"]
        reviewed, _ = assess(c, stamp, [review])
        assert (
            next(r for r in reviewed if r["source_key"] == precise["source_key"])["quality"]
            == "reviewed"
        )
        c.execute("UPDATE ul_silver.postcode_cache SET authority_code='E06000000'")
        contradicted, _ = assess(c, stamp, [])
        blocked = next(r for r in contradicted if r["quality"] == "source")
        assert blocked["review_required"] and not blocked["spatial_usable"]
    with connect(silver_roles["api"]) as api:
        with pytest.raises(Exception, match="permission denied"):
            api.execute("SELECT * FROM ul_silver.observation_location")


@pytest.mark.database
@pytest.mark.parametrize("reviewed_duplicate", [False, True])
def test_canonical_resolution_replay_and_reviewed_generation_rotation(
    tmp_path: Path,
    silver_roles: dict[str, str],
    reviewed_duplicate: bool,
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.classification import build, load, save_json
    from urbanlens.silver.eligibility import Reviews, Taxonomy
    from urbanlens.silver.identity import PairReview, Ref, Rules
    from urbanlens.silver.identity import Reviews as IdentityReviews
    from urbanlens.silver.load import load as load_source
    from urbanlens.silver.location_processing import run as locate
    from urbanlens.silver.resolution import inputs, rotate_generation, run

    root = Path(__file__).parents[1]
    store = MemoryStore()
    source = portable_bundle(tmp_path, store)
    manifest = save_json(store, "manifests/source", source.model_dump(mode="json"))
    taxonomy = Taxonomy.model_validate_json((root / "config/taxonomy.json").read_text())
    classifications = build(store, source, manifest, taxonomy, Reviews(version="fixture"))[0]
    rules = Rules.model_validate_json((root / "config/identity-rules.json").read_text())
    stamp = datetime(2026, 11, 5, tzinfo=UTC)
    with connect(silver_roles["pipeline"]) as c:
        load_source(c, store, source)
        load(c, store, classifications)
        artifact_row = c.execute("SELECT artifact_id FROM ul_control.artifact LIMIT 1").fetchone()
        assert artifact_row is not None
        c.execute(
            "INSERT INTO ul_silver.area SELECT 'LSOA-2021','E01'||lpad(n::text,6,'0'),"
            "'Synthetic','E06000043',2021,public.ST_Multi(public.ST_MakeEnvelope("
            "-0.15,50.81,-0.12,50.83,4326)),1,%s FROM generate_series(1,165) n",
            (artifact_row[0],),
        )
        c.commit()
        locate(c, store, stamp, [])
        entries, _ = inputs(c)

        def ref(index: int) -> Ref:
            e = entries[index]
            return Ref(
                provider=e.provider,
                scope=e.scope,
                source_key=e.source_key,
                generation=e.generation,
                record_hash=e.record_hash,
            )

        mapping = PairReview(
            review_id="fixture",
            left=ref(0),
            right=ref(2),
            decision="same place",
            reviewer="synthetic reviewer",
            reviewed_on=date(2026, 11, 2),
            evidence=manifest.uri,
            reason="Synthetic duplicate fixture",
        )
        reviews = IdentityReviews(version="test", reviews=(mapping,) if reviewed_duplicate else ())
        first = run(c, store, rules, reviews)
        assert first["establishments"] == (3 if reviewed_duplicate else 4)
        assert first["inserted_or_changed_establishments"] == first["establishments"]
        original = dict(
            c.execute(
                "SELECT identity_id,establishment_id FROM "
                "ul_silver.establishment_source_link WHERE valid_to IS NULL"
            ).fetchall()
        )
        again = run(c, store, rules, reviews)
        assert again["inserted_or_changed_establishments"] == 0
        assert again["artifact"] == first["artifact"]
        assert (
            dict(
                c.execute(
                    "SELECT identity_id,establishment_id FROM "
                    "ul_silver.establishment_source_link WHERE valid_to IS NULL"
                ).fetchall()
            )
            == original
        )
        altered = IdentityReviews(
            version="changed", reviews=() if reviewed_duplicate else (mapping,)
        )
        with pytest.raises(
            ValueError, match="Canonical split" if reviewed_duplicate else "Canonical merge"
        ):
            run(c, store, rules, altered)
        c.rollback()
        # run() participates in the caller's transaction; commit the successful baseline
        # before checking a reviewed rotation in a separate transaction.
        run(c, store, rules, reviews)
        c.commit()
        if not reviewed_duplicate:
            old_id = next(
                r[0]
                for r in c.execute(
                    "SELECT establishment_id FROM "
                    "ul_silver.establishment_source_link l JOIN ul_silver.source_identity i "
                    "USING(identity_id) WHERE i.source_key=%s AND l.valid_to IS NULL",
                    (entries[0].source_key,),
                )
            )
            assert (
                rotate_generation(
                    c,
                    ref(0).model_dump(mode="json"),
                    "synthetic reviewer",
                    "Reviewed fixture source ID reuse",
                    manifest,
                )
                == 2
            )
            c.commit()
            load_source(c, store, source)
            load(c, store, classifications)
            locate(c, store, stamp, [])
            result = run(c, store, rules, reviews)
            assert result["source_observations"] == 4
            new = c.execute(
                "SELECT e.establishment_id,i.generation FROM "
                "ul_silver.establishment_source_link l JOIN ul_silver.source_identity i "
                "USING(identity_id) JOIN ul_silver.establishment e USING(establishment_id) "
                "WHERE i.source_key=%s AND l.valid_to IS NULL",
                (entries[0].source_key,),
            ).fetchone()
            assert new is not None and new[0] != old_id and new[1] == 2
            assert c.execute(
                "SELECT count(*) FROM ul_silver.change_event "
                "WHERE event_type<>'processing_correction'"
            ).fetchone() == (0,)
    with connect(silver_roles["api"]) as api:
        assert api.execute("SELECT count(*) FROM ul_api.establishment").fetchone() == (0,)


@pytest.mark.database
def test_reviewed_merges_field_history_replay_and_replacement(
    tmp_path: Path,
    silver_roles: dict[str, str],
) -> None:
    from urbanlens.database.migrations import connect
    from urbanlens.silver.classification import build, load, save_json
    from urbanlens.silver.eligibility import Reviews, Taxonomy, digest
    from urbanlens.silver.identity import Reviews as IdentityReviews
    from urbanlens.silver.identity import Rules
    from urbanlens.silver.load import load as load_source
    from urbanlens.silver.location_processing import run as locate
    from urbanlens.silver.resolution import inputs, run
    from urbanlens.silver.review_mappings import Application, CorrectionBatch, DecisionBatch

    root = Path(__file__).parents[1]
    store = MemoryStore()
    source = portable_bundle(tmp_path, store)
    manifest = save_json(store, "manifests/source", source.model_dump(mode="json"))
    taxonomy = Taxonomy.model_validate_json((root / "config/taxonomy.json").read_text())
    classifications = build(store, source, manifest, taxonomy, Reviews(version="fixture"))[0]
    rules = Rules.model_validate_json((root / "config/identity-rules.json").read_text())
    original = IdentityReviews(version="fixture")
    with connect(silver_roles["pipeline"]) as c:
        load_source(c, store, source)
        load(c, store, classifications)
        row = c.execute("SELECT artifact_id FROM ul_control.artifact LIMIT 1").fetchone()
        assert row is not None
        c.execute(
            "INSERT INTO ul_silver.area SELECT 'LSOA-2021','E01'||lpad(n::text,6,'0'),"
            "'Synthetic','E06000043',2021,public.ST_Multi(public.ST_MakeEnvelope("
            "-0.15,50.81,-0.12,50.83,4326)),1,%s FROM generate_series(1,165) n",
            (row[0],),
        )
        c.commit()
        locate(c, store, datetime(2026, 11, 5, tzinfo=UTC), [])
        run(c, store, rules, original)
        entries, _ = inputs(c)

        def application(name: str = "Approved Cafe", version: str = "fixture-v1") -> Application:
            def ref(index: int) -> dict[str, Any]:
                return {
                    k: v
                    for k, v in entries[index].model_dump(mode="json").items()
                    if k in {"provider", "scope", "source_key", "generation", "record_hash"}
                }

            raw = json.loads(
                (root / "config/venue-review-decisions.json").read_text(encoding="utf-8")
            )
            raw.update(
                version="fixture",
                resolution_manifest=manifest.model_dump(mode="json"),
                hospitality_scope_policy={},
                replacement_reports=["NEW"],
                unresolved_pairs=[],
                reviews=[
                    dict(
                        review_id="TEST",
                        left=ref(0),
                        right=ref(2),
                        decision="same place",
                        notes="Synthetic same-place review",
                        evidence_urls=[],
                        reviewed_on=None,
                        confirmed_on="2026-11-05",
                        pair_id=digest(sorted([entries[0].key, entries[2].key])),
                    )
                ],
            )
            corr = json.loads(
                (root / "config/venue-review-corrections.json").read_text(encoding="utf-8")
            )
            replacement = next(x for x in corr["corrections"] if x["review_id"] == "U544")
            replacement.update(review_id="NEW", superseded_source_labels=[])
            replacement["field_overrides"]["replacement_candidate"]["name"] = "New Restaurant"
            name_override = dict(
                review_id="TEST",
                kind="current_name_or_source_label",
                owner_statement="Synthetic corrected name",
                current_name=name,
                current_source=ref(0),
                superseded_source_labels=[],
                field_overrides={},
                evidence_basis="synthetic review",
                identity_decision="same place",
            )
            corr.update(
                version=version,
                source_review_manifest=manifest.model_dump(mode="json"),
                corrections=[name_override, replacement],
            )
            return Application(
                original, DecisionBatch.model_validate(raw), CorrectionBatch.model_validate(corr)
            )

        before = c.execute("SELECT count(*) FROM ul_silver.observation_version").fetchone()
        applied = run(c, store, rules, original, application=application())
        assert applied["establishments"] == 3 and applied["replacement_candidates"] == 1
        assert applied["new_field_provenance"] == 36
        assert c.execute("SELECT count(*) FROM ul_silver.identity_correction").fetchone() == (1,)
        dated_reviews = c.execute(
            "SELECT count(*) FROM ul_control.review "
            "WHERE reviewed_at IS NULL AND reviewed_on IS NOT NULL"
        ).fetchone()
        assert dated_reviews is not None and dated_reviews[0] > 0
        replacement = c.execute(
            "SELECT primary_spatial_eligible,location FROM ul_silver.establishment "
            "WHERE display_name='New Restaurant'"
        ).fetchone()
        assert replacement == (False, None)
        again = run(c, store, rules, original, application=application())
        assert again["artifact"] == applied["artifact"] and again["new_field_provenance"] == 0
        assert again["inserted_or_changed_establishments"] == 0
        with pytest.raises(ValueError, match="new version"):
            run(c, store, rules, original, application=application("Changed Cafe"))
        revised = run(
            c, store, rules, original, application=application("Changed Cafe", "fixture-v2")
        )
        assert revised["new_field_provenance"] > 0
        history = c.execute(
            "SELECT count(*) FROM ul_silver.field_provenance WHERE valid_to IS NOT NULL"
        ).fetchone()
        assert history is not None and history[0] > 0
        assert c.execute("SELECT count(*) FROM ul_silver.observation_version").fetchone() == before
        assert c.execute(
            "SELECT count(*) FROM ul_silver.change_event WHERE event_type<>'processing_correction'"
        ).fetchone() == (0,)
        assert c.execute(
            "SELECT count(*) FROM ul_silver.establishment_source_link "
            "WHERE decision='accepted' AND valid_to IS NULL"
        ).fetchone() == (4,)
    with connect(silver_roles["api"]) as api:
        assert api.execute("SELECT count(*) FROM ul_api.establishment").fetchone() == (0,)


def test_fsa_portable_pipeline_keeps_ids_rating_states_and_raw_locators(tmp_path: Path) -> None:
    source, snapshot = fsa_source(tmp_path)
    output = LocalArtifactStore(tmp_path / "output")
    bundle, manifest = build([snapshot], source, output)
    assert build([snapshot], source, output) == (bundle, manifest)
    records = list(verified_observations(output, bundle.batches[0]))
    assert [r.source_key for r in records] == ["000001", "000002", "000003", "000004"]
    assert records[0].rating is not None and records[0].rating.score == 5
    assert records[1].address.state == "withheld" and records[1].location.quality == "withheld"
    assert "Must not be exposed" not in records[1].model_dump_json()
    assert (
        records[2].rating is not None
        and records[2].rating.score is None
        and records[2].rating.state == "exempt"
    )
    assert records[3].location.quality == "invalid"
    for i, r in enumerate(records, 1):
        assert r.observed_at == snapshot.partitions[0].fetched_at
        assert r.ingested_at == snapshot.run.started_at
        assert r.source_published_at is None
        assert r.source_extract_date == date(2026, 11, 1)
        assert r.evidence[0].artifact == snapshot.partitions[0].artifacts[0]
        assert r.evidence[0].locator.endswith(f"[{i}]")
        assert r.field_sources["rating"] == "RatingValue,RatingDate"


@pytest.mark.parametrize(
    "raw,expected",
    [(None, "null"), ("", "missing"), ("not a date", "invalid"), ("2024-06-30", "published")],
)
def test_date_states_remain_distinct(raw: str | None, expected: str) -> None:
    assert n.dated(raw).state == expected
    assert n.dated(None, present=False).state == "missing"


def test_missing_address_is_not_automatically_withheld_and_rating_zero_is_real() -> None:
    value = n.fsa({"FHRSID": "123", "BusinessName": " Test ", "RatingValue": "0"}, context())
    assert value.address.state == "missing"
    assert value.rating is not None and value.rating.score == 0 and value.rating.state == "rated"
    assert value.name.raw == " Test " and value.name.value == "Test"


def test_hash_ignores_transport_dates_and_name_whitespace_but_detects_real_changes() -> None:
    raw = dict(FHRSID="123", BusinessName="  Example\u00a0Café ", RatingValue="Exempt")
    first = n.fsa(raw, context())
    another = context()
    another["observed_at"] += timedelta(days=30)
    another["ingested_at"] = datetime(2026, 11, 5, tzinfo=UTC)
    another["source_extracted_at"] = datetime(2026, 10, 30, tzinfo=UTC)
    another["evidence"] = (
        Evidence(
            artifact=Artifact(
                uri="s3://other/bronze/file", sha256="b" * 64, byte_count=2, media_type="text/xml"
            ),
            locator="row/99",
        ),
    )
    same = n.fsa({**raw, "BusinessName": "Example Café"}, another)
    assert first.record_hash == same.record_hash
    assert first.record_hash != n.fsa({**raw, "RatingValue": "5"}, another).record_hash
    assert (
        n.fsa({**raw, "RatingValue": "unknown-one"}, another).record_hash
        != n.fsa({**raw, "RatingValue": "unknown-two"}, another).record_hash
    )
    broken = first.model_dump(mode="json")
    broken["name"]["value"] = "Changed without updating hash"
    with pytest.raises(ValidationError, match="hash mismatch"):
        Observation.model_validate(broken)


def test_osm_domains_tag_order_and_free_text_punctuation() -> None:
    row: dict[str, Any] = dict(
        element_type="way",
        osm_id=42,
        tags={"name": "Fish; Chips", "amenity": "restaurant;cafe", "fhrs:id": "001;002"},
        geometry_state="polygon-point-on-surface",
        longitude=-0.14,
        latitude=50.82,
        geometry=None,
        original_geometry={},
        fhrs_ids=["001", "002"],
        invalid_fhrs_tokens=[],
        in_city=True,
        crosses_city_boundary=False,
    )
    first = n.osm(row, context())
    assert first.source_key == "way/42" and first.name.value == "Fish; Chips"
    assert first.location.quality == "point_on_surface"
    assert first.tags["amenity"] == ("cafe", "restaurant")
    same = n.osm({**row, "tags": {**row["tags"], "amenity": "cafe;restaurant"}}, context())
    assert first.record_hash == same.record_hash
    node = n.osm({**row, "element_type": "node", "geometry_state": "source-node"}, context())
    assert node.source_key == "node/42" and node.record_hash != first.record_hash


@pytest.mark.parametrize("number", ["00000001", "SC000001", "NI000001"])
def test_company_numbers_are_strings_offices_are_not_premises_and_dates_are_honest(
    number: str,
) -> None:
    raw = dict(
        CompanyNumber=number,
        CompanyName="  Example Ltd  ",
        CompanyStatus="Active",
        CompanyCategory="Private Limited Company",
        IncorporationDate="25/12/2001",
        DissolutionDate="31/02/2024",
        **{
            "RegAddress.PostCode": " bn1  1aa ",
            "RegAddress.AddressLine1": " 1 Street ",
            "SICCode.SicText_1": "56101 - Restaurants",
        },
    )
    value = n.company(raw, context(), profile=False)
    assert value.source_key == number and value.name.raw == "  Example Ltd  "
    assert value.postcode.value == "BN1 1AA"
    assert value.company is not None and value.company.incorporated_on.value == date(2001, 12, 25)
    assert value.company.ceased_on.value is None and value.company.ceased_on.state == "invalid"
    assert value.company.sic_codes == ("56101",)
    assert (
        value.location.role == "registered_office" and not value.location.primary_spatial_eligible
    )


@pytest.mark.parametrize("state", ["invalid", "unmatched", "unavailable", "throttled"])
def test_postcode_failure_states_do_not_turn_into_precise_points(state: str) -> None:
    value = Lookup.model_validate(
        dict(
            raw_input="bad" if state == "invalid" else "BN1 1AA",
            postcode="BAD" if state == "invalid" else "BN11AA",
            state=state,
        )
    )
    ctx = {**context(), "snapshot_id": None, "cycle_id": None}
    result = n.postcode_lookup(value, ctx)
    assert result.postcode_result is not None and result.postcode_result.state == state
    assert result.location.longitude is None and not result.location.primary_spatial_eligible


def test_postcode_centroid_and_unknown_vintage_are_explicit() -> None:
    result = n.postcode_lookup(
        Lookup(
            raw_input="bn1 1aa",
            postcode="BN11AA",
            state="resolved",
            longitude=-0.14,
            latitude=50.82,
        ),
        {**context(), "snapshot_id": None, "cycle_id": None},
    )
    assert (
        result.location.quality == "postcode_fallback"
        and not result.location.primary_spatial_eligible
    )
    assert result.postcode_result is not None and result.postcode_result.dataset_version is None


def test_ons_reference_date_and_denominators_remain_typed() -> None:
    row = dict(
        lsoa21cd="E01000001",
        name="Example area",
        authority="E06000043",
        geography_vintage="LSOA-2021",
        reference_period="2024-06-30",
        population=100,
        age_0_15=20,
        age_16_64=60,
        age_65_plus=20,
        land_km2="0.125",
        geometry={"type": "MultiPolygon", "coordinates": []},
        product_editions={"population": "Mid-2024"},
    )
    value = n.area(row, context())
    assert value.area is not None and value.area.reference_date == date(2024, 6, 30)
    assert value.area.official_land_km2 == "0.125" and value.area.population == 100
    with pytest.raises(ValidationError, match="reconcile"):
        n.area({**row, "population": 99}, context())


def test_rejected_snapshot_cannot_produce_a_silver_manifest(tmp_path: Path) -> None:
    source, snapshot = fsa_source(tmp_path)
    # A complete-but-unaccepted collection must never become model input.
    unaccepted = Snapshot.model_validate(
        {**snapshot.model_dump(), "run": {**snapshot.run.model_dump(), "status": "complete"}}
    )
    from urbanlens.ingestion.engine import metadata_key

    source2 = LocalArtifactStore(tmp_path / "unaccepted")
    source2.put_immutable(
        metadata_key(unaccepted.cycle, "accepted"),
        iter([unaccepted.model_dump_json().encode()]),
        media_type="application/json",
        compression="none",
    )
    with pytest.raises(ValueError, match="accepted complete"):
        build([unaccepted], source2, LocalArtifactStore(tmp_path / "output"))
