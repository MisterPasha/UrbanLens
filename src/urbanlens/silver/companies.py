"""Evidence-bound legal context; registered offices never become venues."""

from __future__ import annotations

from datetime import date
from typing import Any, Literal, Self
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import Field, model_validator

from urbanlens.ingestion.companies import SICS
from urbanlens.ingestion.contracts import Artifact, Contract, Digest
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest
from urbanlens.silver.load import artifact_id, identifier

ENGINE = "company-context-v1"


class Link(Contract):
    company_number: str = Field(pattern=r"^[A-Z0-9]{8}$")
    establishment_id: UUID
    company_record_hash: Digest
    establishment_hash: Digest
    relationship: Literal["operator", "franchisee"] = "operator"
    decision: Literal["approved", "rejected"]
    reviewer: str = Field(min_length=1)
    reviewed_on: date
    evidence_url: str = Field(pattern=r"^https://[^\s]+$")
    evidence: Artifact
    # The retained document must explicitly identify the legal operator and premise.
    operating_statement: str = Field(min_length=20)
    reason: str = Field(min_length=10)

    @model_validator(mode="after")
    def portable_evidence(self) -> Self:
        if not self.evidence.uri.startswith("s3://"):
            raise ValueError("Operating evidence must be retained in private S3")
        if self.reviewed_on > date.today():
            raise ValueError("Review date cannot be in the future")
        return self


class Register(Contract):
    version: str = Field(pattern=r"^company-links-v[0-9]+$")
    links: tuple[Link, ...] = ()
    note: str

    @model_validator(mode="after")
    def unique_links(self) -> Self:
        keys = [(x.company_number, x.establishment_id, x.relationship) for x in self.links]
        if len(keys) != len(set(keys)):
            raise ValueError("Duplicate or contradictory company relationship reviews")
        return self


def venue_hash(row: dict[str, Any]) -> str:
    """Bind approval to the canonical venue's current meaning, not its refresh time."""
    return digest(
        {
            k: str(row[k]) if isinstance(row[k], UUID) else row[k]
            for k in (
                "establishment_id",
                "display_name",
                "address",
                "postcode",
                "eligibility",
                "duplicate_status",
                "identity_rule_version",
                "observation_state",
            )
        }
    )


def context(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Use latest collected legal facts; profile breaks equal-time ties with bulk."""
    rows = sorted(
        rows,
        key=lambda r: (
            r["last_observed_at"],
            bool(r["source_fields"]["company"]["profile"]),
            str(r["observation_id"]),
        ),
        reverse=True,
    )
    chosen = rows[0]
    facts = chosen["source_fields"]["company"]
    bulk = next(
        (r for r in rows if r["source_fields"]["company"]["registered_office_cohort"]), None
    )
    local = bool(
        bulk
        and chosen["postcode_normalised"]
        and chosen["postcode_normalised"] == bulk["postcode_normalised"]
        and set(facts["sic_codes"]) & SICS
    )
    status = facts["legal_status"]["value"]
    ceased = facts["ceased_on"]
    dissolved = str(status).lower() in {"dissolved", "converted-closed", "closed"}
    return dict(
        company_number=chosen["source_key"],
        legal_name=chosen["name_normalised"],
        legal_status=status or "unavailable",
        company_type=facts["company_type"]["value"] or "unavailable",
        sic_codes=facts["sic_codes"],
        sic_state=facts["sic_state"],
        incorporation=facts["incorporated_on"],
        cessation=ceased,
        cessation_interpretation=(
            "reported_cessation"
            if ceased["value"]
            else "invalid_date"
            if ceased["state"] == "invalid"
            else "date_unavailable"
            if dissolved
            else "no_cessation_reported"
        ),
        registered_office=dict(
            address=chosen["address_normalised"],
            postcode=chosen["postcode_normalised"],
            address_state=chosen["address_state"],
            location_quality="missing",
            purpose="registered office; not evidence of a trading premise",
        ),
        registered_office_cohort=local,
        registered_office_basis=(
            dict(
                basis="hospitality SIC and accepted local registered-office postcode evidence",
                authority_code="E06000043",
                observation_id=str(bulk["observation_id"]),
                observed_at=bulk["last_observed_at"].isoformat(),
            )
            if local and bulk
            else None
        ),
        profile_url="https://find-and-update.company-information.service.gov.uk/company/"
        + chosen["source_key"],
        observed_at=chosen["last_observed_at"].isoformat(),
        first_observed_at=min(r["first_observed_at"] for r in rows).isoformat(),
        profile_observed_at=max(
            (
                r["last_observed_at"].isoformat()
                for r in rows
                if r["source_fields"]["company"]["profile"]
            ),
            default=None,
        ),
        source_observation_id=str(chosen["observation_id"]),
        record_hash=chosen["record_hash"],
        sources=[
            dict(
                observation_id=str(r["observation_id"]),
                scope=r["scope_key"],
                record_hash=r["record_hash"],
                observed_at=r["last_observed_at"].isoformat(),
                artifact=r["source_fields"]["silver_artifact"],
                fields=r["source_fields"]["field_sources"],
            )
            for r in rows
        ],
        warnings=(
            ["Profile office differs from accepted local bulk office; local cohort withheld"]
            if bulk and not local
            else []
        ),
    )


def run(
    connection: psycopg.Connection[Any], store: ArtifactStore, register: Register
) -> dict[str, Any]:
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT o.*,i.source_key,s.scope_key FROM ul_silver.observation_version o "
                "JOIN ul_silver.source_identity i USING(identity_id) "
                "JOIN ul_control.source_scope s USING(scope_id) "
                "LEFT JOIN ul_silver.identity_scope_state st ON "
                "st.current_observation_id=o.observation_id "
                "AND st.scope_id=o.scope_id AND st.identity_id=o.identity_id "
                "WHERE o.provider='companies-house' AND o.valid_to IS NULL "
                "AND i.retired_at IS NULL "
                "AND coalesce(st.observation_state,'observed')<>'no_longer_observed' "
                "ORDER BY i.source_key,s.scope_key"
            )
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in cursor.fetchall():
                grouped.setdefault(row["source_key"], []).append(row)
            companies = {n: context(rows) for n, rows in grouped.items()}
            cursor.execute(
                "SELECT e.* FROM ul_silver.establishment e WHERE EXISTS (SELECT 1 FROM "
                "ul_silver.establishment_source_link l WHERE l.establishment_id=e.establishment_id "
                "AND l.valid_to IS NULL AND l.decision='accepted') OR EXISTS (SELECT 1 FROM "
                "ul_silver.reviewed_replacement r WHERE r.establishment_id=e.establishment_id)"
            )
            venues = {str(r["establishment_id"]): r for r in cursor.fetchall()}
        if not companies:
            raise ValueError("No accepted company observations loaded")
        for link in register.links:
            company = companies.get(link.company_number)
            venue = venues.get(str(link.establishment_id))
            if company is None or venue is None:
                raise ValueError("Company review refers to an absent company or current venue")
            if (
                company["record_hash"] != link.company_record_hash
                or venue_hash(venue) != link.establishment_hash
            ):
                raise ValueError("Stale company relationship review; refresh and re-review")
            if link.decision == "approved" and (
                venue["eligibility"] != "eligible"
                or venue["duplicate_status"] != "resolved"
                or venue["observation_state"] == "no_longer_observed"
            ):
                raise ValueError("Operator link requires a current resolved eligible establishment")
            b"".join(store.read_verified(link.evidence))
        definition = register.model_dump(mode="json")
        definition_artifact = save_json(store, "evidence/company-link-reviews", definition)
        definition_id = artifact_id(connection, definition_artifact)
        version = ENGINE + "/" + register.version
        old = connection.execute(
            "SELECT sha256 FROM ul_control.review_definition WHERE version=%s", (version,)
        ).fetchone()
        if old and old[0] != definition_artifact.sha256:
            raise ValueError("Company register changed without a new version")
        connection.execute(
            "INSERT INTO ul_control.review_definition VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (version, definition_artifact.sha256, definition_id),
        )
        changed = 0
        # Preserve retained companies but remove current cohort membership without current support.
        connection.execute(
            "UPDATE ul_silver.company SET registered_office_cohort=false WHERE "
            "registered_office_cohort AND NOT(company_number=ANY(%s))",
            (list(companies),),
        )
        for number, body in companies.items():
            changed += connection.execute(
                "INSERT INTO ul_silver.company(company_number,legal_name,legal_status,company_type,"
                "sic_codes,incorporation_date,cessation_date,registered_office,registered_office_postcode,"
                "registered_office_cohort,first_observed_at,last_observed_at,profile_observed_at,"
                "source_observation_id,context,context_sha256) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(company_number) DO UPDATE SET legal_name=excluded.legal_name,"
                "legal_status=excluded.legal_status,company_type=excluded.company_type,sic_codes=excluded.sic_codes,"
                "incorporation_date=excluded.incorporation_date,cessation_date=excluded.cessation_date,"
                "registered_office=excluded.registered_office,registered_office_postcode=excluded.registered_office_postcode,"
                "registered_office_cohort=excluded.registered_office_cohort,last_observed_at=excluded.last_observed_at,"
                "profile_observed_at=excluded.profile_observed_at,source_observation_id=excluded.source_observation_id,"
                "context=excluded.context,context_sha256=excluded.context_sha256 "
                "WHERE ul_silver.company.context_sha256 IS DISTINCT FROM excluded.context_sha256 "
                "OR ul_silver.company.registered_office_cohort IS DISTINCT FROM "
                "excluded.registered_office_cohort",
                (
                    number,
                    body["legal_name"],
                    body["legal_status"],
                    body["company_type"],
                    body["sic_codes"],
                    body["incorporation"]["value"],
                    body["cessation"]["value"],
                    Jsonb(body["registered_office"]),
                    body["registered_office"]["postcode"],
                    body["registered_office_cohort"],
                    body["first_observed_at"],
                    body["observed_at"],
                    body["profile_observed_at"],
                    body["source_observation_id"],
                    Jsonb(body),
                    digest(body),
                ),
            ).rowcount
        current_reviews = []
        for link in register.links:
            action = link.model_dump(mode="json")
            review_id = identifier(
                "company-review/" + definition_artifact.sha256 + "/" + digest(action)
            )
            current_reviews.append(review_id)
            connection.execute(
                "INSERT INTO ul_control.review(review_id,review_type,establishment_id,"
                "company_number,"
                "decision,reason,evidence_uri,evidence_sha256,rule_version,reviewer,reviewed_on) "
                "VALUES (%s,'company_link',%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    review_id,
                    link.establishment_id,
                    link.company_number,
                    link.decision,
                    link.reason,
                    link.evidence.uri,
                    link.evidence.sha256,
                    version,
                    link.reviewer,
                    link.reviewed_on,
                ),
            )
            connection.execute(
                "INSERT INTO ul_silver.review_mapping(mapping_key,review_id,"
                "definition_version,action) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (digest(dict(version=version, action=action)), review_id, version, Jsonb(action)),
            )
        # A new complete register retires removed/rejected/superseded approvals, retaining history.
        connection.execute(
            "UPDATE ul_silver.company_establishment_link l SET valid_to=clock_timestamp() "
            "FROM ul_control.review r WHERE r.review_id=l.review_id AND l.valid_to IS NULL "
            "AND r.rule_version LIKE 'company-context-v1/%%' AND NOT(l.review_id=ANY(%s::uuid[]))",
            (current_reviews,),
        )
        for link, review_id in zip(register.links, current_reviews, strict=True):
            if link.decision == "approved":
                connection.execute(
                    "INSERT INTO ul_silver.company_establishment_link(link_id,company_number,"
                    "establishment_id,review_id,relationship,evidence_uri,valid_from) "
                    "VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp()) ON CONFLICT DO NOTHING",
                    (
                        identifier("company-link/" + str(review_id)),
                        link.company_number,
                        link.establishment_id,
                        review_id,
                        link.relationship,
                        link.evidence.uri,
                    ),
                )
        eligible = {
            k: v
            for k, v in venues.items()
            if v["eligibility"] == "eligible"
            and v["duplicate_status"] == "resolved"
            and v["observation_state"] != "no_longer_observed"
        }
        linked = {
            str(r[0])
            for r in connection.execute(
                "SELECT DISTINCT establishment_id FROM ul_silver.company_establishment_link "
                "WHERE valid_to IS NULL "
                "AND relationship IN ('operator','franchisee')"
            )
        }
        cohort_counts = dict(
            connection.execute(
                "SELECT cohort,count(*) FROM ul_silver.company_cohort GROUP BY cohort"
            ).fetchall()
        )
        report = dict(
            engine=ENGINE,
            register=definition_artifact.model_dump(mode="json"),
            companies=list(companies.values()),
            reviewed_links=definition["links"],
            relationship_review_inputs=[
                dict(
                    establishment_id=k,
                    display_name=v["display_name"],
                    address=v["address"],
                    postcode=v["postcode"],
                    establishment_hash=venue_hash(v),
                )
                for k, v in sorted(eligible.items())
            ],
            cohorts=dict(
                registered_office=cohort_counts.get("registered_office", 0),
                verified_operator=cohort_counts.get("verified_operator", 0),
            ),
            coverage=dict(
                universe="current resolved eligible venues, including all coordinate qualities",
                denominator=len(eligible),
                verified_establishments=len(linked & eligible.keys()),
                percent=100 * len(linked & eligible.keys()) / len(eligible) if eligible else None,
            ),
            zero_link_reason="No operating relationship evidence has been reviewed"
            if not register.links
            else None,
            source_freshness_changed=False,
            lifecycle_events_created=0,
        )
        artifact = save_json(store, "silver/company-context", report)
        output_id = artifact_id(connection, artifact)
        connection.execute(
            "INSERT INTO ul_silver.company_context_run(artifact_id,definition_version) "
            "VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (output_id, version),
        )
        from urbanlens.silver.history import project

        history_events = project(connection, store, "company")
        return dict(
            artifact=artifact.model_dump(mode="json"),
            company_count=len(companies),
            changed_companies=changed,
            cohorts=report["cohorts"],
            coverage=report["coverage"],
            zero_link_reason=report["zero_link_reason"],
            history_events=history_events,
        )
