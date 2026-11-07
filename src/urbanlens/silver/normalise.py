"""Conservative field rules; original evidence is retained separately from values."""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Any

from urbanlens.ingestion.postcodes import EDITION, POSTCODE, Lookup
from urbanlens.silver.models import (
    Address,
    Area,
    Company,
    DateValue,
    Location,
    Observation,
    Postcode,
    Rating,
    Text,
    observation,
)


def clean(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).split())


def text(raw: str | None, *, present: bool = True) -> Text:
    return Text(
        raw=raw,
        value=None if raw is None else clean(raw) or None,
        state="missing"
        if not present or raw == ""
        else "null"
        if raw is None
        else "published"
        if clean(raw)
        else "missing",
    )


def postcode(raw: str | None, *, present: bool = True) -> Text:
    base = text(raw, present=present)
    if base.value is None:
        return base
    token = "".join(base.value.upper().split())
    return Text(
        raw=raw,
        value=token[:-3] + " " + token[-3:] if POSTCODE.fullmatch(token) else None,
        state="published" if POSTCODE.fullmatch(token) else "invalid",
    )


def dated(raw: str | None, *, bulk: bool = False, present: bool = True) -> DateValue:
    base = text(raw, present=present)
    if base.value is None:
        return DateValue(raw=raw, state=base.state)
    try:
        value = (
            datetime.strptime(base.value, "%d/%m/%Y").date()
            if bulk
            else date.fromisoformat(base.value)
        )
    except ValueError:
        return DateValue(raw=raw, state="invalid")
    return DateValue(raw=raw, value=value, state="published")


def address(raw: dict[str, Any] | None, fields: tuple[str, ...]) -> Address:
    if raw is None:
        return Address(state="null")
    value = ", ".join(clean(str(raw[k])) for k in fields if raw.get(k) and clean(str(raw[k])))
    return Address(raw=raw, value=value or None, state="published" if value else "missing")


def point(lon: Any, lat: Any, quality: str = "source_point", role: str = "premises") -> Location:
    if lon is None and lat is None:
        return Location()
    try:
        if (
            isinstance(lon, bool)
            or isinstance(lat, bool)
            or (float(lon) == float(lat) == 0 and role == "premises")
        ):
            raise ValueError("Invalid premises coordinate")
        return Location.model_validate(
            dict(
                longitude=float(lon),
                latitude=float(lat),
                quality=quality,
                role=role,
                primary_spatial_eligible=role == "premises" and quality != "postcode_fallback",
            )
        )
    except (TypeError, ValueError):
        return Location(quality="invalid")


def fsa(row: dict[str, Any], context: dict[str, Any]) -> Observation:
    raw_address = {
        k: row.get(k)
        for k in ("AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4", "PostCode")
    }
    markers = " ".join(v or "" for v in raw_address.values()).casefold()
    held = any(
        marker in markers
        for marker in (
            "private address",
            "address not disclosed",
            "address withheld",
            "address suppressed",
        )
    ) or any(
        str(row.get(k, "")).strip().casefold() == "true"
        for k in ("IsAddressPrivate", "AddressIsPrivate", "AddressWithheld", "PrivacySuppressed")
    )
    addr = address(raw_address, ("AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4"))
    pc = postcode(row.get("PostCode"), present="PostCode" in row)
    location = point(row.get("Longitude") or None, row.get("Latitude") or None)
    if held:
        addr, pc, location = (
            Address(state="withheld"),
            Text(state="withheld"),
            Location(quality="withheld"),
        )
    elif not any(raw_address.values()):
        # An absent address is not proof that the source explicitly withheld it.
        addr, pc, location = Address(raw=raw_address), Text(), Location()
    raw_rating = row.get("RatingValue")
    token = "" if raw_rating is None else raw_rating.strip()
    state = (
        "rated"
        if token in {"0", "1", "2", "3", "4", "5"}
        else {
            "AwaitingInspection": "awaiting_inspection",
            "AwaitingPublication": "awaiting_publication",
            "Exempt": "exempt",
            "": "missing",
        }.get(token, "unknown")
    )
    rating = Rating.model_validate(
        dict(
            raw=raw_rating,
            score=int(token) if state == "rated" else None,
            state=state,
            assessed_on=dated(row.get("RatingDate"), present="RatingDate" in row),
        )
    )
    return observation(
        **context,
        provider="fsa",
        source_key=str(row["FHRSID"]).strip(),
        record_kind="venue",
        name=text(row.get("BusinessName"), present="BusinessName" in row),
        address=addr,
        postcode=pc,
        location=location,
        rating=rating,
        source_values={
            k: row.get(k)
            for k in ("BusinessTypeID", "BusinessType", "SchemeType", "NewRatingPending", "Scores")
        },
        field_sources={
            "source_key": "FHRSID",
            "name": "BusinessName",
            "address": "AddressLine1..4/privacy flags",
            "postcode": "PostCode/privacy flags",
            "location": "Geocode/Longitude,Latitude/privacy flags",
            "rating": "RatingValue,RatingDate",
            "source_values": "BusinessTypeID,BusinessType,SchemeType,NewRatingPending,Scores",
        },
    )


def osm(row: dict[str, Any], context: dict[str, Any]) -> Observation:
    raw_tags = row["tags"]
    tags: dict[str, tuple[str, ...]] = {}
    for key, value in raw_tags.items():
        canonical = clean(key).casefold()
        if canonical in tags:
            raise ValueError("OSM tag-key normalisation collision")
        # Name/free-text punctuation is meaningful; only known set-valued tags split.
        parts = (
            value.split(";") if canonical in {"amenity", "shop", "cuisine", "fhrs:id"} else [value]
        )
        tags[canonical] = tuple(sorted({clean(v) for v in parts if clean(v)}))
    raw_address = {k: v for k, v in raw_tags.items() if k.startswith("addr:")}
    quality = "source_point" if row["geometry_state"] == "source-node" else "point_on_surface"
    location = point(row["longitude"], row["latitude"], quality)
    if location.quality == "missing":
        location = Location(quality="unresolved")
    return observation(
        **context,
        provider="osm",
        source_key=f"{row['element_type']}/{row['osm_id']}",
        record_kind="venue",
        name=text(raw_tags.get("name"), present="name" in raw_tags),
        address=address(
            raw_address,
            ("addr:full",)
            if raw_address.get("addr:full")
            else ("addr:housename", "addr:housenumber", "addr:street", "addr:suburb", "addr:city"),
        ),
        postcode=postcode(raw_tags.get("addr:postcode"), present="addr:postcode" in raw_tags),
        location=location,
        tags_raw=raw_tags,
        tags=tags,
        source_values={
            "geometry": row["geometry"],
            "geometry_state": row["geometry_state"],
            "original_geometry": row["original_geometry"],
            "fhrs_ids": row["fhrs_ids"],
            "invalid_fhrs_tokens": row["invalid_fhrs_tokens"],
            "in_city": row["in_city"],
            "crosses_city_boundary": row["crosses_city_boundary"],
        },
        field_sources={
            "source_key": "type,id",
            "name": "tags/name",
            "address": "tags/addr:*",
            "postcode": "tags/addr:postcode",
            "tags": "tags",
            "location": "lat,lon/geometry/members -> point-on-surface",
            "source_values": "geometry,members,nodes,bounds,tags/fhrs:id",
        },
    )


def company(row: dict[str, Any], context: dict[str, Any], *, profile: bool) -> Observation:
    fields: tuple[str, ...]
    if profile:
        number, name, status, kind = (
            row[k] for k in ("company_number", "company_name", "company_status", "type")
        )
        office = row.get("registered_office_address")
        pc = None if office is None else office.get("postal_code")
        sics = tuple(sorted(set(row.get("sic_codes", []))))
        incorporation, cessation = row.get("date_of_creation"), row.get("date_of_cessation")
        fields = (
            "care_of",
            "po_box",
            "premises",
            "address_line_1",
            "address_line_2",
            "locality",
            "region",
            "country",
        )
    else:
        number, name, status, kind = (
            row[k] for k in ("CompanyNumber", "CompanyName", "CompanyStatus", "CompanyCategory")
        )
        office = {k: v for k, v in row.items() if k.startswith("RegAddress.")}
        pc = row.get("RegAddress.PostCode")
        sics = tuple(
            sorted(
                {
                    m[1]
                    for i in range(1, 5)
                    if (
                        m := re.match(
                            r"^(\d{5})(?:\s|$)", row.get(f"SICCode.SicText_{i}", "").strip()
                        )
                    )
                }
            )
        )
        incorporation, cessation = row.get("IncorporationDate"), row.get("DissolutionDate")
        fields = tuple(
            "RegAddress." + k
            for k in (
                "CareOf",
                "POBox",
                "AddressLine1",
                "AddressLine2",
                "PostTown",
                "County",
                "Country",
            )
        )
    if not all(re.fullmatch(r"[0-9]{5}", code) for code in sics):
        raise ValueError("Invalid SIC code")
    raw_sics = (
        row.get("sic_codes", [])
        if profile
        else [row.get(f"SICCode.SicText_{i}", "") for i in range(1, 5)]
    )
    has_sics = any(str(v).strip() and str(v).strip() != "None Supplied" for v in raw_sics)
    invalid_sics = any(
        str(v).strip()
        and str(v).strip() != "None Supplied"
        and not re.match(r"^\d{5}(?:\s|$)", str(v).strip())
        for v in raw_sics
    )
    return observation(
        **context,
        provider="companies-house",
        source_key=number.strip(),
        record_kind="company",
        name=text(name),
        address=address(office, fields),
        postcode=postcode(pc),
        location=Location(role="registered_office"),
        company=Company(
            legal_status=text(status),
            company_type=text(kind),
            sic_codes=sics,
            sic_state="invalid" if invalid_sics else "published" if has_sics else "missing",
            incorporated_on=dated(
                incorporation, bulk=not profile, present=not profile or "date_of_creation" in row
            ),
            ceased_on=dated(
                cessation, bulk=not profile, present=not profile or "date_of_cessation" in row
            ),
            profile=profile,
            registered_office_cohort=bool(row.get("_cohort", False)),
        ),
        source_values={
            "sic_raw": row.get("sic_codes")
            if profile
            else {k: v for k, v in row.items() if k.startswith("SICCode.")}
        },
        field_sources={
            "source_key": "company_number" if profile else "CompanyNumber",
            "name": "company_name" if profile else "CompanyName",
            "address": "registered_office_address" if profile else "RegAddress.*",
            "postcode": "registered_office_address/postal_code"
            if profile
            else "RegAddress.PostCode",
            "company": "company_status,type,sic_codes,date_of_creation,date_of_cessation"
            if profile
            else "CompanyStatus,CompanyCategory,SICCode.*,IncorporationDate,DissolutionDate",
            "location": "no trading-premises location asserted",
        },
    )


def area(row: dict[str, Any], context: dict[str, Any]) -> Observation:
    editions = row["product_editions"]
    return observation(
        **context,
        provider="ons",
        source_key=row["lsoa21cd"],
        record_kind="area",
        name=text(row["name"]),
        area=Area(
            authority_code=row["authority"],
            geography_version=row["geography_vintage"],
            population_edition=json_edition(editions["population"]),
            reference_date=date.fromisoformat(row["reference_period"]),
            population=row["population"],
            age_0_15=row["age_0_15"],
            age_16_64=row["age_16_64"],
            age_65_plus=row["age_65_plus"],
            official_land_km2=row["land_km2"],
            geometry=row["geometry"],
            editions=editions,
        ),
        field_sources={
            "source_key": "membership/LSOA21CD",
            "name": "membership/LSOA21NM",
            "area.authority_code": "membership/LAD22CD",
            "area.geometry": "boundary/LSOA21CD geometry",
            "area.official_land_km2": "land_area/LSOA21CD -> hectares / 100",
            "area.population": "population/LSOA code,total and broad ages",
            "area.reference_date": "pinned population reference date",
        },
    )


def json_edition(value: Any) -> str:
    import hashlib
    import json

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def postcode_lookup(value: Lookup, context: dict[str, Any]) -> Observation:
    pc = postcode(value.raw_input)
    location = point(value.longitude, value.latitude, "postcode_fallback", "postcode_centroid")
    return observation(
        **context,
        provider="postcodes-io",
        source_key=value.postcode,
        record_kind="postcode",
        postcode=pc,
        location=location,
        postcode_result=Postcode(
            state=value.state,
            authority_code=value.authority,
            lsoa21=value.lsoa21,
            quality=value.quality,
            dataset_version=value.source_edition,
            edition_note=EDITION,
            expires_at=value.expires_at,
        ),
        field_sources={
            "postcode": "query,result/postcode",
            "location": "result/longitude,latitude (centroid)",
            "postcode_result": "result/codes,result/quality,result/dataset_version",
        },
    )
