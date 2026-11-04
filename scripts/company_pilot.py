"""Step 07 local company/postcode feasibility; no operator links or cloud writes."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/pilots/companies/2026-11-02"
SICS = {"56101", "56102", "56103", "56302", "10710", "47240"}
LAD = "E06000043"
SOURCE_EDITION = "ONSPD August 2026 (provider documentation; responses omit edition)"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def postcode(value: str) -> str:
    return "".join(value.upper().split())


def sic_codes(row: dict[str, str]) -> list[str]:
    result = []
    for index in range(1, 5):
        match = re.match(r"^(\d{5})(?:\s|$)", row[f"SICCode.SicText_{index}"].strip())
        if match and match[1] not in result:
            result.append(match[1])
    return result


def parts_from_listing(html: str) -> list[tuple[str, str, int, int]]:
    matches = re.findall(r'href="(BasicCompanyData-([0-9-]+)-part(\d+)_(\d+)\.zip)"', html)
    require(bool(matches), "No bulk part set in listing")
    parts = [(name, edition, int(index), int(total)) for name, edition, index, total in matches]
    editions, totals = {p[1] for p in parts}, {p[3] for p in parts}
    require(len(editions) == len(totals) == 1, "Mixed bulk editions/part counts")
    total = parts[0][3]
    require(
        len(parts) == total and sorted(p[2] for p in parts) == list(range(1, total + 1)),
        "Missing or duplicate company bulk part",
    )
    return sorted(parts, key=lambda p: p[2])


def fetch() -> None:
    """Acquire/replay this dated pilot, retaining URLs, hashes and every required part."""
    RAW.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW / "manifest.json"
    records = read_json(manifest_path) if manifest_path.exists() else {}

    def acquire(name: str, url: str, limit: int = 10_000_000) -> Path:
        path = RAW / name
        if path.exists():
            require(name in records, f"Missing provenance for {name}")
            require(sha(path) == records[name]["sha256"], f"Checksum mismatch: {name}")
            return path
        request = urllib.request.Request(url, headers={"User-Agent": "UrbanLens/0.1"})
        partial = path.with_suffix(path.suffix + ".part")
        size = 0
        with urllib.request.urlopen(request, timeout=60) as response, partial.open("wb") as stream:
            expected = response.headers.get("Content-Length")
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                require(size <= limit, "Acquisition size budget exceeded")
                stream.write(chunk)
            require(expected is None or size == int(expected), "Truncated acquisition")
        if name.endswith(".json"):
            require("error" not in read_json(partial), "Postcode directory provider error")
        partial.replace(path)
        records[name] = {
            "url": url,
            "bytes": size,
            "sha256": sha(path),
            "fetched_at": datetime.now(UTC).isoformat(),
        }
        write_json(manifest_path, records)
        print("Acquired", name, flush=True)
        return path

    listing = acquire("listing.html", "https://download.companieshouse.gov.uk/en_output.html")
    parts = parts_from_listing(listing.read_text(encoding="utf-8"))
    require(parts[0][1] == "2026-11-01", "Listing edition changed; create a new dated pilot")
    for name, edition, index, total in parts:
        acquire(name, "https://download.companieshouse.gov.uk/" + name, 120_000_000)
        records[name].update(edition=edition, part=index, total_parts=total)
    write_json(manifest_path, records)
    item = read_json(
        acquire(
            "onspd-item.json",
            "https://www.arcgis.com/sharing/rest/content/items/189f78ab06fd4df4a35fe806d022c563?f=json",
        )
    )
    base = item["url"] + "/0"
    acquire("onspd-layer.json", base + "?f=json")
    where = "lad26cd='E06000043'"
    count = read_json(
        acquire(
            "onspd-count.json",
            base
            + "/query?"
            + urllib.parse.urlencode({"f": "json", "where": where, "returnCountOnly": "true"}),
        )
    )["count"]
    require(0 < count <= 20_000, "Unexpected postcode directory workload")
    for offset in range(0, count, 1000):
        acquire(
            f"onspd-{offset // 1000:02}.json",
            base
            + "/query?"
            + urllib.parse.urlencode(
                {
                    "f": "json",
                    "where": where,
                    "outFields": "ObjectId,pcds,doterm,lad26cd,lsoa21cd",
                    "returnGeometry": "false",
                    "orderByFields": "ObjectId",
                    "resultOffset": offset,
                    "resultRecordCount": 1000,
                }
            ),
        )
    local_directory()
    print("Acquisition complete; all required parts and postcode pages verified")


def local_directory() -> dict[str, dict[str, Any]]:
    count = read_json(RAW / "onspd-count.json")["count"]
    result: dict[str, dict[str, Any]] = {}
    for index in range((count + 999) // 1000):
        payload = read_json(RAW / f"onspd-{index:02}.json")
        require("error" not in payload, "Postcode directory provider error")
        for feature in payload["features"]:
            row = feature["attributes"]
            key = postcode(row["pcds"])
            require(
                row["lad26cd"] == LAD and key not in result,
                "Directory authority/duplicate mismatch",
            )
            result[key] = row
    require(len(result) == count, "Incomplete local postcode directory")
    return result


def filter_bulk() -> None:
    """Stream every full CSV; keep only relevant SIC + official local-outcode candidates."""
    started = time.perf_counter()
    manifest = read_json(RAW / "manifest.json")
    for name, record in manifest.items():
        require(sha(RAW / name) == record["sha256"], f"Source checksum mismatch: {name}")
    parts = parts_from_listing((RAW / "listing.html").read_text(encoding="utf-8"))
    directory = local_directory()
    outcodes = {key[:-3] for key in directory}
    statistics: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    candidate_keys: set[str] = set()
    national_matches = missing_postcodes = nonfirst_matches = 0
    schema: list[str] | None = None
    observed_sics: Counter[str] = Counter()
    for name, edition, index, total in parts:
        require(name in manifest and (RAW / name).exists(), f"Missing acquired part: {name}")
        part_start = time.perf_counter()
        count = matching = invalid_numbers = 0
        with zipfile.ZipFile(RAW / name) as archive:
            members = archive.infolist()
            require(
                len(members) == 1 and members[0].filename.endswith(".csv"), "Unexpected ZIP members"
            )
            with (
                archive.open(members[0]) as source,
                io.TextIOWrapper(source, encoding="utf-8-sig", newline="") as stream,
            ):
                reader = csv.DictReader(stream)
                fields = reader.fieldnames
                require(fields is not None, "Missing company CSV header")
                assert fields is not None
                if schema is None:
                    schema = list(fields)
                require(fields == schema, "Company part schemas differ")
                reader.fieldnames = [f.strip() for f in fields]
                require(
                    len(set(reader.fieldnames)) == len(fields), "Duplicate stripped header names"
                )
                for row in reader:
                    require(
                        None not in row and all(v is not None for v in row.values()),
                        "Malformed company row",
                    )
                    count += 1
                    number = row["CompanyNumber"].strip()
                    if not re.fullmatch(r"[A-Z0-9]{8}", number):
                        invalid_numbers += 1
                    codes = sic_codes(row)
                    relevant = set(codes) & SICS
                    if not relevant:
                        continue
                    national_matches += 1
                    matching += 1
                    observed_sics.update(relevant)
                    if (
                        not set(
                            sic_codes({**row, **{f"SICCode.SicText_{i}": "" for i in (2, 3, 4)}})
                        )
                        & SICS
                    ):
                        nonfirst_matches += 1
                    pc = postcode(row["RegAddress.PostCode"])
                    if not pc:
                        missing_postcodes += 1
                    if not pc or pc[:-3] not in outcodes:
                        continue
                    require(
                        number not in candidate_keys,
                        f"Duplicate candidate company number: {number}",
                    )
                    candidate_keys.add(number)
                    candidates.append(
                        {
                            "company_number": number,
                            "company_name": row["CompanyName"].strip(),
                            "status": row["CompanyStatus"].strip(),
                            "company_type": row["CompanyCategory"].strip(),
                            "postcode": pc,
                            "sic_codes": ";".join(codes),
                            "relevant_sics": ";".join(sorted(relevant)),
                            "incorporation_date": row["IncorporationDate"],
                            "cessation_date": row["DissolutionDate"],
                            "source_uri": row["URI"],
                            "source_part": index,
                            "ons_directory_local": pc in directory,
                            "ons_directory_terminated": bool(directory.get(pc, {}).get("doterm")),
                        }
                    )
        statistics.append(
            {
                "part": index,
                "total_parts": total,
                "edition": edition,
                "rows": count,
                "sic_matches": matching,
                "invalid_company_number_shapes": invalid_numbers,
                "seconds": round(time.perf_counter() - part_start, 2),
                "compressed_bytes": (RAW / name).stat().st_size,
                "uncompressed_bytes": members[0].file_size,
                "zip_crc": members[0].CRC,
            }
        )
        print(f"Parsed part {index}/{total}: {count:,} rows, {matching:,} SIC matches", flush=True)
    require(
        all(p["invalid_company_number_shapes"] == 0 for p in statistics),
        "Company number schema discrepancy",
    )
    output = RAW / "postcode-candidates.csv"
    require(bool(candidates), "No local-outcode candidates")
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(candidates[0]))
        writer.writeheader()
        writer.writerows(candidates)
    write_json(
        RAW / "bulk-inspection.json",
        {
            "edition": parts[0][1],
            "parts": statistics,
            "schema_original": schema,
            "header_normalisation": "strip documented leading spaces; preserve number strings",
            "national_rows": sum(p["rows"] for p in statistics),
            "national_sic_matches": national_matches,
            "matches_missed_by_first_sic_only": nonfirst_matches,
            "national_matches_missing_postcode": missing_postcodes,
            "sic_counts_overlap": dict(observed_sics),
            "local_directory_rows": len(directory),
            "official_local_outcodes": sorted(outcodes),
            "geographic_candidate_rows": len(candidates),
            "unique_candidate_postcodes": len({r["postcode"] for r in candidates}),
            "seconds": round(time.perf_counter() - started, 2),
            "rule_version": "registered-office-sic-v1",
            "scope": (
                "Postal-outcode prefilter is provisional; cohort membership requires "
                "official authority codes, not names"
            ),
            "cohort_incompleteness": (
                "New/out-of-vintage or malformed/missing postcodes remain unclassified; "
                "no full national uniqueness claim"
            ),
        },
    )


def cache_fresh(entry: dict[str, Any], now: datetime) -> bool:
    return datetime.fromisoformat(entry["expires_at"]) > now


def cache_entry(result: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    return {
        "result": result,
        "fetched_at": now.isoformat(),
        "expires_at": (now + timedelta(days=30 if result is not None else 1)).isoformat(),
        "source_edition_note": SOURCE_EDITION,
    }


def batch_values(values: list[str]) -> list[list[str]]:
    unique = sorted({postcode(value) for value in values})
    return [unique[i : i + 100] for i in range(0, len(unique), 100)]


def parse_batch(payload: dict[str, Any], requested: list[str]) -> dict[str, Any]:
    require(0 < len(requested) <= 100, "Batch must contain 1 to 100 postcodes")
    require(
        payload.get("status") == 200 and isinstance(payload.get("result"), list),
        "Invalid batch envelope",
    )
    result = {}
    for item in payload["result"]:
        key = postcode(item["query"])
        require(key in requested and key not in result, "Unexpected/duplicate batch result")
        value = item["result"]
        require(value is None or isinstance(value, dict), "Invalid batch item")
        if value is not None:
            require(postcode(value["postcode"]) == key, "Postcode result/query mismatch")
            require(
                bool(value.get("codes", {}).get("admin_district")),
                "Missing postcode authority code",
            )
            require(value.get("quality") in range(1, 10), "Unknown postcode positional quality")
        result[key] = value
    require(set(result) == set(requested), "Incomplete batch response")
    return result


def request_postcodes(request_body: bytes) -> bytes:
    """One bounded retry; transport failures never become negative postcode results."""
    request = urllib.request.Request(
        "https://api.postcodes.io/postcodes",
        data=request_body,
        headers={"Content-Type": "application/json", "User-Agent": "UrbanLens/0.1"},
    )
    for attempt in range(2):
        delay = 2.0
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                content: bytes = response.read(2_000_001)
            require(len(content) <= 2_000_000, "Oversized postcode response")
            return content
        except urllib.error.HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 1:
                raise RuntimeError(f"Postcode service HTTP {error.code}; cache retained") from None
            retry_after = error.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    # A date-form Retry-After is handled conservatively by stopping.
                    raise RuntimeError("Postcode service deferred retry; cache retained") from None
                require(delay <= 30, "Postcode retry exceeds pilot wait budget; cache retained")
        except (urllib.error.URLError, TimeoutError):
            if attempt == 1:
                raise RuntimeError("Postcode service unavailable; cache retained") from None
        time.sleep(delay)
    raise RuntimeError("Postcode retry budget exhausted")


def enrich() -> None:
    cache_path = RAW / "postcode-cache.json"
    cache: dict[str, Any] = read_json(cache_path) if cache_path.exists() else {}
    with (RAW / "postcode-candidates.csv").open(encoding="utf-8", newline="") as stream:
        companies = list(csv.DictReader(stream))
    now = datetime.now(UTC)
    missing = [
        c["postcode"]
        for c in companies
        if c["postcode"] not in cache or not cache_fresh(cache[c["postcode"]], now)
    ]
    batches = batch_values(missing)
    max_batches = 50
    require(
        len(batches) <= max_batches,
        "Pilot lookup budget exceeded; partition/review before continuing",
    )
    for index, batch in enumerate(batches):
        request_body = json.dumps({"postcodes": batch}, separators=(",", ":")).encode()
        name = "postcodes-" + hashlib.sha256(request_body).hexdigest()[:16] + ".json"
        path = RAW / name
        reuse = path.exists()
        if reuse:
            payload = read_json(path)
            saved = read_json(path.with_suffix(".meta.json"))
            require(sha(path) == saved["sha256"], "Postcode response checksum mismatch")
            fetched = datetime.fromisoformat(saved["fetched_at"])
            reuse = all(
                cache_fresh(cache_entry(value, fetched), now)
                for value in parse_batch(payload, batch).values()
            )
            if not reuse:
                # Retain old evidence when refreshing an expired batch.
                stamp = now.strftime("%Y%m%dT%H%M%S%f")
                archived = path.with_name(path.stem + "-" + stamp + ".json")
                path.replace(archived)
                path.with_suffix(".meta.json").replace(archived.with_suffix(".meta.json"))
        if not reuse:
            content = request_postcodes(request_body)
            payload = json.loads(content)
            parse_batch(payload, batch)
            path.write_bytes(content)
            fetched = datetime.now(UTC)
            write_json(
                path.with_suffix(".meta.json"),
                {
                    "url": "https://api.postcodes.io/postcodes",
                    "request": json.loads(request_body),
                    "fetched_at": fetched.isoformat(),
                    "sha256": sha(path),
                },
            )
        for pc, result in parse_batch(payload, batch).items():
            cache[pc] = cache_entry(result, fetched)
        write_json(cache_path, cache)
        print(f"Postcode batch {index + 1}/{len(batches)}: {len(batch)} unique lookups", flush=True)
        if index + 1 < len(batches):
            time.sleep(0.5)
    require(all(c["postcode"] in cache for c in companies), "Unenriched candidate postcodes")
    directory = local_directory()
    target_lsoas = {
        r["LSOA21CD"]
        for r in csv.DictReader(
            (ROOT / "reference/geography/target-areas.csv").open(encoding="utf-8")
        )
    }
    counts: Counter[str] = Counter()
    qualities: Counter[str] = Counter()
    local = []
    unresolved = []
    for row in companies:
        result = cache[row["postcode"]]["result"]
        if result is None:
            counts["unmatched_postcode"] += 1
            unresolved.append(row)
            continue
        codes = result["codes"]
        qualities[str(result["quality"])] += 1
        if codes["admin_district"] != LAD:
            counts["outside_authority"] += 1
            continue
        if codes.get("lsoa21") not in target_lsoas:
            counts["unsupported_lsoa21"] += 1
            unresolved.append(row)
            continue
        row.update(
            {
                "authority_code": codes["admin_district"],
                "lsoa21_code": codes["lsoa21"],
                "postcode_quality": result["quality"],
                "membership_basis": "registered-office postcode, not trading venue",
            }
        )
        local.append(row)
        counts["supported_registered_office"] += 1
    with (RAW / "registered-office-cohort.csv").open("w", encoding="utf-8", newline="") as stream:
        if local:
            writer = csv.DictWriter(stream, fieldnames=list(local[0]))
            writer.writeheader()
            writer.writerows(local)
    write_json(
        RAW / "postcode-inspection.json",
        {
            "cohort_rule_version": "registered-office-sic-v1",
            "candidate_companies": len(companies),
            "unique_candidate_postcodes": len({r["postcode"] for r in companies}),
            "lookup_batches_this_run": len(batches),
            "classification": dict(counts),
            "quality_counts": dict(qualities),
            "directory_present_candidate_postcodes": len(
                {r["postcode"] for r in companies if r["postcode"] in directory}
            ),
            "source_edition_note": SOURCE_EDITION,
            "positive_cache_days": 30,
            "negative_cache_days": 1,
            "batch_limit": 100,
            "max_attempts_per_request": 2,
            "unavailable_fixture": "synthetic; no induced public-service outage",
            "unresolved_company_numbers": [r["company_number"] for r in unresolved],
        },
    )
    tracked = sorted(local, key=lambda r: r["company_number"])[:3]
    write_json(
        RAW / "tracked-companies.json",
        {
            "scope": "three registered-office cohort examples, not verified venue operators",
            "daily_requests": len(tracked),
            "monthly_31_day_requests": 31 * len(tracked),
            "companies": [
                {"company_number": r["company_number"], "company_name": r["company_name"]}
                for r in tracked
            ],
        },
    )
    print("Registered-office cohort:", len(local), "companies;", dict(counts))


def profiles() -> None:
    values = dotenv_values(ROOT / ".env")
    key = os.environ.get("COMPANIES_HOUSE_API_KEY") or values.get("COMPANIES_HOUSE_API_KEY")
    require(bool(key), "Companies House key missing; save it locally in .env, not in chat")
    assert key is not None
    tracked = read_json(RAW / "tracked-companies.json")["companies"]
    require(len(tracked) <= 3, "Profile pilot request limit exceeded")
    for company in tracked:
        number = company["company_number"]
        path = RAW / f"profile-{number}.json"
        if path.exists():
            require(
                sha(path) == read_json(path.with_suffix(".meta.json"))["sha256"],
                "Company profile checksum mismatch",
            )
            continue
        request = urllib.request.Request(
            f"https://api.company-information.service.gov.uk/company/{number}",
            headers={
                "Authorization": "Basic " + base64.b64encode((key + ":").encode()).decode(),
                "Accept": "application/json",
                "User-Agent": "UrbanLens/0.1 bounded-profile-pilot",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(
                f"Company profile HTTP {error.code}; no credentials logged"
            ) from None
        require(payload["company_number"] == number, "Profile identity mismatch")
        # Persist only the company fields needed for this pilot, not officer/control links.
        clean = {
            k: payload.get(k)
            for k in [
                "company_number",
                "company_name",
                "company_status",
                "type",
                "date_of_creation",
                "date_of_cessation",
                "sic_codes",
                "registered_office_address",
                "etag",
            ]
        }
        write_json(path, clean)
        write_json(
            path.with_suffix(".meta.json"),
            {
                "fetched_at": datetime.now(UTC).isoformat(),
                "url": request.full_url,
                "sha256": sha(path),
                "authentication": "server-side API key; value not retained",
            },
        )
        print("Verified authenticated company profile:", number, flush=True)


def fixtures() -> None:
    """Real public postcode edge cases; outage behaviour is tested synthetically."""
    queries = ["BN11JE", "SW1A1AA", "NOTAPOSTCODE", "AA11AA"]
    path = RAW / "postcode-contract.json"
    if path.exists():
        require(
            sha(path) == read_json(path.with_suffix(".meta.json"))["sha256"],
            "Contract fixture checksum mismatch",
        )
        content = path.read_bytes()
    else:
        content = request_postcodes(json.dumps({"postcodes": queries}).encode())
        parse_batch(json.loads(content), queries)
        path.write_bytes(content)
        write_json(
            path.with_suffix(".meta.json"),
            {
                "url": "https://api.postcodes.io/postcodes",
                "request": {"postcodes": queries},
                "fetched_at": datetime.now(UTC).isoformat(),
                "sha256": sha(path),
            },
        )
    values = parse_batch(json.loads(content), queries)
    require(values["BN11JE"]["codes"]["admin_district"] == LAD, "Local fixture changed")
    require(values["SW1A1AA"]["codes"]["admin_district"] != LAD, "Outside fixture changed")
    require(
        values["NOTAPOSTCODE"] is None and values["AA11AA"] is None,
        "Invalid/unknown fixture changed",
    )
    print("Real postcode fixtures verified: valid, outside, invalid/null and unknown/null")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    tasks = {
        "fetch": fetch,
        "filter": filter_bulk,
        "postcodes": enrich,
        "profiles": profiles,
        "fixtures": fixtures,
    }
    parser.add_argument("task", choices=list(tasks))
    task = parser.parse_args().task
    tasks[task]()


if __name__ == "__main__":
    main()
