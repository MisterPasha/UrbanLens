"""Assess venue coordinates and persist private, auditable source-level geography."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

import psycopg

from scripts.database import credential
from scripts.silver import ROOT, environment
from urbanlens.database.migrations import connect
from urbanlens.silver.location_processing import run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    parser.add_argument(
        "--as-of",
        default=datetime.now(UTC).date().isoformat(),
        help="UTC date for postcode cache validity; defaults to today",
    )
    parser.add_argument("--reviews", help="Optional source-hash-bound coordinate reviews JSON list")
    args = parser.parse_args()
    as_of = datetime.fromisoformat(args.as_of).replace(tzinfo=UTC)
    reviews = json.loads((ROOT / args.reviews).read_text()) if args.reviews else []
    _, store, _, _, _, _ = environment("s3")
    with connect(credential(args.target, "pipeline")) as connection:
        report = run(connection, store, as_of, reviews)
    path = ROOT / f"data/silver/locations-{args.target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error) as error:
        (ROOT / ".cache/location-error.txt").write_text(str(error))
        raise SystemExit(
            f"Location assessment failed ({type(error).__name__}); "
            "inspect private .cache/location-error.txt"
        ) from None
