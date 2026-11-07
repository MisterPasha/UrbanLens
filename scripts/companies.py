"""Build private Silver legal-company context and import reviewed relationships."""

from __future__ import annotations

import argparse
import json

from scripts.database import credential
from scripts.silver import ROOT, environment
from urbanlens.database.migrations import connect
from urbanlens.silver.companies import Register, run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["run", "schema"])
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    args = parser.parse_args()
    if args.task == "schema":
        (ROOT / "reference/silver/company-links.schema.json").write_text(
            json.dumps(Register.model_json_schema(), indent=2) + "\n", encoding="utf-8"
        )
        return
    register = Register.model_validate_json(
        (ROOT / "config/company-links.json").read_text(encoding="utf-8")
    )
    _, store, _, _, _, _ = environment("s3")
    with connect(credential(args.target, "pipeline")) as connection:
        report = run(connection, store, register)
    (ROOT / f"data/silver/company-context-{args.target}.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
