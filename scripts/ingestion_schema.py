"""Export/check the versioned ingestion JSON schemas without network or database access."""

import argparse
import json
from pathlib import Path

from urbanlens.ingestion.contracts import Run, Snapshot

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "reference/ingestion"


def schemas() -> dict[str, str]:
    return {
        name + ".schema.json": json.dumps(model.model_json_schema(), indent=2) + "\n"
        for name, model in (("snapshot", Snapshot), ("run", Run))
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for name, text in schemas().items():
        path = DIRECTORY / name
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != text:
                raise SystemExit(f"Outdated schema: {path.relative_to(ROOT)}")
        else:
            DIRECTORY.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
    print("Ingestion schemas " + ("verified" if args.check else "exported"))


if __name__ == "__main__":
    main()
