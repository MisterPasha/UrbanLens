"""Small diagnostic CLI; later pipeline commands must not pretend to be implemented."""

import argparse
import json
import platform
import sys
from pathlib import Path

from urbanlens import __version__
from urbanlens.config import Settings
from urbanlens.database.dbt import debug


def main() -> None:
    parser = argparse.ArgumentParser(prog="urbanlens")
    parser.add_argument("command", choices=["doctor", "dbt-debug"])
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    args = parser.parse_args()
    if args.command == "dbt-debug":
        root = Path(__file__).resolve().parents[2]
        sys.exit(debug(root, args.target))
    settings = Settings()
    print(
        json.dumps(
            {
                "package": "urbanlens",
                "version": __version__,
                "python": platform.python_version(),
                "environment": settings.environment,
                "research_data": "not loaded",
            },
            indent=2,
        )
    )
