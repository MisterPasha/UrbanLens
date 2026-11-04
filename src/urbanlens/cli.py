"""Small diagnostic CLI; later pipeline commands must not pretend to be implemented."""

import argparse
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

from urbanlens import __version__
from urbanlens.config import Settings


def main() -> None:
    parser = argparse.ArgumentParser(prog="urbanlens")
    parser.add_argument("command", choices=["doctor", "dbt-debug"])
    args = parser.parse_args()
    if args.command == "dbt-debug":
        root = Path(__file__).resolve().parents[2]
        environment = os.environ.copy()
        for key, value in dotenv_values(root / ".env").items():
            if key.startswith("LOCAL_POSTGRES_") and value is not None:
                environment.setdefault(key, value)
        dbt = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
        result = subprocess.run(
            [
                str(dbt),
                "debug",
                "--project-dir",
                str(root / "dbt"),
                "--profiles-dir",
                str(root / "dbt"),
            ],
            env=environment,
            check=False,
        )
        sys.exit(result.returncode)
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
