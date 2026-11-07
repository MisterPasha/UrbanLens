"""Run dbt diagnostics with the scoped SQL role; passwords stay in process environment."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import dotenv_values
from psycopg.conninfo import conninfo_to_dict


def debug(root: Path, target: str) -> int:
    environment = os.environ.copy()
    environment.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    values = dotenv_values(root / ".env")
    key = ("URBANLENS_LOCAL_" if target == "local" else "URBANLENS_") + "DBT_DATABASE_URL"
    dsn = environment.get(key) or values.get(key)
    executable = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    arguments = [str(executable), "debug", "--project-dir", str(root / "dbt")]
    if not dsn:
        if target != "local":
            raise ValueError("Configure scoped Neon roles with database setup first")
        for name, value in values.items():
            if name.startswith("LOCAL_POSTGRES_") and value is not None:
                environment.setdefault(name, value)
        return subprocess.run(
            arguments + ["--profiles-dir", str(root / "dbt")], env=environment, check=False
        ).returncode
    settings = conninfo_to_dict(dsn)
    if settings.get("user") != "urbanlens_dbt":
        raise ValueError("dbt diagnostics require the restricted urbanlens_dbt role")
    for name, default in (
        ("host", "127.0.0.1"),
        ("port", "5432"),
        ("user", ""),
        ("password", ""),
        ("dbname", ""),
        ("sslmode", "prefer"),
    ):
        environment["UL_DBT_" + name.upper()] = str(settings.get(name) or default)
    if settings.get("channel_binding"):
        environment["PGCHANNELBINDING"] = str(settings["channel_binding"])
    cache = (root / ".cache").resolve()
    if not cache.is_relative_to(root.resolve()):
        raise ValueError("Database diagnostic directory escapes the workspace")
    cache.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dbt-role-", dir=cache) as directory:
        profile = (
            "urbanlens:\n  target: scoped\n  outputs:\n    scoped:\n      type: postgres\n"
            "      schema: ul_gold\n      threads: 2\n      connect_timeout: 10\n"
        )
        for name in ("host", "port", "user", "password", "dbname", "sslmode"):
            expression = (
                "{{ env_var('UL_DBT_"
                + name.upper()
                + "')"
                + (" | int" if name == "port" else "")
                + " }}"
            )
            profile += "      " + name + ': "' + expression + '"\n'
        (Path(directory) / "profiles.yml").write_text(profile, encoding="utf-8")
        return subprocess.run(
            arguments + ["--profiles-dir", directory], env=environment, check=False
        ).returncode
