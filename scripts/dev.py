"""Cross-platform task runner. Run from a checkout with Python 3.13; no activation needed."""

from __future__ import annotations

import argparse
import os
import secrets
import shutil
import subprocess
import sys
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WINDOWS = os.name == "nt"
UV_VERSION = "0.13.6"
TOOLS = ROOT / ".tools"


def executable(environment: Path, name: str) -> str:
    directory = environment / ("Scripts" if WINDOWS else "bin")
    return str(directory / (f"{name}.exe" if WINDOWS else name))


def run(arguments: list[str], *, cwd: Path = ROOT) -> None:
    environment = os.environ.copy()
    environment.setdefault("UV_CACHE_DIR", str(ROOT / ".cache" / "uv"))
    environment.setdefault("UV_PYTHON_DOWNLOADS", "never")
    environment.setdefault("UV_SYSTEM_CERTS", "true")
    environment.setdefault("NODE_USE_SYSTEM_CA", "1")
    environment.setdefault("npm_config_cache", str(ROOT / ".cache" / "npm"))
    environment.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(ROOT / ".cache" / "ms-playwright"))
    environment.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    if WINDOWS and Path(arguments[0]).name.lower() == "docker.exe":
        # Desktop's credential helper shares this directory; inherited PATH can be stale.
        environment["PATH"] = (
            str(Path(arguments[0]).parent) + os.pathsep + environment.get("PATH", "")
        )
    # Do not echo commands: future commands can contain private connection details.
    subprocess.run(arguments, cwd=cwd, env=environment, check=True)


def tool(name: str) -> str:
    candidates = [f"{name}.cmd", name] if WINDOWS and name == "npm" else [name]
    for candidate in candidates:
        if resolved := shutil.which(candidate):
            return resolved
    if WINDOWS and name == "docker":
        # Newly installed Desktop may not reach this terminal's inherited PATH yet.
        for directory in [
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "DockerDesktop",
            Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Docker" / "Docker",
        ]:
            candidate_path = directory / "resources" / "bin" / "docker.exe"
            if candidate_path.is_file():
                return str(candidate_path)
    raise RuntimeError(f"{name} is missing from PATH. See docs/local-development.md.")


def uv() -> str:
    path = executable(TOOLS, "uv")
    if not Path(path).exists():
        raise RuntimeError("Run python scripts/dev.py setup first.")
    return path


def in_environment(*arguments: str) -> None:
    run([uv(), "run", "--locked", "--all-extras", *arguments])


def initialise_env() -> None:
    path = ROOT / ".env"
    if path.exists():
        return
    template = (ROOT / ".env.example").read_text(encoding="utf-8")
    content = template.replace(
        "LOCAL_POSTGRES_PASSWORD=\n", f"LOCAL_POSTGRES_PASSWORD={secrets.token_urlsafe(24)}\n"
    )
    # Exclusive creation preserves an existing file, even if another setup starts concurrently.
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(content)
    if not WINDOWS:
        path.chmod(0o600)
    print("Created private .env with a generated local database password.")


def setup() -> None:
    if sys.version_info[:2] != (3, 13):
        raise RuntimeError("Use Python 3.13 for this project's locked environment.")
    node_major = subprocess.check_output([tool("node"), "--version"], text=True).strip()
    if not node_major.startswith("v24."):
        raise RuntimeError("Use Node.js 24 (see .node-version for the checked version).")
    if not Path(executable(TOOLS, "python")).exists():
        venv.create(TOOLS, with_pip=True)
    run(
        [
            executable(TOOLS, "python"),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            f"uv=={UV_VERSION}",
        ]
    )
    run([uv(), "sync", "--locked", "--all-extras", "--python", sys.executable])
    run([tool("npm"), "ci"], cwd=ROOT / "web")
    initialise_env()
    profile = ROOT / "dbt" / "profiles.yml"
    if not profile.exists():
        shutil.copyfile(ROOT / "dbt" / "profiles.example.yml", profile)
    print("Setup complete. Run python scripts/dev.py check.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "task",
        choices=[
            "setup",
            "check",
            "format",
            "doctor",
            "api",
            "web",
            "browser-install",
            "browser-test",
            "db-up",
            "db-down",
            "db-check",
            "dbt-debug",
            "geography-fetch",
            "geography-check",
            "ingestion-schema",
            "ingest",
            "pipeline",
            "bronze",
            "database",
            "silver",
            "classify",
            "locations",
            "resolve",
            "companies",
            "history",
            "maintenance",
            "gold",
            "release",
            "api-schema",
            "api-acceptance",
            "api-check",
            "fsa-fetch",
            "fsa-check",
            "osm-fetch",
            "osm-check",
            "venue-pilot-fetch",
            "venue-pilot-check",
            "company-pilot-fetch",
            "company-pilot-filter",
            "company-pilot-postcodes",
            "company-pilot-profiles",
            "company-pilot-fixtures",
            "runtime-build",
            "runtime-containers",
            "runtime-smoke",
            "runtime-database-local",
            "runtime-database-container",
            "runtime-database-neon",
            "runtime-backup-local",
            "runtime-backup-neon",
            "runtime-neon-read",
            "runtime-neon-wake",
            "runtime-spatial",
        ],
    )
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.arguments and args.task not in {
        "ingest",
        "pipeline",
        "bronze",
        "database",
        "silver",
        "classify",
        "locations",
        "resolve",
        "companies",
        "history",
        "maintenance",
        "gold",
        "release",
        "api-schema",
        "dbt-debug",
    }:
        parser.error(
            "Additional arguments require ingest, pipeline, bronze, database, "
            "silver, classify, locations, resolve, companies, history, gold or dbt-debug"
        )
    match args.task:
        case "setup":
            setup()
        case "check":
            in_environment("ruff", "format", "--check", "src", "scripts", "tests", "pyproject.toml")
            in_environment("ruff", "check", "src", "scripts", "tests", "pyproject.toml")
            in_environment("mypy")
            in_environment("pytest", "-m", "not database")
            for script in ["typecheck", "lint", "build"]:
                run([tool("npm"), "run", script], cwd=ROOT / "web")
        case "format":
            in_environment("ruff", "format", "src", "scripts", "tests", "pyproject.toml")
            in_environment("ruff", "check", "--fix", "src", "scripts", "tests", "pyproject.toml")
        case "doctor":
            in_environment("urbanlens", "doctor")
        case "api":
            in_environment(
                "uvicorn",
                "urbanlens.api.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
                "--no-access-log",
            )
        case "web":
            run([tool("npm"), "run", "dev"], cwd=ROOT / "web")
        case "browser-install":
            run([tool("npm"), "exec", "--", "playwright", "install", "chromium"], cwd=ROOT / "web")
        case "browser-test":
            run([tool("npm"), "run", "test:e2e"], cwd=ROOT / "web")
        case "db-up":
            run([tool("docker"), "compose", "up", "-d", "--wait", "--wait-timeout", "120", "db"])
        case "db-down":
            # Preserve the volume; destructive resets require an explicit separate operation.
            run([tool("docker"), "compose", "down"])
        case "db-check":
            in_environment("pytest", "-m", "database")
        case "dbt-debug":
            in_environment("urbanlens", "dbt-debug", *args.arguments)
        case "geography-fetch":
            in_environment("python", "scripts/geography.py", "fetch")
        case "geography-check":
            in_environment("python", "scripts/geography.py", "check")
        case "ingestion-schema":
            in_environment("python", "-m", "scripts.ingestion_schema")
        case "ingest":
            in_environment("python", "-m", "urbanlens.ingestion.commands", *args.arguments)
        case "pipeline":
            in_environment("python", "-m", "scripts.source_pipeline", *args.arguments)
        case "bronze":
            in_environment("python", "-m", "scripts.bronze", *args.arguments)
        case "database":
            in_environment("python", "-m", "scripts.database", *args.arguments)
        case "silver":
            in_environment("python", "-m", "scripts.silver", *args.arguments)
        case "classify":
            in_environment("python", "-m", "scripts.classify", *args.arguments)
        case "locations":
            in_environment("python", "-m", "scripts.locations", *args.arguments)
        case "resolve":
            in_environment("python", "-m", "scripts.resolve", *args.arguments)
        case "companies":
            in_environment("python", "-m", "scripts.companies", *args.arguments)
        case "history":
            in_environment("python", "-m", "scripts.history", *args.arguments)
        case "maintenance":
            in_environment("python", "-m", "scripts.maintenance", *args.arguments)
        case "gold":
            in_environment("python", "-m", "scripts.gold", *args.arguments)
        case "release":
            in_environment("python", "-m", "scripts.release", *args.arguments)
        case "api-schema":
            in_environment("python", "-m", "scripts.api_schema", *args.arguments)
        case "api-acceptance":
            in_environment("python", "-m", "scripts.api_acceptance", *args.arguments)
        case "api-check":
            in_environment(
                "pytest",
                "tests/test_published_api.py",
                "-q",
                "-p",
                "no:cacheprovider",
                "--basetemp",
                str(ROOT / ".cache" / ("api-check-" + secrets.token_hex(6))),
            )
        case "fsa-fetch" | "fsa-check":
            in_environment("python", "-m", "scripts.fsa_source", args.task.removeprefix("fsa-"))
        case "osm-fetch" | "osm-check":
            in_environment("python", "-m", "scripts.osm_source", args.task.removeprefix("osm-"))
        case "venue-pilot-fetch":
            in_environment("python", "scripts/venue_pilot.py", "fetch")
        case "venue-pilot-check":
            in_environment("python", "scripts/venue_pilot.py", "check")
        case task if task.startswith("company-pilot-"):
            in_environment(
                "python", "scripts/company_pilot.py", task.removeprefix("company-pilot-")
            )
        case task if task.startswith("runtime-"):
            in_environment("python", "-m", "scripts.runtime_pilot", task.removeprefix("runtime-"))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError):
            print(f"Task failed with exit code {error.returncode}.", file=sys.stderr)
        else:
            print(error, file=sys.stderr)
        sys.exit(1)
