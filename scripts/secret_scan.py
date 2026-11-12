"""Scan versioned and unignored source files; report filenames, never secret values."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

PATTERNS = (
    re.compile(rb"(?:AKIA|ASIA)[A-Z0-9]{16}"),
    re.compile(rb"gh[pousr]_[A-Za-z0-9]{36,}"),
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(rb"postgres(?:ql)?://[^\s:/]+:(?P<password>[^\s@]{16,})@"),
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        capture_output=True,
        check=True,
    )
    failures = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        relative = raw.decode()
        path = root / relative
        if not path.is_file() or path.stat().st_size > 2_000_000:
            continue
        if path.name == ".env" or (path.name.startswith(".env.") and path.name != ".env.example"):
            failures.append(relative)
        else:
            for pattern in PATTERNS:
                for match in pattern.finditer(path.read_bytes()):
                    if match.groupdict().get("password") in {
                        b"role-placeholder",
                        b"test-placeholder",
                    }:
                        continue  # Named synthetic test credentials, not arbitrary test files.
                    failures.append(relative)
    if failures:
        raise SystemExit("Potential credentials in source: " + ", ".join(sorted(set(failures))))
    print("Source secret scan passed (known credential patterns; not a guarantee).")


if __name__ == "__main__":
    main()
