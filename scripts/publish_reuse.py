"""Publish a verified data-only archive using short-lived GitHub workflow credentials."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any
from uuid import UUID

import requests

from scripts.dev import ROOT
from urbanlens.ingestion.contracts import Artifact
from urbanlens.reuse import encoded, sha, verify

REPOSITORY = "MisterPasha/UrbanLens"
BASE = "https://github.com/" + REPOSITORY + "/releases/download/data-"


def candidate(store: Any, identifier: str) -> dict[str, Any]:
    UUID(identifier)
    artifact = store.stat(f"reuse/candidates/{identifier}/manifest.json")
    if artifact is None:
        raise ValueError("No prepared reuse bundle for this release")
    return dict(json.loads(b"".join(store.read_verified(artifact))))


def receipt(store: Any, identifier: str) -> dict[str, Any]:
    prepared = candidate(store, identifier)
    policy = json.loads((ROOT / "config/reuse-policy.json").read_bytes())
    if prepared["policy_sha256"] != sha(encoded(policy)):
        raise ValueError("The stored bundle's field-level review is obsolete")
    artifact = store.stat(f"reuse/published/{identifier}.json")
    if artifact is None:
        raise ValueError(
            "Publish the verified reuse download before making this data release active"
        )
    proof = dict(json.loads(b"".join(store.read_verified(artifact))))
    expected_url = BASE + identifier + "/urbanlens-" + identifier + ".zip"
    if (
        proof["sha256"] != prepared["sha256"]
        or proof["url"] != expected_url
        or not proof["anonymous_download_verified"]
    ):
        raise ValueError("Reuse publication receipt does not match the prepared bundle")
    return proof


def main() -> None:
    from scripts.production import client
    from urbanlens.ingestion.registry import S3StateBackend
    from urbanlens.ingestion.storage import S3ArtifactStore
    from urbanlens.operations.control import Coordinator

    store = S3ArtifactStore(client("s3"), os.environ["BRONZE_BUCKET"])
    pending, _ = Coordinator(
        S3StateBackend(store.client, store.bucket, "operations"), None, ""
    ).read("pending-publication.json")
    ready = bool(pending.get("release_id"))
    if ready:
        print(json.dumps(publish_package(store, pending["release_id"])))
    else:
        print(json.dumps({"status": "no_pending_publication"}))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write("ready=" + str(ready).lower() + "\n")


def github_session() -> requests.Session:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token and not os.environ.get("CI"):
        result = subprocess.run(
            ["git", "credential", "fill"],
            input="protocol=https\nhost=github.com\n\n",
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
            env={**os.environ, "GCM_INTERACTIVE": "never", "GIT_TERMINAL_PROMPT": "0"},
        )
        values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        token = values.get("password")
    if not token:
        raise ValueError("GitHub publication requires the existing login or workflow GITHUB_TOKEN")
    session = requests.Session()
    ca = ROOT / ".cache/aws-system-ca.pem"
    session.verify = str(ca) if ca.exists() else True
    session.headers.update(Authorization="Bearer " + token, Accept="application/vnd.github+json")
    return session


def publish_package(store: Any, identifier: str | None) -> dict[str, Any]:
    if identifier is None:
        response = store.client.get_object(
            Bucket=store.bucket, Key="operations/pending-publication.json"
        )
        identifier = json.loads(response["Body"].read())["release_id"]
    prepared = candidate(store, identifier)
    policy = json.loads((ROOT / "config/reuse-policy.json").read_bytes())
    if prepared["policy_sha256"] != sha(encoded(policy)):
        raise ValueError("The prepared bundle's field-level review is obsolete")
    bundle = b"".join(store.read_verified(Artifact.model_validate(prepared["artifact"])))
    manifest = verify(bundle)
    if manifest["release_id"] != identifier or sha(bundle) != prepared["sha256"]:
        raise ValueError("Prepared release and archive differ")
    session = github_session()
    api = "https://api.github.com/repos/" + REPOSITORY

    def request(method: str, url: str, **kwargs: Any) -> requests.Response:
        result = session.request(method, url, timeout=90, **kwargs)
        if not result.ok:
            raise ValueError("GitHub publication request failed: HTTP " + str(result.status_code))
        return result

    repository = request("GET", api).json()
    if repository["private"]:
        raise ValueError("The reuse offer requires an anonymous public repository")
    tag = "data-" + identifier
    found = session.get(api + "/releases/tags/" + tag, timeout=30)
    if found.status_code == 404:
        release = request(
            "POST",
            api + "/releases",
            json=dict(
                tag_name=tag,
                name="UrbanLens data " + identifier,
                body=(
                    "Versioned public derived database under ODbL 1.0. See manifest, schema, "
                    "source notices and checksums inside the ZIP. Observations are not "
                    "verified business openings/closures."
                ),
                draft=True,
                prerelease=False,
                make_latest="false",
            ),
        ).json()
    else:
        found.raise_for_status()
        release = found.json()
    name = "urbanlens-" + identifier + ".zip"
    assets = {a["name"]: a for a in release["assets"]}
    for filename, body, content_type in (
        (name, bundle, "application/zip"),
        (name + ".sha256", (sha(bundle) + "  " + name + "\n").encode(), "text/plain"),
    ):
        if filename in assets:
            existing = request(
                "GET",
                api + "/releases/assets/" + str(assets[filename]["id"]),
                headers={"Accept": "application/octet-stream"},
            )
            if existing.content != body:
                raise ValueError("Refusing to replace a different immutable release asset")
        else:
            if not release["draft"]:
                raise ValueError("Published data release is incomplete; do not modify it silently")
            request(
                "POST",
                "https://uploads.github.com/repos/"
                + REPOSITORY
                + "/releases/"
                + str(release["id"])
                + "/assets",
                params={"name": filename},
                data=body,
                headers={"Content-Type": content_type},
            )
    if release["draft"]:
        request(
            "PATCH",
            api + "/releases/" + str(release["id"]),
            json={"draft": False, "make_latest": "false"},
        )
    url = BASE + identifier + "/" + name
    # Deliberately no authenticated session: prove that an ordinary visitor can download it.
    public = requests.get(url, timeout=90, verify=session.verify)
    public.raise_for_status()
    if sha(public.content) != prepared["sha256"]:
        raise ValueError("Anonymous download differs from the verified archive")
    verify(public.content)
    proof = dict(
        release_id=identifier,
        url=url,
        sha256=sha(bundle),
        byte_count=len(bundle),
        anonymous_download_verified=True,
    )
    store.put_immutable(
        f"reuse/published/{identifier}.json",
        iter([encoded(proof)]),
        media_type="application/json",
        compression="none",
    )
    return proof


if __name__ == "__main__":
    main()
