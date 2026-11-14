# Public data reuse

The public database is offered under ODbL 1.0; source contents retain their rights.
`config/reuse-policy.json` records the reviewed fields and source terms. Unknown
columns, private evidence, suppressed locations and NI postcodes stop export.

- Build: `python scripts/dev.py reuse build --release-id UUID`.
- Publish: `python scripts/dev.py reuse publish --release-id UUID`.
- Verify receipt: `python scripts/dev.py reuse receipt --release-id UUID`.

CodeBuild builds a validated candidate's CSV/JSONL/GeoJSON ZIP before activation.
The monthly `reuse.yml` workflow uses the existing production owner approval,
uploads the prepared ZIP to GitHub Releases, verifies an anonymous download,
then starts the publication-only CodeBuild job. No GitHub credentials enter AWS.
An incomplete offer holds the candidate; the previous release stays active.
Daily health reports `reuse_publication_pending` until publication completes.

Keep immutable downloads for publicly used releases and at least 12 months.
See `NOTICES.txt` and the bundled original ODbL/OGL texts for source obligations.
This is a documented engineering assessment, not independent legal certification.
