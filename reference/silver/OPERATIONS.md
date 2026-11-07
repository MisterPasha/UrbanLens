# Silver maintenance

Steps 27–28 add retention, recoverable backups, coverage reporting and an audit. They do not run a server or fetch new provider data.

## Commands

```powershell
python scripts/dev.py maintenance coverage --target neon
python scripts/dev.py maintenance backup --target neon
python scripts/dev.py maintenance retention --target neon
python scripts/dev.py maintenance retention --target neon --apply
python scripts/dev.py maintenance audit --target neon
python scripts/dev.py maintenance replay --target neon
python scripts/dev.py maintenance lifecycle
```

Use `--target local` for Docker. Retention previews changes unless `--apply` is supplied. Backup creates daily and weekly points, verifies downloaded bytes, and restores into a disposable local database; Docker must be running. It never restores over Neon or your main local database. A rerun reuses the first successful backup for that day/week.

Replay repeats the existing Silver pipeline against pinned inputs and compares meaningful database state. It makes no provider API requests. Keep the existing ignored bundle/classification pointers and location assessment report for this development command.

## Retention

| Data | Policy |
| --- | --- |
| Raw Bronze objects | S3 expiry after 90 days |
| Events and obsolete, unreferenced normalised evidence | 12 calendar months |
| Current entities, identity registries, links, reviews and evidence still needed | Retained |
| Lambda logs | Seven-day default in the infrastructure template |
| Logical backups | Seven daily / four weekly points; S3 expiry after 7 / 28 days |

S3 expiry rules are live. Database cleanup and backup creation are explicit commands until automation in step 49. Provider collections remain **monthly**. Never apply blanket expiry to the Silver prefix: current and historical evidence can outlive its raw source file.

## Files and results

- `config/retention.json`: policy reference.
- `scripts/maintenance.py`: command entry point and replay/object cleanup.
- `scripts/backup.py`: logical dump, private S3 upload and isolated restore verification.
- `src/urbanlens/silver/operations.py`: database cleanup, coverage and backup fingerprints.
- `src/urbanlens/silver/audit.py`: sample/provenance checks and semantic comparison.
- Migration `0012_silver_operations.sql`: private current-roster and coverage views.
- `data/silver/`: ignored, detailed coverage/audit/replay/backup reports; durable proofs and code archives also live in private S3.

Coverage names its denominator: all current canonical candidates, resolved eligible supported venues, or primary spatial venues. Exclusion reasons overlap; do not add them together. These counts describe accepted evidence, not a complete census of Brighton hospitality.

[Retention/backup evidence](../../docs/evidence/silver-operations.md) · [M2 audit](../../docs/evidence/m2-audit.md)
