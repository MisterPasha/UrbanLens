# Recovery

Run an isolated recovery check with Docker Desktop running, the local PostGIS
database available, scoped local roles configured, and the `urbanlens_dev` AWS
profile able to read private backups/evidence:

```powershell
python scripts/dev.py db-up
python scripts/dev.py recovery
```

This creates a separate daily recovery point, restores it into a new local
PostgreSQL 17/PostGIS database, verifies every backed-up table's fingerprint and
rebuilds the explicit role permissions. Older query functions are adjusted to
use schema/table names when the restored database has a different name.
Neon data and production release pointers are untouched.

The check reconciles both retained releases, rejects an invalid candidate,
detects a deliberately damaged local publication, tests interrupted rollback,
and switches to the previous release. Pinned API reads must remain consistent.
It then reinstates the original local release and removes the disposable database.

Raw replay reparses retained source files using their saved definitions, compares
accepted rows with retained parsed rows, and rebuilds/verifies normalised records.
Provider downloads are disabled. Postcodes use dated retained cache evidence.
History explanation verifies saved event evidence without reading Bronze.
Current events are recent; this simulates raw expiry rather than claiming a
year-old production dataset exists.

Reuse an existing point instead of taking another backup:

```powershell
python scripts/dev.py recovery --backup-key backups/neon/daily/<point>.json
```

If restore passed but a later stage failed, inspect its log, fix the cause and
resume the same local report directory. This reuses the completed restore and
raw replay when present, rechecks backup/output hashes, and finishes verification:

```powershell
python scripts/dev.py recovery --resume data/recovery/<timestamp>
```

Results are saved under `data/recovery/<timestamp>/` and private S3
`evidence/maintenance/recovery/`. `operations/last-restore.json` identifies the
specific verified backup and date; it does not claim every later backup was
restored. Failed attempts remain in local logs.

For an incident, pause writers first. Restore the selected point into an isolated
database, reconcile it, provision new scoped credentials, then deliberately switch
readers/writers. Do not overwrite the current database with an unverified dump.
A new Neon branch and production credential cutover have not been rehearsed here.
For a bad publication with intact previous data, use the existing release rollback
procedure instead of restoring the entire database.

Recovery loses database changes after the selected snapshot. Successful daily
backups normally bound this to roughly 24 hours; failures/delays increase exposure.
Daily points expire after seven days and weekly points after 28. Raw replay is
limited to the 90-day Bronze window. Normalised history has a rolling 12-month
policy, preserving evidence still needed by current records and retained events.

S3 expiry eligibility rounds to the next UTC midnight and deletion is asynchronous.
Versioning/noncurrent-version retention must be checked separately: the development
identity cannot read that setting. No destructive retention test is performed.
See [actual timings and evidence](../../docs/evidence/recovery-verification.md).
