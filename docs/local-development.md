# Development

Requires Python 3.13, Node.js 24/npm 11, Git and Docker Desktop with its engine running.

```powershell
git clone https://github.com/MisterPasha/UrbanLens.git
cd UrbanLens
python scripts/dev.py setup
python scripts/dev.py db-up
python scripts/dev.py database setup --target local
```

Setup installs locked dependencies, creates private `.env` and configures local
roles. It preserves existing credentials. Never commit `.env` or use the database
administrator login for the API. No cloud account is needed for local setup.

Run these in separate terminals:

```powershell
python scripts/dev.py api
python scripts/dev.py web
```

Open http://127.0.0.1:5173. Vite serves the frontend and proxies `/api` to FastAPI
on port 8000. A fresh database has no published data; the UI shows unavailable,
not zero. `db-down` stops Docker while retaining its data volume.

```powershell
python scripts/dev.py check
python scripts/dev.py browser-install
python scripts/dev.py browser-test
```

The browser checks use controlled fixtures. For full real-data processing,
configure the non-root `urbanlens_dev` AWS profile with access to the existing
private S3 evidence, then follow the [accepted-input rehearsal](../reference/operations/REHEARSAL.md).
It rebuilds in a disposable database and removes it afterwards. Cloud setup and
manual refresh require separate infrastructure/access; local setup creates none.

Data flow: five providers ? private S3 Bronze ? typed/matched PostGIS Silver ?
dbt Gold ? validated immutable release ? read-only FastAPI ? React.
Python owns ingestion/Silver, dbt builds Gold, and numbered SQL migrations own
schema changes. Add migrations; never edit an applied migration. API requests
pin a release; publication and rollback switch pointers atomically.

- [Source contracts](../reference/ingestion/SOURCES.md)
- [Database migrations](../src/urbanlens/database/sql/)
- [Monthly refresh, CI/CD and incident actions](../reference/operations/README.md)
- [Restore and rollback](../reference/operations/RECOVERY.md)
- [API schema](../reference/api/openapi.json)

All scheduled source refreshes are monthly; backups are daily. Population uses
its stated reference date, not the refresh date. Raw files, private reviews,
credentials and generated evidence stay out of GitHub. Public data reuse bundles
are verified before activation; see the [reuse guide](../reference/reuse/README.md).
