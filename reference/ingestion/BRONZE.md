# Private S3 Bronze — step 18

Administrator setup: [stack instructions](../../infra/bronze/README.md). Local data remains available throughout setup.

Run these commands in order after the stack reaches `CREATE_COMPLETE`:

```powershell
python scripts/dev.py bronze prepare
python scripts/dev.py bronze bootstrap
python scripts/dev.py bronze security
python scripts/dev.py bronze deploy
python scripts/dev.py bronze configure-secret
python scripts/dev.py bronze verify
python scripts/dev.py bronze replay
python scripts/dev.py bronze recover
python scripts/dev.py bronze enable
```

| Command | Purpose |
| --- | --- |
| `prepare` | Build a Linux Lambda ZIP from hash-verified locked dependencies; no credentials |
| `bootstrap` | Upload retained source objects, manifests, reviews and postcode cache evidence; download/check every copied object; seed current pointers without changing source dates |
| `security` | Check live privacy, encryption, ownership and TLS; test overwrite protection using a synthetic object |
| `deploy` | Upload the credential-free Lambda package |
| `configure-secret` | With explicit owner authorization, copy the local Companies House key into a private SSM SecureString |
| `verify` | Invoke each cloud source twice and replay inside Lambda; check identical baseline IDs/dates and private S3 retention |
| `replay` | Reparse S3 inputs and compare exact decoded JSONL hashes; no provider downloads or fresh observations |
| `recover` | Inject partition/provider failures in explicitly synthetic scopes; verify resume and last-valid preservation |
| `enable` | Enable verified monthly jobs; company profiles stay disabled until the private key is configured |

New files: `scripts/bronze.py` connects these commands; `ingestion/archive.py` copies/verifies artifacts and resolves original local URIs through an S3 catalog; `ingestion/cloud.py` runs the existing source adapters using S3 state; `ingestion/verification.py` performs replay/recovery checks. The old manifests and review digests remain unchanged.

Monthly cloud jobs cover FSA, OSM and tracked company profiles, on the seventh at 08:00/09:00/10:00 UTC. Already accepted November observations are reused; the next new routine observations are December. Requests are bounded; profiles have a shared 100-attempt monthly cloud budget, seeded with existing local use. After bootstrap, local profile commands only reuse the accepted local month; new profile collection uses the cloud ledger.

National company ZIP processing and ONS edition checks remain explicit local monthly jobs for this skeleton; their artifacts can be uploaded using `bootstrap` **only while source/cloud current pointers agree**. Future monthly local loads need a separately validated import/promotion path; bootstrap deliberately refuses to overwrite a different cloud baseline. Postcodes.io remains an on-demand cache/enrichment service, with its own dated evidence, rather than a pretend citywide snapshot. Full workflow orchestration is later work.

The private raw files expire 90 days after S3 upload. Manifests/reviews/normalized evidence are separate; original source dates remain authoritative. Within retention, replay checks raw hashes before parsing. No database loading, canonical venue changes or public publication occurs here.

Stored-file checksums always cover the original compressed bytes. Replay compares the exact decoded records separately: Windows and Linux gzip implementations can encode identical data differently. Reports retain both comparisons.

Local state is in ignored `.cache/bronze-state.json`. Results are in ignored `data/ingestion/bronze-*.json`; full source objects remain in private S3. No credentials belong in GitHub.
