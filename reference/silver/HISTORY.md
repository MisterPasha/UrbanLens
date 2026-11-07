# Observed history

History tracks changes seen in monthly FSA/OSM venue data and Companies House
records. ONS statistics and postcode lookups remain dated reference data.

| Observation | Result |
| --- | --- |
| First successful collection in a scope | Baseline; no mass additions |
| One complete monthly absence | Pending; previous support retained |
| Second consecutive complete monthly absence | Source no longer observed |
| Failure, incomplete collection or collection gap | No absence; restart the streak |
| Replay/retry of the same accepted collection | No extra absence or event |
| Another accepted source still supports the venue | Venue remains observed |
| Return after confirmed disappearance | Reappeared; not a verified reopening |
| Matching/taxonomy/review correction | Processing correction |

Company dissolution changes legal context, never venue observation status.
Observed additions/disappearances are not verified business openings/closures.

```powershell
python scripts/dev.py history run --target local
```

Use `--target neon` for Neon. `run` replays the already loaded Silver bundle and
projects current venue/company state. It reads private S3 evidence, not provider
APIs. After new source data, finish classification/location/resolution and company
context before projecting history; stale venue states are rejected.

Normal source loads now assess history automatically. Resolution and company loads
record their respective changes. `history sources` assesses source state only;
`history project` checks the resulting entity history without replaying the bundle.
The source command scans retained attempt manifests for failures. Scope/query
changes require an explicitly reviewed scope rather than counting incompatible
collections as absences. New collections need fresh acquisition metadata and raw
objects belonging to their own run; unchanged payload hashes are allowed.
Normal company edition changes and the approved daily-to-monthly cadence migration
are compatible; changing the set of tracked profile requests requires review.

Files: `src/urbanlens/silver/history.py` contains the rules; `scripts/history.py`
provides commands; migration `0011` stores baselines and current history state.
`data/silver/history-local.json` / `-neon.json` report results and S3 evidence.
Events retain before/after normalised values, source references and processing
versions in private `evidence/history/` objects, independently of raw-file expiry.
