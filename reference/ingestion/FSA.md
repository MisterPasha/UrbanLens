# FSA collection

Refresh policy: monthly. Existing November observations count for November; their original metadata remains unchanged. The next routine collection is December 2026.

Collects the complete Brighton and Hove food-hygiene extract. Retains every source record before any hospitality filtering or venue matching.

```powershell
python scripts/dev.py fsa-fetch
python scripts/dev.py fsa-check
```

- `fsa-fetch`: discovers the extract through the authority API, saves raw files, validates and normalizes records, then selects the accepted snapshot. Repeating collection within an accepted UTC calendar month reuses it.
- `fsa-check`: rebuilds from saved files without downloading; compares normalized hashes. Does not advance source freshness or replace the current snapshot.

| File | Use |
| --- | --- |
| [config/fsa-source.json](../../config/fsa-source.json) | Authority mapping, version and count limits; change deliberately when scope changes |
| [fsa.py](../../src/urbanlens/ingestion/fsa.py) | Download, XML parsing, source-field normalization and privacy handling |
| [fsa_source.py](../../scripts/fsa_source.py) | Fetch/check command implementation |
| [fixtures](../../tests/fixtures/fsa/valid.xml) | Invented premises for offline tests |
| [complete manifest](examples/fsa-complete.json) | Synthetic four-record accepted snapshot, with illustrative artifact locations |

Local output is in ignored `data/ingestion/`: immutable raw/normalized files and manifests under `objects/`, durable run/current state in `registry.sqlite`, and the latest replay summary in `fsa-check.json`. Raw artifacts remain after parsing failures; failed/rejected collections cannot replace accepted data. Rejection-reason summaries accompany parsed attempts.

`source_extract_date` records FSA's date-only publication field. `fetched_at` records our download time in UTC; unknown source timestamps stay null. An unchanged publication date is allowed. IDs and original identity fields remain available alongside normalized values.

Missing coordinates retain useful records. Withheld or entirely unavailable addresses receive a hold state; normalized address/coordinates are suppressed, without inferring hidden locations. Nonnumeric hygiene ratings remain distinct states, never score zero. Raw source files are internal evidence, not public exports.

Downloads use bounded retries, sizes, time and requests. FSA alone opts into at most two same-host HTTPS redirects; every hop counts against the request budget. Collection rejects malformed/truncated XML, inconsistent counts, wrong authority, duplicate IDs, missing required fields and implausible volume (1,000–20,000 records; maximum 20% drop from current).

Acceptance here validates source ingestion. Canonical hospitality venues, database loading, scheduling and publication are later steps.

Provider guidance: [FSA best practices](https://api.ratings.food.gov.uk/Help/BestPractices), [API](https://api.ratings.food.gov.uk/Help/Index), [open data](https://ratings.food.gov.uk/open-data).
