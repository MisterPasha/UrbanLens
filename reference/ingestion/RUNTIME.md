# Collection utilities

Step 11 implements the contracts. Provider-specific interpretation and acceptance rules follow in steps 12–16.

| Python file in `src/urbanlens/ingestion/` | Responsibility |
| --- | --- |
| `http.py` | Timed downloads, provider throttling, bounded retries and request/byte/time limits |
| `storage.py` | Compressed, write-once local/S3 artifacts and checksum verification |
| `registry.py` | Durable run checkpoints, scope leases and the accepted-snapshot pointer; SQLite locally or conditional S3 writes across workers |
| `validation.py` | Schema/scope hooks, duplicate detection and record-count checks |
| `engine.py` | Coordinate partitions, retain failed/rejected evidence, resume completed parts and accept validated collections |
| `commands.py` | Raw fetch, retained-file replay and metadata inspection |
| `errors.py` | Classified failures without source bodies, URLs or credentials in error messages |

## Commands

```powershell
python scripts/dev.py ingest --help
python scripts/dev.py ingest fetch --provider fsa --scope brighton-hove --url "<public-source-url>"
python scripts/dev.py ingest inspect "<snapshot-path>"
python scripts/dev.py ingest replay "<snapshot-path>" --format xml --record-tag EstablishmentDetail
```

Replace placeholders with actual values. JSON replay accepts a list, or `--records-key` for a list inside an object; CSV uses `--format csv`. ZIPs require the later provider-specific parsers.

Local state lives in ignored `data/ingestion/`: `objects/` holds artifacts/manifests and `registry.sqlite` tracks runs. The fetch command prints the snapshot path. Repeating the same provider/scope/URL/date reuses its raw snapshot. **Raw fetch is not acceptance; replay does not download data or change its collection time/current snapshot.**

Default limits: three HTTP requests, 100 MiB downloaded including retry bodies, 120 seconds HTTP/replay, 300 seconds per collection, one million parsed records, 128 MiB per artifact and 16 MiB combined reads/decompression for generic replay. Leases last 360 seconds; automatic consecutive run retries stop at three attempts. Provider adapters must configure limits from measured workloads.

For cloud use, inject a bounded-timeout SDK client into `S3ArtifactStore` and `S3StateBackend`; share the same bucket/control prefix across workers. SDK retries must be bounded independently. Permissions need prefix-scoped Get/Put and ListBucket to distinguish absent keys from denied access. Conditional writes protect lease/pointer updates together. Cloud IAM/client integration still requires a live deployment check; step 11 tests S3 with a simulated client. No cloud resources are created by installing or importing these utilities.
