# Shared ingestion contracts

Step 10 defines metadata and interfaces; it does not collect data or implement storage clients.

Step 11 adds the implementations and commands: [collection utilities](RUNTIME.md).

Step 12 adds [FSA collection and replay](FSA.md), including a [complete synthetic manifest](examples/fsa-complete.json). Partitions may carry `source_extract_date` for date-only publication metadata; unknown precise source timestamps remain null.

Step 13 adds [OSM collection and replay](OSM.md), with a versioned query, retained geometry and checks for partial/error responses.

Steps 14–17 add [postcode/company/reference collection and source review](SOURCES.md). First baselines and material count changes require an evidence-bound inspection; structural failures cannot be approved.

| File | Purpose |
| --- | --- |
| `snapshot.schema.json` | Portable structure for a collection, its run, partitions, artifacts and validation |
| `run.schema.json` | Standalone run records, including retries and replays |
| `examples/complete-empty.json` | Synthetic accepted collection with explicitly zero records |
| `examples/replay-run.json` | Replay referencing that original snapshot/cycle |
| `examples/empty-source.json` | Synthetic raw bytes matching the example artifact's hash/size |

Example IDs, query hash and `file:///synthetic/...` location are illustrative, not live source evidence. Unknown source editions/timestamps remain `null`.

Python models: `src/urbanlens/ingestion/contracts.py`. JSON schemas describe structure; Python validation additionally enforces UTC, reconciled counts, complete partition sets, matching versions, validation and state transitions. Load persisted metadata with `model_validate_json`; use `advance_run` rather than unchecked model copies.

## Identity and outcomes

- A **cycle** identifies one intended collection window for a provider, scope and versioned query. Its query hash identifies the nonsecret query/configuration, including geographic scope. Pin expected partition identities before fetching.
- A **run** identifies one attempt. A retry gets a new run ID, keeps its cycle and references the immediately preceding failed, retryable attempt. `validate_retry` checks identity, ordering and unchanged pipeline version; durable registry/budget enforcement follows in step 11.
- A **snapshot** records a source observation, not just its content hash. Identical bytes collected in a later window have a different cycle/snapshot identity.
- A **replay** references the original snapshot and cycle; it does not create another fresh snapshot or advance freshness/absence counters. `validate_replay` checks the reference. Parsing replayed data uses retained artifacts, not `fetch`.
- Complete means every expected partition parsed all its raw records, with consistent edition/schema/pipeline versions. Accepted additionally requires reconciled accepted/rejected counts and all required blocking checks passing. Required defaults: schema, scope, completeness and counts; adapters extend these for provider-specific rules.
- `null` counts mean unknown; `0` means measured zero. Successful empty requires a complete, validated, accepted collection with zero raw records. Failures carry a category/code and never substitute an empty result.

Stages: `pending → fetched → parsed → complete → accepted → transformed → modelled → published`. Parsing/completeness validation can end in `rejected`; active stages can end in `failed`. Rejected, failed, published or otherwise finished runs cannot reopen. Raw artifacts can remain available after failure/rejection; they are not automatically accepted/current.

## Artifact conventions

```text
{layer}/{provider}/{scope}/{collection-date}/{cycle-id}/{run-id}/[partition-id/]filename
```

Layers: `bronze`, `silver`, `manifests`, `evidence`, `backups`. `artifact_key` rejects path escapes and unexpected partitions. Attempt-specific paths preserve earlier evidence; SHA-256 covers the exact stored bytes, including compression. URIs are absolute local `file://` or private `s3://` locations without credentials or signed queries.

`ProviderAdapter` separates fetch from retained-data parsing. `ArtifactStore` defines streaming immutable writes, verified reads and finalized-object inspection for local or S3 storage. Conflicting writes fail; partial writes cannot appear finalized. Step 11 implements these boundaries, leases, retries and current-snapshot selection; provider adapters follow separately.

Regenerate schemas after intentional contract changes:

```powershell
python scripts/dev.py ingestion-schema
```

The test suite checks generated schemas and examples against the models. No cloud credentials or live datasets are needed.

Step 18: [private S3 setup, replay and recovery](BRONZE.md).
