# Silver normalisation — step 20

Turns accepted source records into a shared, typed format. It preserves original values, explicit data-quality states and references to the original evidence. Eligibility, matching, company relationships and publication follow in later steps.

[Step 21 classification guide](CLASSIFICATION.md): eligibility/category decisions are stored separately from these source observations.

[Step 22 locations](LOCATIONS.md) and [step 23 identity](IDENTITY.md): spatial
checks, reviewed matches, stable establishment UUIDs and unresolved candidates.

[Step 24 reviews](REVIEWS.md): approved mappings, selected-field provenance and
correction history, including the preference for current OSM names.

[Step 25 companies](COMPANIES.md): legal context, separate company cohorts and
evidence-bound operating relationships.

[Step 26 history](HISTORY.md): monthly baselines, conservative absence tracking,
multi-source venue support and evidence-bearing observed changes.

| File | Purpose |
| --- | --- |
| `src/urbanlens/silver/models.py` | Observation types, field states and semantic hash contract |
| `normalise.py` | Rules for the five providers |
| `pipeline.py` | Read accepted evidence; write and verify immutable JSONL artifacts/manifests |
| `load.py` | Transactionally load bounded PostgreSQL working state |
| `scripts/silver.py` | Commands below |
| `observation.schema.json` | Generated description of one normalised record |
| `tests/test_silver.py` | Source fidelity, replay, changed-version and rollback checks |
| `src/urbanlens/database/sql/0005_observation_streams.sql` | Separate bulk/profile streams; postcode cache; preserve leading-zero FSA IDs |

## Use

```powershell
python scripts/dev.py silver build --storage s3
python scripts/dev.py silver check --storage s3
python scripts/dev.py database migrate --target local
python scripts/dev.py silver load --storage s3 --target local
```

Use `--target neon` on the last two commands for Neon. The loader uses its restricted database login. `build`/`check` also support `--storage local` for offline development; database loads require S3 references so provenance stays portable. `silver schema` regenerates the JSON schema.

These are explicit development/pipeline commands, not a running server. They reuse accepted evidence, make no source API requests, and leave monthly collection schedules unchanged. S3 commands use the existing private bucket and `urbanlens_dev` profile. National company parts take longer because original CSV values are recovered from the retained ZIPs.

## Record meaning

- **Identity:** provider plus source key. FSA IDs stay strings, OSM includes `node/way/relation`, and company numbers retain zeros/prefixes. Company bulk/profile records share a legal identity but keep separate observation streams.
- **Original/value/state:** fields retain original values alongside normalised values and distinguish missing, explicit null, invalid, withheld and unavailable. Withheld addresses/postcodes/coordinates stay suppressed; raw evidence remains private.
- **Locations:** WGS84/SRID 4326 with quality and role. Postcode centroids are approximate; a registered office is not a trading-premises point. Spatial eligibility here describes precision, not hospitality eligibility.
- **Ratings/dates:** nonnumeric FSA ratings remain named states, never zero. Invalid dates retain their original text. ONS statistical reference dates remain dates.
- **Times:** `observed_at` is the original acquisition time; `ingested_at` is the source collection/import run start. Extraction/base timestamps, feature edit dates, legal dates and population reference dates remain separate. Unreported publication dates remain null. Database processing time is recorded separately.
- **Traceability:** `evidence` contains artifact URI/hash and record locator; `field_sources` identifies the source fields. The normalisation version identifies the transformation rules. ONS records reference all pinned source products; source/reference definitions supply worksheet and edition details.

## Hash and storage rules

`record_hash` is SHA-256 of canonical JSON containing provider/key/kind, interpreted name/address/postcode/location/rating/company/area/postcode-result fields, normalised tags and selected source meaning. JSON keys and set-valued tags are sorted. Unicode uses NFKC, whitespace is collapsed, and postcodes use uppercase with one inward-code space. Free-text semicolons remain intact.

Transport URIs/hashes/locators, collection IDs, observation/ingestion/extraction/edit times, normalisation version, published fields' `.raw` members, `tags_raw` and cache expiry do not enter the semantic hash. Invalid/unknown typed fields retain their raw values in the hash. Original OSM geometry and selected provider-specific values, including original SIC descriptions, remain part of source meaning. Changing a meaningful field changes the hash; moving/replaying evidence does not.

S3 holds compressed full observations plus a manifest under `silver/normalised/` and `manifests/silver/`. Partitions include a decoded-content hash in their key; failed/unpublished builds never overwrite evidence. `.cache/silver-s3.json` selects the completed development bundle; it does not publish a product release or advance source freshness.

PostgreSQL holds current observations per provider identity/scope, changed versions, 165 area/population rows and one current row per cached postcode. Unchanged monthly records update last-seen metadata. Replaying a bundle creates no duplicate versions; an older source head is rejected; any failed batch rolls back the whole database load. Full monthly copies remain in S3. Reference-aware retention and backups are available through the [maintenance commands](OPERATIONS.md); scheduling follows in step 49.

Changed geography or population values require a new pinned version/edition; the loader rejects attempts to change existing reference values under the same key.

Machine-readable results are ignored files in `data/silver/`. Detailed failures are in private `.cache/silver-error.txt`. [Verification evidence](../../docs/evidence/silver-normalisation.md).

[Maintenance, backups, coverage and audit](OPERATIONS.md): steps 27?28 complete; [M2 audit](../../docs/evidence/m2-audit.md).

[Gold analytical models](../gold/README.md): steps 29?32 build private venue, area/city, company, activity and health marts from this Silver state.
