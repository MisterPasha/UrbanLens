# Postcodes, companies, references and acceptance

All scheduled source work is **monthly**. These commands run locally; private S3 deployment and M1 acceptance are documented in [BRONZE.md](BRONZE.md).

```powershell
python scripts/dev.py pipeline postcode-check
python scripts/dev.py pipeline company-bootstrap
python scripts/dev.py pipeline company-profiles
python scripts/dev.py pipeline reference-import
python scripts/dev.py pipeline reference-check
python scripts/dev.py pipeline status
```

`company-bootstrap` verifies/reuses the retained November ZIPs. `company-fetch` is the recurring path: discover and pin the listing, download required parts, resume complete partitions, and replace the cohort only after all checks/review pass. Repeating accepted monthly work reuses it.

| Files | Responsibility |
| --- | --- |
| [postcodes.py](../../src/urbanlens/ingestion/postcodes.py) | Single/bulk lookup, input normalization, durable cache, explicit outcomes |
| [companies.py](../../src/urbanlens/ingestion/companies.py), [config](../../config/companies-source.json) | Full ZIP scans, all four SIC fields, registered-office cohort, tracked profiles and reviewed operator references |
| [reference.py](../../src/urbanlens/ingestion/reference.py), [config](../../config/reference-source.json) | Rebuild pinned ONS products in isolation; stage edition changes |
| [acceptance.py](../../src/urbanlens/ingestion/acceptance.py), [policy](../../config/source-acceptance.json) | First-baseline review, anomaly holds and immutable inspection evidence |
| [source_pipeline.py](../../scripts/source_pipeline.py) | Commands and source-health summaries |

Postcode states distinguish invalid input, unmatched, outside scope, incompatible geography, throttled, unavailable and resolved. Batches contain at most 100 unique postcodes. Positive/null caches last 30 days/one day; failures never become cached nulls. Only explicit `lsoa21` membership reconciles to the pinned geography. Search-centre use is separate from approximate establishment fallback; postcode centroids never become precise venue-count points. Final search-centre boundary checks remain API work.

Company manifests reconcile **all national scanned rows**, including exact exclusion totals. Out-of-scope rows use aggregate exclusion counts after full CSV/ZIP validation; eligible company IDs are individually deduplicated. This is not a national company-uniqueness guarantee. The postal-outcode prefilter can leave missing/new/out-of-vintage postcodes unclassified. Registered offices and SIC codes do not prove a venue operator. The verified-operator list is currently empty; reviewed operators can be included independently. Absence from the live bulk file does not mean dissolution.

Profiles use the existing private `.env` key. Default limits: 50 tracked companies and **100 attempted profile requests per UTC month**, including retries, tracked durably in `profile-requests.sqlite`. Raw files remain private.

Reference checks compare edition indicators and inspect the official population download listing. Changed candidates are saved for compatibility review; existing reference pins remain untouched. Repeat checks reuse the month's report. A staged import can specify `--reference-config`, `--reference-manifest` and `--reference-directory`; unsupported year/vintage changes require a migration.

First baselines are held for inspection. Later count changes are held when **both ≥20 records and ≥20%**, in either direction, globally or per partition. Inspect a candidate, then record the decision:

```powershell
python scripts/dev.py pipeline review --candidate PATH --reviewer NAME --reason "Inspection findings"
```

Rerun the original collection command to validate/promote the same retained inputs. Review alone does not promote data; it cannot override malformed, incomplete, duplicate or wrong-scope inputs. Failed/held runs retain the previous current source and its original dates. `status` reports these states.

Outputs are in ignored `data/ingestion/`: raw/normalized artifacts and manifests under `objects/`, postcode/request caches in SQLite, edition/check summaries, and `source-inspection.json`. Existing pilot inputs remain unchanged.

Provider contracts: [Postcodes.io bulk](https://postcodes.io/docs/postcode/bulk/), [lookup](https://postcodes.io/docs/postcode/lookup/), [company bulk](https://download.companieshouse.gov.uk/en_output.html), [profiles](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/companyprofile), [ONS population editions](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics). Cloud scheduling must allow company-edition publication delays.
