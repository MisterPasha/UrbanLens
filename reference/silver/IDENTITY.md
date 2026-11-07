# Establishment identity — step 23

Combines accepted source relationships into stable internal UUIDs. Uncertain
records keep separate IDs and remain outside primary spatial totals.

| File | Purpose |
| --- | --- |
| `config/identity-rules.json` | Candidate distances, similarity thresholds and guarded reference policy |
| `config/identity-reviews.json` | Your 139 decisions, bound to original source hashes/generations |
| `src/urbanlens/silver/identity.py` | Reference validation, candidates, reviews and accepted identity graph |
| `resolution.py` | Stable UUID registry, linked eligibility, location selection and database loading |
| `scripts/resolve.py` | Run resolution and prepare a JSON review queue |
| `scripts/identity_reviews.py` | One-time import from the original reviewed pilot snapshots |
| `src/urbanlens/database/sql/0008_establishment_resolution.sql` | Private canonical, candidate and input state |

```powershell
python scripts/dev.py resolve run --target neon
python scripts/dev.py resolve run --target local
```

Requires current source, classification and location assessments and migration
0008. Reuse the existing database to preserve UUIDs. When populating a secondary
database for the first time, use `--registry data/silver/resolution-run-neon.json`
to copy canonical IDs; assessed inputs must match exactly. Existing conflicting
registries fail rather than silently merging or splitting established identities.

Rules:

- Normalise semicolon-separated FHRS IDs, preserving the original source keys.
  Invalid, unknown, multiple and unresolved repeated references require review.
- Automatically accept a unique reference only when names, house numbers,
  postcodes, categories and precise point distances pass conservative checks.
- Guarded node/area duplicates need an identical name, single shared reference,
  compatible addresses and points within 30 m. Retain every original feature URL.
- Name/address/proximity similarity discovers candidates; it does not accept them.
- Reviews bind both source hashes and generations. Changed facts leave a pair
  pending; conflicting or missing-generation mappings fail rather than guessing.
  Reviewed distinct businesses override automatic matching, including shared
  pub/kitchen premises. Accepted paths resolve redundant candidate pairs.
- Retain accepted FSA-only/OSM-only records. Eligibility, duplicates, location and
  observation status are independent gates. An accepted OSM link can supply fine
  category evidence to an otherwise unresolved FSA record.
- Prefer usable FSA points, then usable OSM points. Distinct precise points more
  than 75 m apart require location review even for an accepted identity. Keep all
  source alternatives in S3. Approximate points cannot enter primary totals.
- Source-ID reuse is flagged when a changed record moves substantially and its
  name/postcode changes. Preserve the previous baseline so reruns cannot erase
  the alert. Possible changed-ID continuity remains a candidate, not a lifecycle
  claim. This heuristic cannot identify every reuse.

`data/silver/resolution-run-{target}.json` contains counts and the S3 evidence
reference. Its `review_file` identifies an editable JSON queue containing only
pending pairs plus source holds. Existing answers are preserved; step 24 adds
review/correction loading. No need to repeat your original 139 decisions.

Your obsolete Wolfsmouth/Cyrlls records and the Fish & Chips/Pizza King correction
are held for field review. Eat Central is selected from your preferred OSM name.
Source observations are not overwritten.

Explicit, reviewed source-ID reuse can use `resolve rotate-generation --target
local --review path.json`. The JSON supplies `source` (provider, scope, source_key,
generation, record_hash), `reviewer` and `reason`. It archives evidence, retires the
old generation and reserves the next one; replay source loading, classification
and locations before resolving again. It creates no opening/closure events.
Do not run this for ordinary name/rating edits. It was tested only on synthetic
records; no real source generations were rotated during step 23.
