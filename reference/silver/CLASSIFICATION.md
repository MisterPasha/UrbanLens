# Eligibility and taxonomy — step 21

Classifies source observations independently of coordinates, duplicate matching and observation status. It does not yet produce unique venue counts or publish a release.

| File | Purpose |
| --- | --- |
| `config/taxonomy.json` | Versioned eligibility rules and six categories |
| `config/eligibility-reviews.json` | Evidence-bound owner overrides; currently M091/M092 scope exclusions |
| `src/urbanlens/silver/eligibility.py` | Typed decisions, rules, stale-review checks and resolution of already accepted links |
| `classification.py` | Immutable S3 outputs, coverage checks and bounded database loading |
| `scripts/classify.py` | Build/check/load commands |
| `src/urbanlens/database/sql/0006_observation_classification.sql` | Private decisions and immutable rule-version definitions |
| `tests/test_eligibility.py` | Scope, category, review, conflict and coverage fixtures |

## Commands

```powershell
python scripts/dev.py classify build --storage s3
python scripts/dev.py classify check --storage s3
python scripts/dev.py database migrate --target local
python scripts/dev.py classify load --storage s3 --target local
```

Use `--target neon` on the last two commands for Neon. Offline build/check supports `--storage local` after a local Silver build. These commands reuse accepted records; no source API calls or schedule changes occur.

## Decisions

- Eligibility is `eligible`, `excluded` or `unresolved`, with rule IDs and reasons. Only eligible observations receive a primary category.
- Fine categories are café, restaurant, takeaway, bakery and pub/bar. Known public food service without one supported fine category stays `food_service`.
- FSA Restaurant/Cafe/Canteen and Pub/bar/nightclub remain unresolved without more specific evidence. Retailers–other never automatically means bakery. Takeaway/sandwich shop supports takeaway.
- Explicit OSM venue tags support fine categories. Private access excludes; institutions, general retail, accommodation and production require distinct public-venue evidence. A venue within a larger property needs explicit public access when that context is present. Lifecycle tags remain unresolved evidence, not verified closures.
- Multiple supported primary categories remain one eligible `food_service` observation with category ambiguity flagged. Original secondary tags remain available. Names alone never determine scope/category.
- Company, area and postcode records are marked not applicable to venue classification. Their legal/reference uses remain intact.

Tag interpretation follows the OSM documentation for [bakery shops](https://wiki.openstreetmap.org/wiki/Tag:shop=bakery), [access](https://wiki.openstreetmap.org/wiki/Key:access) and [canteens](https://wiki.openstreetmap.org/wiki/Tag:amenity=canteen); inclusion/category choices above are UrbanLens policy.

Each decision retains the original record hash, source evidence, automatic result, applied review and rule/config hashes. Overrides bind provider, scope, source key and record hash. Changed source meaning makes the review stale and eligibility unresolved until reconfirmed. Eligible overrides require explicit public-venue confirmation and documented evidence. The two existing exclusions affect only their FSA records, not Stoneham Bakehouse or Foodielicious.

Rules/reviews are copied to immutable S3 evidence. Changing a registered definition requires a new version; previous decision files remain in S3. PostgreSQL keeps one current classification per retained observation in `ul_silver.observation_classification`. Rule changes update this projection; they do not rewrite source observations or manufacture real-world events. API access remains denied.

## Review and coverage

The build writes a local `data/silver/eligibility-review-<manifest hash>.json` queue. Existing answers are preserved. For a clear case, fill `decision`, `primary_category` when eligible, `public_venue_confirmed`, `notes` and `evidence`. Reviewed answers can then be promoted to a new version of `config/eligibility-reviews.json` and rebuilt. Leave uncertain cases unresolved.

The queue need not be completed before step 22. Many coarse FSA cases may gain evidence after accepted matching in step 23. Identity/name/co-location reviews are separate from category overrides. The approved 10 November category review is preserved in `eligibility-reviews-v3`, which corrects note encoding without changing decisions; see [review decisions](REVIEWS.md).

Fine-category coverage is **eligible observations with a specific primary category / all eligible observations**, including broad/ambiguous ones. The report also shows all candidates and unresolved counts: this is provisional source-level coverage, with duplicates still present. It is not city-wide venue accuracy or a count of unique competitors.

Private results: `data/silver/classification-*.json`; completed artifact index: `.cache/classification-s3.json`. [Verified results](../../docs/evidence/eligibility-taxonomy.md).
