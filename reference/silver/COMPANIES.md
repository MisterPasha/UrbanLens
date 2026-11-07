# Company context

Registered offices describe legal companies, not competitors. The two company
cohorts can overlap: hospitality-SIC registered offices in Brighton & Hove, and
reviewed operators of eligible local venues (including outside registered offices).

```powershell
python scripts/dev.py database migrate --target local
python scripts/dev.py companies run --target local
```

Use `--target neon` for Neon. These commands reuse accepted observations and the
existing private S3 bucket; they do not fetch new provider data or publish anything.
`companies schema` regenerates `company-links.schema.json` offline.

Files:

- `src/urbanlens/silver/companies.py`: legal facts, cohorts, relationship validation and load.
- `config/company-links.json`: complete reviewed relationship register; initially empty.
- `0010_company_context.sql`: company provenance and private cohort view.
- `data/silver/company-context-local.json` / `-neon.json`: load summaries and S3 evidence location.

Company context retains source dates, status, SIC codes, incorporation/cessation
field states, registered-office quality and official profile links. The latest
observation supplies legal facts; a profile wins equal-time ties. A profile office
that differs from accepted local bulk evidence loses local-office membership.
An absent cessation date is never an inferred closure date.

To add a relationship, inspect the saved S3 JSON: companies include `record_hash`;
`relationship_review_inputs` includes eligible venue IDs and hashes. Retain explicit
operating evidence in private S3, then complete the schema's review fields, including
the original HTTPS URL, operating statement, reviewer and review date. Name, brand,
postcode, director or operator-tag matches alone are insufficient. Increment the
register version and run the loader. Changed source/venue hashes require re-review.
Removing/rejecting an entry retires the prior link while retaining its evidence.
One company may have several premises; venues do not need a company association.

Coverage uses all current resolved eligible venues, including unlocated ones.
Zero verified links is a valid result. Monthly provider refreshes remain unchanged.
