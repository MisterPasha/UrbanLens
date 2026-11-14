# Small performance check

```powershell
python scripts/dev.py benchmark
```

Uses the existing `urbanlens_dev` profile to read the private London Lambda API
and live Neon data. No deployment, public access, provider downloads or database
writes. Run while no refresh is active.

Ten visitors each make eight requests: metadata, filtered exploration, next page,
radius, comparison, detail, companies and activity. Five-second think time and
staggered starts keep demand around two requests/second. SDK retries are disabled;
each successful response must retain the selected release.

Results: `data/benchmarks/<timestamp>/report.json` and `requests.jsonl`.
Warm analytical p95 must be at most two seconds, with all 80 browsing requests
successful. Metadata, errors and observed Lambda cold starts are excluded from
that percentile. Lambda logs identify cold starts; Neon wake is not independently
identifiable. This mix has no application response-cache hit path.

This is a short backend check, not a sustained capacity guarantee. Cloudflare,
API Gateway, map tiles and browser rendering are outside the measurement. Verify
public throttling and reserved concurrency when enabling public access.
See [measured results](../../docs/evidence/performance-benchmark.md).
