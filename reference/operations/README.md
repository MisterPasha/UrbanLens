# Operations

Once enabled, monthly refresh runs on the 7th at 08:00 UTC. One CodeBuild job sequences bounded
FSA/OSM/profile Lambdas, the complete company cohort, postcode enrichment, reference
checks, Silver, dbt tests, backup and atomic publication. Stages save S3 checkpoints.
Partial cohorts, stale reviews or failed checks cannot replace the public release.
Changed reference editions are staged for compatibility review; checks do not alter
the population reference date. Configured daily backups run at 03:00 UTC. Health checks at
13:00 UTC read saved summaries and never wake Neon.

## Commands

```powershell
python scripts/dev.py production status
python scripts/dev.py production smoke --private
python scripts/dev.py production start --job migrate
python scripts/dev.py production start --job backup
python scripts/dev.py production start --job refresh
python scripts/dev.py production retry --job refresh
python scripts/dev.py production cutoff
python scripts/dev.py production resume --bucket <existing-bucket> --confirm-headroom
```

Duplicate deliveries reuse their period's job; retries are explicit and limited to
two attempts. CodeBuild concurrency is one, timeout 30 minutes, queue timeout five
minutes. A durable global lock prevents conflicting jobs/manual pipelines. A daily
watchdog stops overdue work; an ambiguous start remains locked until the matching
build is found or an operator verifies no job exists. Never clear that lock blindly.
Completed company partitions are reused on retries; national downloads are not
repeated. Owner review packets in `data/reviews` are never deployed or auto-approved.

An explicitly authorised recovery beyond the normal allowance uses:

```powershell
python scripts/dev.py production recover --bucket <existing-bucket> --confirm-recovery --reason "Owner-approved recovery after diagnosed fix"
```

Each invocation creates an approval bound to the exact previous failed state and
deployed software, archives that state and starts one job. It does not enable
automatic retries or change the normal two-attempt allowance.

## Monitoring and cutoff

Use the **UrbanLens** CloudWatch dashboard, scoped alarms and SNS emails. Warnings
cover errors/throttles, API p95 above two seconds, workflow/backup age, storage growth,
and saved Neon storage/connections. Monthly deadlines include 48 hours' grace.
Storage warns at 800 MB of the reported 1 GB allowance. Saved usage measurements
come from real jobs; they are not synthetic keep-alives. Check Neon compute usage
and remaining free allowances in its console before resuming: no Neon account API
key is configured. Daily backup hashes are verified; isolated restore verification
is separate and remains a step-54 release requirement.

The owner reported **GBP 0.01** month-to-date on 11 November 2026. Keep the GBP 5/10
review points and act before the forecast reaches GBP 15. AWS-native budgets use
additional conservative **USD 5/10** actual/forecast warnings and a **USD 15** cutoff.
These are different currencies, not an exact conversion. Both budgets cover the
account and include tax; free quotas, credits, exchange rates and other projects
must be checked independently. See [cost assumptions](COSTS.md).

Cutoff first pauses new jobs, disables old/new paid schedules, throttles the API and
collector, stops active builds, and requests a CloudFormation update removing API
Gateway. Verify the stack update completes and the API URL disappears. Partial AWS
permission failures are reported and retried by the health check. Storage, registry,
logs, in-flight work and billing lag can still incur charges: this is not a guaranteed
billing cap. Keep Cloudflare Pages/Workers and Neon on Free plans. The Pages API
proxy returns `503 service_limited` when disabled; the website must not display zero
venues for unavailable data. No automatic paid upgrades or retrying fallback exists.

Resume private processing after checking spend/forecast, Neon allowances and failure
cause. Re-enable API/schedules and Cloudflare `SERVICE_ENABLED` separately under the
release procedure. Restoring processing does not automatically reopen public access.

## CI/CD

`ci.yml` uses fixtures and disposable PostGIS, validates templates, scans credential
patterns, checks Python/frontend/contracts/dbt/browser flows, and builds separate
locked API/pipeline images. It does not fetch live providers or run production ELT.
Frontend artifacts are retained for three days; local environments and secrets are
excluded from image contexts and artifacts.

The GitHub **production** environment now requires owner approval and restricts
deployment to main. Variables `AWS_DEPLOY_ROLE` (foundation output), `BRONZE_BUCKET`
and `RUNTIME_CONFIG_KEY` are configured; add `API_URL` at public activation. Software updates
retain the alert address already configured in AWS.
No AWS access key or database URL belongs in GitHub. Dispatch a full commit SHA with
successful push CI. The workflow deploys the pipeline, runs forward migrations,
then updates API readers. It preserves existing public/schedule settings.

Step 59 also needs `CLOUDFLARE_PROJECT`, `CLOUDFLARE_FREE_PLAN_CONFIRMED=true`, and
environment secrets `CLOUDFLARE_API_TOKEN`/`CLOUDFLARE_ACCOUNT_ID`. Use a token scoped
to the Pages project/account. Configure Pages `API_ORIGIN` with the verified gateway
URL and `SERVICE_ENABLED=false` until public smoke checks pass. Only `/api/*` invokes
Pages Functions; static files remain static. Exhausted free request quotas must fail
closed, not upgrade the account or retry upstream.

For software rollback, dispatch the previous tested SHA and inspect the resulting
stack/smoke checks. Migrations remain forward-only; require backward compatibility.
ECR tags are immutable and deployments use digests. A new push is blocked at eight
stored images per repository; review cleanup while protecting deployed/rollback
digests. Untagged images expire after seven days. Data rollback is independent:
`python scripts/dev.py release rollback --target neon`.

The pipeline registry currently contains nine manifest records: the final local
Docker push also added an image index and attestation. The deployed AMD64 manifest
is valid. New image uploads are blocked by the retention guard until unused records
are reviewed/removed; protect deployed and rollback digests. Deployment builds now
disable provenance indexes to produce Lambda-compatible single-platform images.

## Current verification — 12 November 2026

Private infrastructure, migrations, API smoke checks, checksum-verified backup,
real cutoff/resume and email delivery passed. The monthly retry stopped at
classification loading; the published release remains unchanged at 1,178 venues.
Windows/Linux JSON encoding caused the version conflict. UTF-8 parsing and a new
immutable review version fix it; a read-only preflight now catches such conflicts
before collection. No pending owner reviews were applied.

The owner approved one additional private recovery attempt on 12 November. It
completed Gold and all 16 dbt tests, then stopped at release preparation: the worker
had omitted its run-specific source schema. The code now passes `--source-schema`
explicitly. This latest handoff fix has not been verified by another cloud run.
The published release remains unchanged; schedules and public access remain disabled.

The recovery approval is bound to the failed-state hash and deployed software in
`operations/recovery-approvals/`; attempt 2 is archived in `recovery-history/`.
It permits attempt 3 only. Reuse cannot start attempt 4. No retry counter was reset.
GitHub's first deployment also remains unverified until changes are committed/pushed.
