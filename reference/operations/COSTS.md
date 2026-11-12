# Operating cost assumptions

11 November 2026; owner-reported AWS month-to-date: **GBP 0.01**. This is a planning
model, not a measured production forecast or a billing cap. The full rehearsal in
step 52 must replace duration/storage assumptions with actual measurements.

| Driver | Bounded configuration / planning scenario |
| --- | --- |
| Collection | One monthly cycle; at most two explicit attempts; tracked profiles capped at 50 with a shared request budget |
| Processing | One small on-demand CodeBuild worker, 30-minute timeout; daily backups plus monthly refresh/migration jobs; no fleet |
| API | 512 MB, ten-second timeout, two reserved executions after quota approval; gateway 2 requests/sec with burst 4 |
| Traffic example | 100,000 monthly requests averaging two seconds = 100,000 GB-seconds; free allowances must be verified |
| Object storage | Three monthly raw editions at most; seven daily/four weekly backups; existing history retention rules protect retained evidence |
| Images/logs | Eight images per repository before cleanup review; untagged expiry seven days; logs seven days |
| Monitoring | One dashboard, five alarms, warning budgets and SNS-triggered cutoff; verify remaining free quotas |

Measured during private rehearsals: database approximately 444 MB, verified dump
45,265,673 bytes, selected images 660,324,803 compressed bytes combined. The second
failed refresh spent approximately 164 seconds in its build phase; this is not a
full-cycle duration. ECR storage includes retained versions and shared layers.
Current Neon compute consumption is unavailable.

Calculate the forecast using current London prices for compute minutes, Lambda
requests/GB-seconds, HTTP API requests, compressed ECR GB, S3 GB/requests and logging.
Allow headroom for retries, cold starts, retained backups/images, tax, currency
conversion and billing delay. Do not assume credits or reserved/provisioned capacity.
Cloudflare and Neon stay on their confirmed Free plans; quota exhaustion suspends
service rather than authorising an upgrade. Review spend at GBP 5/10 and respond
before the GBP 15 forecast; AWS-native USD warnings/cutoff provide extra protection.

Sources: [AWS Budgets limitations](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-best-practices.html),
[Lambda pricing](https://aws.amazon.com/lambda/pricing/),
[CodeBuild pricing](https://aws.amazon.com/codebuild/pricing/),
[S3 pricing](https://aws.amazon.com/s3/pricing/),
[ECR pricing](https://aws.amazon.com/ecr/pricing/),
[API Gateway pricing](https://aws.amazon.com/api-gateway/pricing/).
