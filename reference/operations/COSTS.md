# Operating forecast - 12 November 2026

Owner dashboard report on 12 November: **USD 0.02 AWS month-to-date**, **1.34 of
100 Neon CU-hours used**. These are owner-reported figures, not billing API reads.
The forecast below excludes free allowances, tax and currency conversion; it is
not an invoice or cap.

Measured after the rehearsal: full refresh **330 seconds**, standalone backup
**45 seconds**, backup dump **45.46 MB**. Neon is **449.77 MB**, with two connections;
the previous 528.56 MB measurement included obsolete workspaces since removed.
Silver occupies 231.42 MB; active/previous frozen schemas 50.13/50.14 MB; another
retained release 49.85 MB; existing development Gold 39.61 MB. These are relation
sizes, not separate additions to the database total. No automatic scratch remains.

S3 has **844 current objects / 1.262 GB**. Version listing is denied, so noncurrent
storage is unmeasured and explicitly covered by scenario headroom, not claimed
zero. ECR image-record sizes sum to **2.838 GB** across API 3 / pipeline 5 records;
shared layers make this an upper bound. Daily/weekly backup retention is 7/28 days:
seven daily plus four weekly dumps at the measured size need about **0.50 GB**.
Restore remains step 54.

| Monthly scenario | Estimated USD |
| --- | ---: |
| CodeBuild: 30 backups rounded to 1 minute, two 6-minute refreshes, four 5-minute software jobs at $0.005/minute | 0.31 |
| One dashboard and five standard alarm metrics, ignoring free allowances | 3.50 |
| Retained ECR images: conservative 3 GB at $0.10/GB-month | 0.30 |
| S3: 10 GB at London Standard $0.024/GB-month | 0.24 |
| Logs, object requests, collection/control Lambda, SNS, KMS and transfer reserve; unmeasured allowance | 2.00 |
| Private-operation scenario subtotal, rounded | 6.35 |
| Later public scenario: 100,000 requests, 512 MB, average 2 seconds; Lambda duration/requests plus London HTTP API | 1.81 |
| Public scenario total, rounded | 8.16 |

London catalogue rates checked on 12 November: CodeBuild Linux general1.small
$0.005/minute; Lambda $0.0000166667/GB-second and $0.20/million requests; HTTP API
$1.16/million; standard alarm metric $0.10/month; S3 Standard $0.024/GB-month.
Dashboard and ECR rates use the official pricing pages. Regional catalogues are at
`https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/eu-west-2/index.json`
for CodeBuild, AWSLambda, AmazonApiGateway, AmazonCloudWatch and AmazonS3.

The 10 GB S3 allowance covers current data, retained backups, monthly inputs within
the 90-day raw window and version/evidence headroom. The 3 GB ECR allowance is
close to the summed image-size upper bound: review retention before further image
growth. Each extra six-minute CodeBuild refresh is about **USD 0.03**, excluding
collection, storage and transfer. The USD 2 reserve is not a measured service bill.

Owner update, 14 November: AWS month-to-date **USD 0.03**, Neon **1.96 CU-hours used**.
Neon has **98.04 CU-hours remaining** and approximately **550 MB** database headroom
against the reported 1 GB limit. Database size is a proxy; the Neon storage meter
is authoritative. At unchanged size, another 50 MB candidate fits; review growth
at 800 MB, including retained history/releases. There is insufficient elapsed
history to claim an observed monthly database growth rate.

Thirty 45-second backups, two six-minute refreshes and four five-minute software
jobs, each with five idle minutes, imply approximately **0.98–7.82 CU-hours/month**
at 0.25–2 CU, before browsing. This is a wake-time scenario, not measured job CU.
Continuously awake 0.25 CU needs about **180 CU-hours in 30 days**, above the Free
allowance; public traffic must be measured in step 55. A 45.46 MB daily dump also
implies about **1.36 GB/month** of database transfer before API reads. Neon documents
100 CU-hours, 1 GB storage and 5 GB public transfer on Free:
[Neon plans](https://neon.com/docs/introduction/plans).

The private USD 6.35 / later public USD 8.16 scenarios remain reasonable planning
allowances, subject to unmeasured versions, request mix, free allowances and actual
usage. Extra retries/deployments increase costs. No paid upgrade is authorised.

Keep GBP 5/10 reviews and act before a GBP 15 forecast. Additional account-wide
AWS-native USD 5/10 warnings and USD 15 cutoff are separate currency thresholds.
Billing delay and retained storage prevent a guaranteed cap. See the tested cutoff
and recovery procedure in [operations](README.md).

Sources: [CodeBuild](https://aws.amazon.com/codebuild/pricing/),
[CloudWatch](https://aws.amazon.com/cloudwatch/pricing/),
[ECR](https://aws.amazon.com/ecr/pricing/),
[S3](https://aws.amazon.com/s3/pricing/),
[Lambda](https://aws.amazon.com/lambda/pricing/),
[HTTP API](https://aws.amazon.com/api-gateway/pricing/).
