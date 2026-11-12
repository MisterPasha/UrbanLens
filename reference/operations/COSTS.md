# Operating forecast - 12 November 2026

Last owner-reported spend: GBP 0.01 on 11 November. Current billing and Neon compute
consumption are not accessible to the development identity. This is a scenario
forecast before free allowances, tax and currency conversion, not an invoice or cap.

Measured: full refresh approximately 5.5 minutes, standalone backup 45 seconds,
Neon 528,556,032 bytes and two connections after publication. Daily backups are
checksum-verified; isolated production restore is step 54. Current S3 objects total
1.25 GB across 835 objects; noncurrent versions are excluded from that measurement.

| Monthly scenario | Estimated USD |
| --- | ---: |
| CodeBuild: 30 backups rounded to 1 minute, two 6-minute refreshes, four 5-minute software jobs at $0.005/minute | 0.31 |
| One dashboard and five standard alarm metrics, ignoring free allowances | 3.50 |
| Retained ECR images: conservative 3 GB at $0.10/GB-month | 0.30 |
| S3: 10 GB at London Standard $0.024/GB-month | 0.24 |
| Logs, object requests, collection/control Lambda, SNS, KMS and transfer reserve; an allowance to verify in step 52 | 2.00 |
| Private-operation scenario subtotal, rounded | 6.35 |
| Later public scenario: 100,000 requests, 512 MB, average 2 seconds; Lambda duration/requests plus London HTTP API | 1.81 |
| Public scenario total, rounded | 8.16 |

London catalogue rates checked on 12 November: CodeBuild Linux general1.small
$0.005/minute; Lambda $0.0000166667/GB-second and $0.20/million requests; HTTP API
$1.16/million; standard alarm metric $0.10/month; S3 Standard $0.024/GB-month.
Dashboard and ECR rates use the official pricing pages. Regional catalogues are at
`https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/eu-west-2/index.json`
for CodeBuild, AWSLambda, AmazonApiGateway, AmazonCloudWatch and AmazonS3.

The 10 GB object and 3 GB registry figures are conservative scenario allowances,
not measured account totals. Step 52 must reconcile storage versions, API response
sizes, current billing, remaining Free allowances and Neon compute. Extra retries,
software deployments and traffic increase costs. No paid upgrade is authorised.

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
