# Bronze storage

Create a **new** CloudFormation stack named `urbanlens-bronze` in **Europe (London)** using [template.yaml](template.yaml). Set `DeveloperUser` to the IAM username behind the `urbanlens_dev` CLI profile and acknowledge IAM resource creation. This needs an administrator because the development identity cannot create this infrastructure.

The stack creates:

- One private, encrypted S3 bucket with ACLs disabled; raw `bronze/` objects expire after 90 days. Manifests, reviews and normalized evidence use separate prefixes.
- One Python 3.13 Lambda with a five-minute timeout, 1 GB memory and 1 GB temporary disk; seven-day logs.
- Three **disabled** monthly rules: FSA, OSM and tracked company profiles on the seventh at 08:00, 09:00 and 10:00 UTC.
- A runtime role and a developer policy scoped to these resources. No IAM administration or object deletion is granted to the developer.

The initial Lambda deliberately fails until the real package/catalog is uploaded. Schedules stay disabled until live verification passes. Companies House uses a private SSM SecureString, configured from the local key without printing it.

The account's measured Lambda concurrency limit is 10, so a reserved concurrency of 1 cannot be configured while preserving AWS's required unreserved allowance. Scope leases prevent concurrent writers for the same source; schedules are staggered and automated retries are disabled.

Bucket deletion/replacement retains the bucket. Conditional writes prevent the application overwriting retained artifacts; this is not S3 Object Lock. Retention starts at S3 upload time; manifests preserve the earlier source collection times. Lifecycle expiry means raw replay is only promised within the retention window. No public endpoint, database or application deployment is included.

AWS references: [conditional-write enforcement](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html), [lifecycle rules](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-s3-bucket-rule.html).
