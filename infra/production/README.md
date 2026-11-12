# Production setup

`foundation.yaml` creates two private ECR repositories, a runtime permission boundary,
scoped deployment access and GitHub OIDC trust. `template.yaml` creates the private
workflow, CodeBuild, API Lambda, schedules, budget alerts and dashboard. It reuses
the existing Bronze bucket. Public API access and paid schedules default to disabled.

## Administrator bootstrap

Create `urbanlens-production-access` in CloudFormation, **Europe (London)**, using
`foundation.yaml`. Set `ArtifactsBucket` to the existing Bronze bucket and
`DeveloperUser` to `urbanlens_dev`. Keep `GitHubRepository=MisterPasha/UrbanLens`.
Supply `ExistingOidcProviderArn` only if this account already has GitHub OIDC.
Acknowledge named IAM resources. This stack creates no running compute.

The administrator access stack is already deployed. These instructions are for
recreating it; do not use root access keys.

### Existing access stack: permission correction

The access-stack permission correction is complete, including Lambda retry settings
and runtime cleanup access. The private runtime was successfully recreated.

Permissions were checked against the [official London resource schemas](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/resource-type-schemas.html)
and service authorization references. Optional VPC, custom KMS, managed-role-policy,
log-delivery and code-signing operations are outside this configuration. Runtime
roles retain the fixed permission boundary.

## Runtime preparation

```powershell
python scripts/dev.py production prepare
python scripts/dev.py production configure-secrets
```

Preparation uploads verified nonsecret reference assets and configuration to the
existing private bucket. Secret configuration copies the five existing Neon role
connections into scoped encrypted SSM parameters; it prints no values.

Build/push the `api` and `pipeline` Dockerfile targets as Linux AMD64 images using
`containers/base-image.txt`. Use ECR **digest** URIs, not mutable tags, with:

CI identifies software by the tested commit SHA. Local rehearsals use the first
40 characters of the immutable pipeline digest, so uncommitted work is not recorded
as code from an earlier commit.

```powershell
python scripts/dev.py production deploy --wait --api-image <digest-uri> --pipeline-image <digest-uri> --runtime-config-key <prepared-key> --bucket <existing-bucket> --alert-email <your-email>
```

Confirm the SNS subscription email. Resume **private** processing only after checking
headroom. Test migrations and a backup before enabling monthly automation. Disable
the three old `urbanlens-bronze-*` rules before enabling the replacement refresh rule;
both collectors must not schedule the same source independently.

The alert address can also be saved locally as `URBANLENS_ALERT_EMAIL` in `.env`;
then `--alert-email` is optional.

The earlier Lambda quota was 10 concurrent executions. Public release requires at
least **104** (100 unreserved plus four reserved); request an increase if needed.
Then set `ReserveFunctionConcurrency=true`. The private setup can be validated
without reservations. API Gateway is created only when both that setting and
`ApiEnabled=true` are present. Public activation belongs to step 59.

See [operations and deployment](../../reference/operations/README.md).
