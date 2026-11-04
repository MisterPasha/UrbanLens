# Step 08 private pilot

**Completed and runtime resources removed, 3 November 2026.** [Measurements](../../docs/evidence/runtime-feasibility.md).

The London pilot verified API Lambda cold/warm invocation, seven company-data downloads, private S3 persistence and deterministic replay. dbt failed on Lambda's shared-memory limitation; on-demand CodeBuild passed dbt/load/native backup. The actual downloaded backup restored with all counts matching. See the [runtime decision](../../docs/decisions/0002-pipeline-runtime.md).

## Access

An administrator owns `urbanlens-step08-access`, created and updated from [access.yaml](access.yaml). It attaches a temporary scoped developer policy and supplies the fixed runtime permissions boundary. The developer cannot administer users, keys or that boundary. **This access stack remains**; remove it through the administrator Console when no longer needed.

CLI profile: `urbanlens_dev`. Credentials stay in local AWS configuration.

## Runtime and cost

[template.yaml](template.yaml) defined two private Lambda functions, one private S3 bucket and one on-demand CodeBuild project, with separate bounded execution roles and one-day logs. No public endpoints, schedules or reserved build fleet. Images were digest-pinned; a private `NoEcho` parameter supplied the Neon scratch connection. Bootstrap inputs were hash checked.

The [London planning envelope](../../config/pilot-budget.json) allows 26 Lambda invocations, two bounded CodeBuild attempts, 3 GB-month ECR, 1 GB-month S3 and a log/request buffer: approximately **US$0.99 / £1.99**, within the **£2 reserve**, without assuming free allowances. Owner confirmed £0.00 current-month charges before deployment. This is an estimate, not a guaranteed billing cap.

## Commands

Historical commands from the project root with its virtual environment; **the completed pilot is not a running refresh service**:

```powershell
python -m scripts.cloud_pilot preflight --profile urbanlens_dev
# Do not repeat these completed paid-pilot steps against the retained state.
python -m scripts.cloud_pilot dbt-update --profile urbanlens_dev
python -m scripts.cloud_pilot dbt --profile urbanlens_dev
python -m scripts.cloud_pilot cleanup --profile urbanlens_dev
```

Helpers retain private state and enforce attempt limits; installing the project runs nothing in AWS. Both days' managed evidence lives in ignored `data/pilots/runtime/2026-11-02/`.

Cleanup removed the owned pilot's objects, runtime stack, logs and ECR repositories; deletion was verified. It left the administrator access stack and pre-existing Stockholm bucket untouched. Accrued charges can appear later. Local Docker data and Neon scratch objects remain available.
