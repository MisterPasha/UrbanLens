# Accepted-input rehearsal

Prerequisites: project setup, running local PostGIS with scoped roles, and the
`urbanlens_dev` AWS profile. The production stack needs a completed refresh and no
active workflow.

```powershell
python scripts/dev.py db-up
python scripts/dev.py database setup --target local
python scripts/dev.py rehearsal
```

The last command creates a separate local database, applies migrations, reads the
last successful run's retained S3 inputs and accepted reviews, builds Silver and
Gold, publishes locally, rejects an incomplete company partition, and repeats the
processing to compare semantic hashes and complete release inputs, including code.
It traces all five sources through real API reads. It removes its database on exit.

For step 53, add `--quality` to inject candidate faults, reconcile filters and audit
real records. `--reuse-inputs data/rehearsals/<timestamp>` reuses verified Silver
and classification manifests from a successful rehearsal of the same refresh;
database state is always rebuilt. See [data quality](../quality/README.md).

Expect about 20 minutes locally. Results are in `data/rehearsals/<timestamp>/`;
private stage logs remain under `.cache/rehearsal-<timestamp>/`. Neither belongs in
GitHub. AWS receives immutable derived evidence; no upstream provider is polled,
no CodeBuild job is started, and Neon is not written. Public access is unchanged.

This fresh current-input baseline does not recreate historical changes. Recovery
of retained production history is step 54. Browser screenshots and live private
API/schedule/capacity checks supplement the command in the acceptance report.

See [step-52 evidence](../../docs/evidence/end-to-end-rehearsal.md) and
[capacity forecast](COSTS.md).
