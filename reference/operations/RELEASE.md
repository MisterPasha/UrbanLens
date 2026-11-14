# Release review - 14 November 2026

**HOLD: step 58 is not complete.** No public application has been enabled.
Active data: `4948727a-c526-4f8d-a2ea-206b16bba518` (1,178 precise venues,
165 areas, 1,378 companies, 66 observations, zero verified company links).
Population: 30 June 2024; geography: LSOA-2021. Active data remains unchanged.
Private validated candidate: `b74a8f8a-578f-4c2a-9b16-e7f90c6c07cf`.
All 165 workbook links are corrected; 11 statistical tables compare identically.
13 models and 16 dbt checks pass; candidate API access is denied.

Statuses below distinguish passed preflight evidence from checks requiring the
public deployment. Full pins, hashes, versions and measurements are retained in
`data/release-candidate/candidate.json`; these are not public download files.

| Gate | Status | Evidence / remaining action |
| --- | --- | --- |
| RC-01 Scope/access | Pending public check | Fixed Brighton & Hove/hospitality; anonymous browser journeys pass. Verify public HTTPS/no-login in step 59. |
| RC-02 Five sources | Passed | All five source-to-UI traces retained, including postcode enrichment. Frozen candidate has 165 correct population-workbook links; source/review versions unchanged. |
| RC-03 Consistent totals | Pending public check | Frozen hashes and API totals reconcile; browser map/list/category scenarios pass. Repeat on the deployed URL. |
| RC-04 Radius | Passed | Existing metre/boundary/PostGIS evidence plus current radius browser scenarios; precise counts exclude approximate points. |
| RC-05 Comparison | Passed | All 165 areas, category/rate/age/population/land statistics match active data; corrected provenance is frozen in the candidate. |
| RC-06 Venue evidence | Pending public check | Current detail, withholding, rating and stale-response browser scenarios pass; repeat on the public URL. |
| RC-07 Companies | Passed | 1,378 registered-office companies; zero operator links, so none await approval. Registered office is explicitly not a trading-premises claim. |
| RC-08 History | Passed | 66 observed venue changes, zero company events, explicit baselines; frozen hashes unchanged and no synthetic evidence markers. Existing absence/failure/reappearance checks retained. |
| RC-09 Reproducibility | Passed | Retained-input replay, interrupted company partition rejection and identical repeat state recorded in steps 52-53. |
| RC-10 Publication/quality | Blocked | Active fingerprints and private Lambda smoke pass. Corrected validated candidate and reviewed software deployment still required. |
| RC-11 Coverage/audit | Passed | Actual coverage retained; 36-record production audit covers categories, source overlap, uncertain/nonmatched records and coordinate quality. |
| RC-12 Performance | Passed, private scope | Ten-reader private workload: warm analytical p95 356 ms; cold 4.9 s, no browsing errors. Gateway/browser smoke remains step 59. |
| RC-13 Usability | Passed | Current desktop/mobile browser scenarios, keyboard/error states and 12 accessibility audits pass. |
| RC-14 Cost/quotas | Blocked | Owner reports USD 0.03 and 1.96/100 CU-hours; DB 539.6 MB/1 GB after preparation. London quota remains 10; AWS requires a request of at least 1,000 to meet our technical minimum of 104. |
| RC-15 Recovery | Passed, preflight | Real restore/rollback and today's backup passed. Candidate has three restore-safe functions; current verified release/reuse receipt is its rollback target after activation. Legacy target remains held. |
| RC-16 Documentation/delivery | Blocked | Licences, anonymous reuse ZIP, setup and runbooks exist. Workspace changes have no reviewed deployment SHA yet; public hosting configuration and URL remain absent. |

## Concrete remaining work

1. Completed with owner approval: fixed all 165 workbook links, built/tested/froze
   the private candidate and saved its verified reuse ZIP/evidence to private S3.
   Both publication pointers and every other tracked semantic state are unchanged.
   No provider downloads, public activation or CodeBuild run occurred.
2. Commit the tested changes, pass GitHub Foundation checks, and use the existing
   production owner approval for private deployment. Verify the corrected
   candidate/reuse offer and rollback target before activation.
3. Resolve the London Lambda quota and configure the existing Cloudflare Free
   deployment variables/secrets. GitHub currently has no Cloudflare environment
   secrets; credentials must never be pasted into chat or committed.
4. Step 59: enable the bounded API/Pages deployment, then verify HTTPS/no-login,
   real-data journeys, attribution/reuse, timestamps, quotas and operating alerts.

The owner explicitly approved private candidate preparation on 14 November,
following the initial automatic approval rejection. Preparation and verification passed.
Local PostGIS repeats could not run because Docker's engine was unavailable;
historical successful database/recovery evidence is retained, not relabelled as new.

Evidence: `data/step58-*.json`, `data/step58-*.log`, the current-evidence/candidate
records, and [existing evidence](../../docs/evidence/README.md).
