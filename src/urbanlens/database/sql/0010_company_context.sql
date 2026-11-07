-- Legal context remains separate from establishment discovery and spatial counts.
ALTER TABLE ul_silver.company
 ADD COLUMN context jsonb NOT NULL DEFAULT '{}' CHECK(pg_column_size(context)<=65536),
 ADD COLUMN context_sha256 ul_control.sha256;
CREATE TABLE ul_silver.company_context_run (
 artifact_id uuid PRIMARY KEY REFERENCES ul_control.artifact,
 definition_version text NOT NULL REFERENCES ul_control.review_definition,
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE VIEW ul_silver.company_cohort AS
 SELECT c.company_number,'registered_office'::text cohort,
        c.context->'registered_office_basis' basis
 FROM ul_silver.company c WHERE c.registered_office_cohort
 UNION ALL
 SELECT DISTINCT c.company_number,'verified_operator'::text,
        jsonb_build_object('basis','reviewed operating relationship',
                           'observed_at',c.last_observed_at)
 FROM ul_silver.company c JOIN ul_silver.company_establishment_link l USING(company_number)
 JOIN ul_silver.establishment e USING(establishment_id)
 WHERE l.valid_to IS NULL AND l.relationship IN ('operator','franchisee')
 AND e.eligibility='eligible' AND e.observation_state<>'no_longer_observed'
 AND e.duplicate_status='resolved';
