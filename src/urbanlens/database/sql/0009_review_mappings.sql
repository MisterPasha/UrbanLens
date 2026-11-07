-- Date-only owner reviews must not acquire a fabricated midnight timestamp.
ALTER TABLE ul_control.review ADD COLUMN reviewed_on date;
ALTER TABLE ul_control.review DROP CONSTRAINT review_check;
ALTER TABLE ul_control.review ADD CHECK(
 (decision='pending' AND reviewer IS NULL AND reviewed_at IS NULL AND reviewed_on IS NULL)
 OR (decision<>'pending' AND reviewer IS NOT NULL AND length(reviewer)>0
     AND (reviewed_at IS NOT NULL OR reviewed_on IS NOT NULL))
);
CREATE TABLE ul_control.review_definition (
 version text PRIMARY KEY,
 sha256 ul_control.sha256 NOT NULL,
 artifact_id uuid NOT NULL REFERENCES ul_control.artifact
);
GRANT INSERT ON ul_control.review_definition TO urbanlens_loader;
CREATE TABLE ul_silver.review_mapping (
 mapping_key ul_control.sha256 PRIMARY KEY,
 review_id uuid NOT NULL UNIQUE REFERENCES ul_control.review,
 definition_version text NOT NULL REFERENCES ul_control.review_definition,
 action jsonb NOT NULL CHECK(pg_column_size(action)<=65536),
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE ul_silver.identity_correction (
 correction_key ul_control.sha256 PRIMARY KEY,
 identity_id uuid NOT NULL REFERENCES ul_silver.source_identity,
 previous_establishment_id uuid NOT NULL REFERENCES ul_silver.establishment,
 establishment_id uuid NOT NULL REFERENCES ul_silver.establishment,
 definition_version text NOT NULL REFERENCES ul_control.review_definition,
 artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(previous_establishment_id<>establishment_id)
);
ALTER TABLE ul_silver.field_provenance
 ADD COLUMN value_sha256 ul_control.sha256,
 ADD COLUMN detail jsonb NOT NULL DEFAULT '{}' CHECK(pg_column_size(detail)<=65536);
CREATE TABLE ul_silver.reviewed_replacement (
 replacement_key text PRIMARY KEY,
 establishment_id uuid NOT NULL UNIQUE REFERENCES ul_silver.establishment,
 review_id uuid NOT NULL REFERENCES ul_control.review,
 reported_on date NOT NULL,
 recorded_at timestamptz NOT NULL,
 detail jsonb NOT NULL CHECK(pg_column_size(detail)<=65536)
);
