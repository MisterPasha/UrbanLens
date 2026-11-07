ALTER TABLE ul_silver.establishment
 ADD COLUMN duplicate_status text NOT NULL DEFAULT 'unresolved'
   CHECK(duplicate_status IN ('resolved','unresolved')),
 ADD COLUMN identity_decision jsonb NOT NULL DEFAULT '{}'
   CHECK(jsonb_typeof(identity_decision)='object' AND pg_column_size(identity_decision)<=65536),
 ADD COLUMN resolution_hash ul_control.sha256,
 ADD COLUMN resolution_artifact_id uuid REFERENCES ul_control.artifact,
 ADD CHECK(NOT primary_spatial_eligible OR duplicate_status='resolved');

CREATE TABLE ul_silver.identity_candidate (
 pair_id ul_control.sha256 PRIMARY KEY,
 left_identity_id uuid NOT NULL REFERENCES ul_silver.source_identity,
 right_identity_id uuid NOT NULL REFERENCES ul_silver.source_identity,
 decision text NOT NULL CHECK(decision IN ('accepted','rejected','pending')),
 method text NOT NULL,
 detail jsonb NOT NULL CHECK(pg_column_size(detail)<=65536),
 artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
 assessed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(left_identity_id<>right_identity_id)
);
CREATE INDEX identity_candidate_pending ON ul_silver.identity_candidate(decision);

CREATE TABLE ul_silver.resolution_input (
 identity_id uuid PRIMARY KEY REFERENCES ul_silver.source_identity,
 entry jsonb NOT NULL CHECK(pg_column_size(entry)<=65536),
 artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
 rule_sha256 ul_control.sha256 NOT NULL,
 flags text[] NOT NULL,
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
