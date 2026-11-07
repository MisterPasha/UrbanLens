ALTER TABLE ul_silver.identity_scope_state
 ADD COLUMN value jsonb NOT NULL DEFAULT '{}' CHECK(pg_column_size(value)<=65536),
 ADD COLUMN processing_version text NOT NULL DEFAULT '',
 ADD COLUMN absence_interrupted boolean NOT NULL DEFAULT false;
CREATE TABLE ul_silver.history_scope (
 scope_id uuid PRIMARY KEY REFERENCES ul_control.source_scope,
 snapshot_id uuid NOT NULL REFERENCES ul_control.source_snapshot,
 cycle_id uuid NOT NULL REFERENCES ul_control.collection_cycle,
 window_start timestamptz NOT NULL,
 window_end timestamptz NOT NULL,
 query_sha256 ul_control.sha256 NOT NULL
);
CREATE TABLE ul_silver.history_attempt (
 run_id uuid PRIMARY KEY,
 scope_id uuid NOT NULL REFERENCES ul_control.source_scope,
 cycle_id uuid NOT NULL,
 window_start timestamptz NOT NULL,
 artifact_id uuid NOT NULL REFERENCES ul_control.artifact
);
CREATE TABLE ul_silver.history_projection (
 entity_type text NOT NULL CHECK(entity_type IN ('establishment','company')),
 entity_key text NOT NULL,
 value jsonb NOT NULL CHECK(pg_column_size(value)<=65536),
 processing_sha256 ul_control.sha256 NOT NULL,
 support_sha256 ul_control.sha256 NOT NULL,
 evidence jsonb NOT NULL CHECK(pg_column_size(evidence)<=65536),
 PRIMARY KEY(entity_type,entity_key)
);
CREATE TABLE ul_silver.history_baseline (
 entity_type text PRIMARY KEY CHECK(entity_type IN ('establishment','company')),
 established_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
