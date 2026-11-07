-- The same legal company can appear in bulk and profile scopes. Keep one current
-- version per identity AND scope; neither stream overwrites the other's evidence.
ALTER TABLE ul_silver.source_identity DROP CONSTRAINT source_identity_check2;
ALTER TABLE ul_silver.source_identity ADD CHECK(provider<>'fsa' OR source_key ~ '^[0-9]*[1-9][0-9]*$');
ALTER TABLE ul_silver.observation_version ADD COLUMN scope_id uuid;
UPDATE ul_silver.observation_version o SET scope_id=s.scope_id
  FROM ul_control.source_snapshot s WHERE s.snapshot_id=o.first_snapshot_id;
ALTER TABLE ul_silver.observation_version
    ALTER COLUMN scope_id SET NOT NULL,
    ADD FOREIGN KEY(first_snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id),
    ADD FOREIGN KEY(last_snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id),
    DROP CONSTRAINT observation_version_identity_id_revision_key,
    ADD UNIQUE(identity_id,scope_id,revision),
    ADD UNIQUE(observation_id,identity_id,scope_id);
DROP INDEX ul_silver.one_current_observation;
CREATE UNIQUE INDEX one_current_observation ON ul_silver.observation_version(identity_id,scope_id) WHERE valid_to IS NULL;
ALTER TABLE ul_silver.identity_scope_state
    ADD FOREIGN KEY(current_observation_id,identity_id,scope_id)
        REFERENCES ul_silver.observation_version(observation_id,identity_id,scope_id);
CREATE OR REPLACE FUNCTION ul_silver.changed_version_only() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE previous_hash text; previous_revision integer;
BEGIN
    PERFORM 1 FROM ul_silver.source_identity WHERE identity_id=NEW.identity_id FOR UPDATE;
    IF NEW.scope_id IS NULL THEN
      SELECT scope_id INTO NEW.scope_id FROM ul_control.source_snapshot WHERE snapshot_id=NEW.first_snapshot_id;
    END IF;
    SELECT record_hash,revision INTO previous_hash,previous_revision
      FROM ul_silver.observation_version WHERE identity_id=NEW.identity_id AND scope_id=NEW.scope_id
      ORDER BY revision DESC LIMIT 1;
    IF previous_hash=NEW.record_hash THEN
      RAISE EXCEPTION 'unchanged observation must update its existing version' USING ERRCODE='23514';
    END IF;
    NEW.revision=coalesce(previous_revision,0)+1;
    RETURN NEW;
END $$;
-- Postcode results have dated lookup evidence, not fictitious collection snapshots.
CREATE TABLE ul_silver.postcode_cache (
    postcode text PRIMARY KEY,
    record_hash ul_control.sha256 NOT NULL,
    observed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK(expires_at>observed_at),
    state text NOT NULL CHECK(state IN ('unmatched','outside','incompatible-geography','resolved')),
    longitude double precision,
    latitude double precision,
    authority_code text,
    area_code text,
    dataset_version text,
    edition_note text NOT NULL,
    source_artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    silver_artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    CHECK((longitude IS NULL)=(latitude IS NULL)),
    CHECK(longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90)
);
