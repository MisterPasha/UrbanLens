-- Processing corrections can reinterpret the same snapshot without inventing a
-- fresh observation. Observed dates remain independent of processing validity.
ALTER TABLE ul_silver.observation_version
    ADD COLUMN processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN valid_from timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN revision integer NOT NULL DEFAULT 1 CHECK (revision>0),
    DROP CONSTRAINT observation_version_identity_id_first_snapshot_id_key,
    ADD UNIQUE(identity_id,first_snapshot_id,normalisation_version),
    ADD UNIQUE(identity_id,revision),
    ADD CHECK (valid_to IS NULL OR valid_to>valid_from),
    ADD CHECK ((location IS NOT NULL) = (location_quality IN ('source_point','point_on_surface','postcode_fallback')));
CREATE OR REPLACE FUNCTION ul_silver.changed_version_only() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE previous_hash text; previous_revision integer;
BEGIN
    PERFORM 1 FROM ul_silver.source_identity WHERE identity_id=NEW.identity_id FOR UPDATE;
    SELECT record_hash,revision INTO previous_hash,previous_revision
      FROM ul_silver.observation_version WHERE identity_id=NEW.identity_id
      ORDER BY revision DESC LIMIT 1;
    IF previous_hash=NEW.record_hash THEN
      RAISE EXCEPTION 'unchanged observation must update its existing version' USING ERRCODE='23514';
    END IF;
    NEW.revision=coalesce(previous_revision,0)+1;
    RETURN NEW;
END $$;
ALTER TABLE ul_silver.establishment
    ADD CHECK ((location IS NOT NULL) = (location_quality IN ('source_point','point_on_surface','postcode_fallback'))),
    ADD CHECK (location_quality<>'withheld' OR (address IS NULL AND postcode IS NULL));
ALTER TABLE ul_gold.establishment
    ADD CHECK ((location IS NOT NULL) = (location_quality IN ('source_point','point_on_surface','postcode_fallback'))),
    ADD CHECK (location_quality<>'withheld' OR (address IS NULL AND postcode IS NULL));
