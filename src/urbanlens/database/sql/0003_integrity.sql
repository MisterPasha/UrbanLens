ALTER TABLE ul_control.source_snapshot ADD UNIQUE(snapshot_id,cycle_id);
ALTER TABLE ul_control.ingestion_run DROP CONSTRAINT replay_original_fk;
ALTER TABLE ul_control.ingestion_run ADD CONSTRAINT replay_original_fk
    FOREIGN KEY(replay_of_snapshot_id,cycle_id)
    REFERENCES ul_control.source_snapshot(snapshot_id,cycle_id);
ALTER TABLE ul_silver.observation_version ADD CHECK (valid_to IS NULL OR last_observed_at<=valid_to);
ALTER TABLE ul_silver.company
    ADD COLUMN registered_office_location_quality text NOT NULL DEFAULT 'missing'
        CHECK (registered_office_location_quality IN ('missing','postcode_fallback','unresolved')),
    ADD CHECK ((registered_office_location IS NOT NULL) = (registered_office_location_quality='postcode_fallback'));
