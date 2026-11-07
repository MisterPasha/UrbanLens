-- Source files and monthly observation copies stay in S3. No raw payload table.
CREATE DOMAIN ul_control.sha256 AS text CHECK (VALUE ~ '^[a-f0-9]{64}$');

CREATE TABLE ul_control.source_scope (
    scope_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider text NOT NULL CHECK (provider IN ('fsa','osm','companies-house','ons','postcodes-io')),
    scope_key text NOT NULL CHECK (length(scope_key) BETWEEN 1 AND 128),
    cadence text NOT NULL DEFAULT 'monthly' CHECK (cadence IN ('monthly','on-demand')),
    baseline_snapshot_id uuid,
    UNIQUE(provider,scope_key), UNIQUE(scope_id,provider)
);
CREATE TABLE ul_control.collection_cycle (
    cycle_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL REFERENCES ul_control.source_scope,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL CHECK (window_end > window_start),
    query_version text NOT NULL,
    query_sha256 ul_control.sha256 NOT NULL,
    source_edition text,
    UNIQUE(scope_id,window_start,window_end), UNIQUE(cycle_id,scope_id)
);
CREATE TABLE ul_control.ingestion_run (
    run_id uuid PRIMARY KEY,
    cycle_id uuid NOT NULL,
    scope_id uuid NOT NULL,
    mode text NOT NULL CHECK (mode IN ('collect','replay')),
    replay_of_snapshot_id uuid,
    pipeline_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending','fetched','parsed','complete','accepted','rejected','failed')),
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    retry_of_run_id uuid REFERENCES ul_control.ingestion_run,
    started_at timestamptz NOT NULL,
    ended_at timestamptz CHECK (ended_at >= started_at),
    failure_code text,
    FOREIGN KEY(cycle_id,scope_id) REFERENCES ul_control.collection_cycle(cycle_id,scope_id),
    CHECK ((mode='replay') = (replay_of_snapshot_id IS NOT NULL)),
    CHECK ((status='failed') = (failure_code IS NOT NULL)),
    CHECK (status NOT IN ('accepted','rejected','failed') OR ended_at IS NOT NULL),
    CHECK (mode <> 'replay' OR status NOT IN ('accepted','complete')),
    UNIQUE(run_id,cycle_id,scope_id,mode,status)
);
CREATE INDEX ingestion_run_scope_time ON ul_control.ingestion_run(scope_id,started_at DESC);

-- Only accepted, complete collections enter this table; rejected attempts stay in run/S3.
CREATE TABLE ul_control.source_snapshot (
    snapshot_id uuid PRIMARY KEY,
    cycle_id uuid NOT NULL UNIQUE,
    scope_id uuid NOT NULL,
    provider text NOT NULL,
    run_id uuid NOT NULL UNIQUE,
    run_mode text NOT NULL DEFAULT 'collect' CHECK (run_mode='collect'),
    run_status text NOT NULL DEFAULT 'accepted' CHECK (run_status='accepted'),
    manifest_uri text NOT NULL CHECK (manifest_uri ~ '^s3://[^/]+/.+'),
    manifest_sha256 ul_control.sha256 NOT NULL,
    fetched_at timestamptz NOT NULL,
    source_published_at timestamptz,
    source_extract_date date,
    validation_version text NOT NULL,
    validation_passed boolean NOT NULL CHECK (validation_passed),
    expected_parts integer NOT NULL CHECK (expected_parts > 0),
    complete_parts integer NOT NULL CHECK (complete_parts=expected_parts),
    raw_count bigint NOT NULL CHECK (raw_count >= 0),
    parsed_count bigint NOT NULL CHECK (parsed_count=raw_count),
    accepted_count bigint NOT NULL CHECK (accepted_count >= 0),
    rejected_count bigint NOT NULL CHECK (rejected_count >= 0 AND accepted_count+rejected_count=parsed_count),
    FOREIGN KEY(scope_id,provider) REFERENCES ul_control.source_scope(scope_id,provider),
    FOREIGN KEY(run_id,cycle_id,scope_id,run_mode,run_status)
        REFERENCES ul_control.ingestion_run(run_id,cycle_id,scope_id,mode,status),
    UNIQUE(snapshot_id,scope_id), UNIQUE(snapshot_id,provider)
);
ALTER TABLE ul_control.source_scope ADD CONSTRAINT baseline_scope_fk
    FOREIGN KEY(baseline_snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id);
ALTER TABLE ul_control.ingestion_run ADD CONSTRAINT replay_original_fk
    FOREIGN KEY(replay_of_snapshot_id) REFERENCES ul_control.source_snapshot;
CREATE TABLE ul_control.source_head (
    scope_id uuid PRIMARY KEY REFERENCES ul_control.source_scope,
    snapshot_id uuid NOT NULL,
    FOREIGN KEY(snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id)
);
CREATE TABLE ul_control.artifact (
    artifact_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid REFERENCES ul_control.ingestion_run,
    uri text NOT NULL UNIQUE CHECK (uri ~ '^s3://[^/]+/.+'),
    sha256 ul_control.sha256 NOT NULL,
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    compression text NOT NULL CHECK (compression IN ('none','gzip','zip')),
    media_type text NOT NULL,
    layer text NOT NULL CHECK (layer IN ('bronze','silver','evidence','manifests')),
    source_partition text,
    retained_until timestamptz
);
CREATE INDEX artifact_run ON ul_control.artifact(run_id);

CREATE TABLE ul_silver.area (
    geography_version text NOT NULL,
    area_code text NOT NULL CHECK (area_code ~ '^E01[0-9]{6}$'),
    area_name text NOT NULL,
    authority_code text NOT NULL CHECK (authority_code ~ '^E[0-9]{8}$'),
    geography_year integer NOT NULL CHECK (geography_year=2021),
    geom public.geometry(MultiPolygon,4326) NOT NULL,
    official_land_km2 numeric(16,6) NOT NULL CHECK (official_land_km2 > 0),
    source_artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    PRIMARY KEY(geography_version,area_code),
    CHECK (public.ST_IsValid(geom) AND NOT public.ST_IsEmpty(geom))
);
CREATE INDEX area_geom_gist ON ul_silver.area USING gist(geom);
CREATE TABLE ul_silver.population (
    geography_version text NOT NULL,
    area_code text NOT NULL,
    population_edition text NOT NULL,
    reference_date date NOT NULL,
    population integer NOT NULL CHECK (population >= 0),
    age_0_15 integer NOT NULL CHECK (age_0_15 >= 0),
    age_16_64 integer NOT NULL CHECK (age_16_64 >= 0),
    age_65_plus integer NOT NULL CHECK (age_65_plus >= 0),
    source_artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    PRIMARY KEY(geography_version,area_code,population_edition,reference_date),
    FOREIGN KEY(geography_version,area_code) REFERENCES ul_silver.area,
    CHECK (age_0_15+age_16_64+age_65_plus=population)
);
CREATE TABLE ul_silver.source_identity (
    identity_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider text NOT NULL CHECK (provider IN ('fsa','osm','companies-house','ons','postcodes-io')),
    source_key text NOT NULL CHECK (length(source_key) BETWEEN 1 AND 128),
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    retired_at timestamptz CHECK (retired_at >= created_at),
    CHECK (provider <> 'osm' OR source_key ~ '^(node|way|relation)/[1-9][0-9]*$'),
    CHECK (provider <> 'fsa' OR source_key ~ '^[1-9][0-9]*$'),
    CHECK (provider <> 'companies-house' OR source_key ~ '^[A-Z0-9]{8}$'),
    UNIQUE(provider,source_key,generation), UNIQUE(identity_id,provider)
);
CREATE UNIQUE INDEX one_active_identity_generation ON ul_silver.source_identity(provider,source_key)
    WHERE retired_at IS NULL;
CREATE TABLE ul_silver.observation_version (
    observation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identity_id uuid NOT NULL,
    provider text NOT NULL,
    record_hash ul_control.sha256 NOT NULL,
    normalisation_version text NOT NULL,
    first_snapshot_id uuid NOT NULL,
    last_snapshot_id uuid NOT NULL,
    first_observed_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL CHECK (last_observed_at >= first_observed_at),
    valid_to timestamptz CHECK (valid_to > first_observed_at),
    source_artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    raw_record_locator text NOT NULL CHECK (length(raw_record_locator)>0),
    name_raw text,
    name_normalised text,
    address_raw jsonb,
    address_normalised text,
    postcode_raw text,
    postcode_normalised text,
    address_state text NOT NULL CHECK (address_state IN ('published','withheld','missing')),
    location public.geography(Point,4326),
    location_quality text NOT NULL CHECK (location_quality IN ('source_point','point_on_surface','postcode_fallback','missing','invalid','withheld','unresolved')),
    primary_spatial_eligible boolean NOT NULL DEFAULT false,
    source_fields jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(source_fields)='object' AND pg_column_size(source_fields)<=65536),
    FOREIGN KEY(identity_id,provider) REFERENCES ul_silver.source_identity(identity_id,provider),
    FOREIGN KEY(first_snapshot_id,provider) REFERENCES ul_control.source_snapshot(snapshot_id,provider),
    FOREIGN KEY(last_snapshot_id,provider) REFERENCES ul_control.source_snapshot(snapshot_id,provider),
    CHECK (NOT primary_spatial_eligible OR (location IS NOT NULL AND location_quality IN ('source_point','point_on_surface'))),
    CHECK (location_quality NOT IN ('missing','invalid','withheld','unresolved') OR location IS NULL),
    CHECK (address_state <> 'withheld' OR (location IS NULL AND NOT primary_spatial_eligible AND address_normalised IS NULL AND postcode_normalised IS NULL)),
    UNIQUE(identity_id,first_snapshot_id), UNIQUE(observation_id,identity_id)
);
CREATE UNIQUE INDEX one_current_observation ON ul_silver.observation_version(identity_id) WHERE valid_to IS NULL;
CREATE INDEX observation_history ON ul_silver.observation_version(identity_id,first_observed_at DESC);
CREATE INDEX observation_location_gist ON ul_silver.observation_version USING gist(location);
-- A repeated monthly sighting updates last_snapshot/last_observed, not a new version.
CREATE FUNCTION ul_silver.changed_version_only() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE previous_hash text;
BEGIN
    PERFORM 1 FROM ul_silver.source_identity WHERE identity_id=NEW.identity_id FOR UPDATE;
    SELECT record_hash INTO previous_hash FROM ul_silver.observation_version
      WHERE identity_id=NEW.identity_id ORDER BY first_observed_at DESC LIMIT 1;
    IF previous_hash=NEW.record_hash THEN
      RAISE EXCEPTION 'unchanged observation must update its existing version' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER observation_changed_version BEFORE INSERT ON ul_silver.observation_version
    FOR EACH ROW EXECUTE FUNCTION ul_silver.changed_version_only();
CREATE TABLE ul_silver.identity_scope_state (
    scope_id uuid NOT NULL REFERENCES ul_control.source_scope,
    identity_id uuid NOT NULL REFERENCES ul_silver.source_identity,
    current_observation_id uuid NOT NULL,
    last_seen_snapshot_id uuid NOT NULL,
    last_assessed_snapshot_id uuid NOT NULL,
    consecutive_complete_absences smallint NOT NULL DEFAULT 0 CHECK (consecutive_complete_absences BETWEEN 0 AND 2),
    observation_state text NOT NULL CHECK (observation_state IN ('observed','pending_absence','no_longer_observed')),
    PRIMARY KEY(scope_id,identity_id),
    FOREIGN KEY(current_observation_id,identity_id) REFERENCES ul_silver.observation_version(observation_id,identity_id),
    FOREIGN KEY(last_seen_snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id),
    FOREIGN KEY(last_assessed_snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id),
    CHECK ((observation_state='observed' AND consecutive_complete_absences=0)
       OR (observation_state='pending_absence' AND consecutive_complete_absences=1)
       OR (observation_state='no_longer_observed' AND consecutive_complete_absences=2))
);
CREATE TABLE ul_silver.establishment (
    establishment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name text,
    address text,
    postcode text,
    eligibility text NOT NULL CHECK (eligibility IN ('eligible','excluded','unresolved')),
    primary_category text CHECK (primary_category IN ('cafe','restaurant','takeaway','bakery','pub_bar','food_service')),
    secondary_categories text[] NOT NULL DEFAULT '{}',
    taxonomy_version text NOT NULL,
    identity_rule_version text NOT NULL,
    location public.geography(Point,4326),
    location_quality text NOT NULL CHECK (location_quality IN ('source_point','point_on_surface','postcode_fallback','missing','withheld','unresolved')),
    primary_spatial_eligible boolean NOT NULL DEFAULT false,
    geography_version text,
    area_code text,
    observation_state text NOT NULL CHECK (observation_state IN ('observed','pending_absence','no_longer_observed')),
    first_observed_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL CHECK (last_observed_at >= first_observed_at),
    FOREIGN KEY(geography_version,area_code) REFERENCES ul_silver.area MATCH FULL,
    CHECK (eligibility <> 'eligible' OR primary_category IS NOT NULL),
    CHECK (NOT primary_spatial_eligible OR (location IS NOT NULL AND location_quality IN ('source_point','point_on_surface'))),
    CHECK (location_quality NOT IN ('missing','withheld','unresolved') OR location IS NULL)
);
CREATE INDEX establishment_location_gist ON ul_silver.establishment USING gist(location);
CREATE TABLE ul_silver.company (
    company_number text PRIMARY KEY CHECK (company_number ~ '^[A-Z0-9]{8}$'),
    legal_name text NOT NULL,
    legal_status text NOT NULL,
    company_type text NOT NULL,
    sic_codes text[] NOT NULL DEFAULT '{}',
    incorporation_date date,
    cessation_date date,
    registered_office jsonb,
    registered_office_postcode text,
    registered_office_location public.geography(Point,4326),
    registered_office_cohort boolean NOT NULL DEFAULT false,
    first_observed_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL CHECK (last_observed_at>=first_observed_at),
    profile_observed_at timestamptz,
    source_observation_id uuid REFERENCES ul_silver.observation_version,
    CHECK (array_position(sic_codes,NULL) IS NULL AND array_to_string(sic_codes,',') ~ '^([0-9]{5}(,[0-9]{5})*)?$')
);
CREATE TABLE ul_control.review (
    review_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    review_type text NOT NULL CHECK (review_type IN ('source_baseline','anomaly','identity','company_link','eligibility','rule_change')),
    source_run_id uuid REFERENCES ul_control.ingestion_run,
    source_identity_id uuid REFERENCES ul_silver.source_identity,
    establishment_id uuid REFERENCES ul_silver.establishment,
    alternative_establishment_id uuid REFERENCES ul_silver.establishment,
    company_number text REFERENCES ul_silver.company,
    decision text NOT NULL DEFAULT 'pending' CHECK (decision IN ('pending','approved','rejected','superseded')),
    reason text NOT NULL CHECK (length(reason)>0),
    evidence_uri text NOT NULL CHECK (evidence_uri ~ '^(https://|s3://).+'),
    evidence_sha256 ul_control.sha256,
    rule_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    reviewer text,
    reviewed_at timestamptz,
    CHECK ((decision='pending' AND reviewer IS NULL AND reviewed_at IS NULL)
        OR (decision<>'pending' AND length(reviewer)>0 AND reviewer IS NOT NULL AND reviewed_at IS NOT NULL)),
    CHECK (review_type<>'company_link' OR (company_number IS NOT NULL AND establishment_id IS NOT NULL)),
    UNIQUE(review_id,company_number,establishment_id,review_type,decision)
);
CREATE INDEX review_pending ON ul_control.review(created_at) WHERE decision='pending';
CREATE TABLE ul_silver.establishment_source_link (
    link_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    establishment_id uuid NOT NULL REFERENCES ul_silver.establishment,
    identity_id uuid NOT NULL REFERENCES ul_silver.source_identity,
    method text NOT NULL,
    evidence_uri text NOT NULL CHECK (evidence_uri ~ '^(https://|s3://).+'),
    rule_version text NOT NULL,
    review_id uuid REFERENCES ul_control.review,
    decision text NOT NULL CHECK (decision IN ('accepted','rejected','pending')),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz CHECK (valid_to>valid_from)
);
CREATE UNIQUE INDEX one_current_source_link ON ul_silver.establishment_source_link(identity_id)
    WHERE valid_to IS NULL AND decision='accepted';
CREATE INDEX source_link_establishment ON ul_silver.establishment_source_link(establishment_id);
CREATE TABLE ul_silver.company_establishment_link (
    link_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    company_number text NOT NULL REFERENCES ul_silver.company,
    establishment_id uuid NOT NULL REFERENCES ul_silver.establishment,
    review_id uuid NOT NULL,
    review_type text NOT NULL DEFAULT 'company_link' CHECK (review_type='company_link'),
    decision text NOT NULL DEFAULT 'approved' CHECK (decision='approved'),
    relationship text NOT NULL CHECK (relationship IN ('operator','franchisee','other_verified')),
    evidence_uri text NOT NULL CHECK (evidence_uri ~ '^(https://|s3://).+'),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz CHECK (valid_to>valid_from),
    FOREIGN KEY(review_id,company_number,establishment_id,review_type,decision)
      REFERENCES ul_control.review(review_id,company_number,establishment_id,review_type,decision),
    UNIQUE(link_id,company_number,establishment_id)
);
CREATE UNIQUE INDEX one_current_company_relationship ON ul_silver.company_establishment_link(company_number,establishment_id,relationship)
    WHERE valid_to IS NULL;
CREATE TABLE ul_silver.field_provenance (
    provenance_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    establishment_id uuid NOT NULL REFERENCES ul_silver.establishment,
    field_name text NOT NULL,
    observation_id uuid REFERENCES ul_silver.observation_version,
    review_id uuid REFERENCES ul_control.review,
    rule_version text NOT NULL,
    selection_reason text NOT NULL,
    source_time timestamptz,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz CHECK (valid_to>valid_from),
    CHECK (observation_id IS NOT NULL OR review_id IS NOT NULL)
);
CREATE UNIQUE INDEX one_current_field_provenance ON ul_silver.field_provenance(establishment_id,field_name)
    WHERE valid_to IS NULL;
CREATE TABLE ul_silver.change_event (
    event_id uuid PRIMARY KEY,
    event_key ul_control.sha256 NOT NULL UNIQUE,
    entity_type text NOT NULL CHECK (entity_type IN ('establishment','source','company')),
    establishment_id uuid REFERENCES ul_silver.establishment,
    identity_id uuid REFERENCES ul_silver.source_identity,
    company_number text REFERENCES ul_silver.company,
    event_type text NOT NULL CHECK (event_type IN ('newly_observed','no_longer_observed','reappeared','source_changed','company_incorporated','company_dissolved','processing_correction')),
    snapshot_id uuid REFERENCES ul_control.source_snapshot,
    observed_at timestamptz NOT NULL,
    source_event_date date,
    before_value jsonb,
    after_value jsonb,
    evidence_fields jsonb NOT NULL CHECK (jsonb_typeof(evidence_fields)='object' AND pg_column_size(evidence_fields)<=32768),
    retained_evidence_uri text CHECK (retained_evidence_uri ~ '^s3://[^/]+/evidence/.+'),
    rule_version text NOT NULL,
    CHECK (num_nonnulls(establishment_id,identity_id,company_number)=1),
    CHECK ((entity_type='establishment' AND establishment_id IS NOT NULL)
        OR (entity_type='source' AND identity_id IS NOT NULL)
        OR (entity_type='company' AND company_number IS NOT NULL))
);
CREATE INDEX event_observed_time ON ul_silver.change_event(observed_at DESC,event_id);
CREATE TABLE ul_silver.event_observation_evidence (
    event_id uuid NOT NULL REFERENCES ul_silver.change_event ON DELETE CASCADE,
    observation_id uuid NOT NULL REFERENCES ul_silver.observation_version,
    PRIMARY KEY(event_id,observation_id)
);

CREATE TABLE ul_control.data_release (
    release_id uuid PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('building','validated','published','failed','retired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    quality_passed boolean NOT NULL DEFAULT false,
    geography_version text NOT NULL,
    population_edition text NOT NULL,
    population_reference_date date NOT NULL,
    taxonomy_version text NOT NULL,
    pipeline_version text NOT NULL,
    manifest_uri text NOT NULL CHECK (manifest_uri ~ '^s3://[^/]+/.+'),
    manifest_sha256 ul_control.sha256 NOT NULL,
    CHECK (status<>'published' OR (quality_passed AND published_at IS NOT NULL))
);
CREATE TABLE ul_control.release_source (
    release_id uuid NOT NULL REFERENCES ul_control.data_release,
    scope_id uuid NOT NULL,
    snapshot_id uuid NOT NULL,
    PRIMARY KEY(release_id,scope_id),
    FOREIGN KEY(snapshot_id,scope_id) REFERENCES ul_control.source_snapshot(snapshot_id,scope_id)
);
-- At most two published model sets plus one transient build; old metadata remains.
CREATE TABLE ul_control.release_slot (
    slot text PRIMARY KEY CHECK (slot IN ('active','previous','building')),
    release_id uuid NOT NULL UNIQUE REFERENCES ul_control.data_release
);
CREATE TABLE ul_gold.establishment (
    release_id uuid NOT NULL REFERENCES ul_control.release_slot(release_id) ON DELETE CASCADE,
    establishment_id uuid NOT NULL REFERENCES ul_silver.establishment,
    display_name text NOT NULL CHECK (length(display_name)>0),
    primary_category text NOT NULL CHECK (primary_category IN ('cafe','restaurant','takeaway','bakery','pub_bar','food_service')),
    address text,
    postcode text,
    location public.geography(Point,4326),
    location_quality text NOT NULL CHECK (location_quality IN ('source_point','point_on_surface','postcode_fallback','missing','withheld','unresolved')),
    primary_spatial_eligible boolean NOT NULL,
    observation_state text NOT NULL CHECK (observation_state IN ('observed','pending_absence','no_longer_observed')),
    area_code text,
    PRIMARY KEY(release_id,establishment_id),
    CHECK (NOT primary_spatial_eligible OR (location IS NOT NULL AND location_quality IN ('source_point','point_on_surface'))),
    CHECK (location_quality NOT IN ('missing','withheld','unresolved') OR location IS NULL)
);
CREATE INDEX gold_establishment_location_gist ON ul_gold.establishment USING gist(location);
CREATE INDEX gold_establishment_category ON ul_gold.establishment(release_id,primary_category);
CREATE TABLE ul_gold.company (
    release_id uuid NOT NULL REFERENCES ul_control.release_slot(release_id) ON DELETE CASCADE,
    company_number text NOT NULL REFERENCES ul_silver.company,
    legal_name text NOT NULL,
    legal_status text NOT NULL,
    company_type text NOT NULL,
    sic_codes text[] NOT NULL,
    incorporation_date date,
    cessation_date date,
    registered_office_postcode text,
    registered_office_cohort boolean NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY(release_id,company_number)
);
CREATE TABLE ul_gold.company_establishment (
    release_id uuid NOT NULL,
    company_number text NOT NULL,
    establishment_id uuid NOT NULL,
    link_id uuid NOT NULL,
    PRIMARY KEY(release_id,company_number,establishment_id),
    FOREIGN KEY(release_id,company_number) REFERENCES ul_gold.company ON DELETE CASCADE,
    FOREIGN KEY(release_id,establishment_id) REFERENCES ul_gold.establishment ON DELETE CASCADE,
    FOREIGN KEY(link_id,company_number,establishment_id) REFERENCES ul_silver.company_establishment_link(link_id,company_number,establishment_id)
);
CREATE TABLE ul_gold.activity (
    release_id uuid NOT NULL REFERENCES ul_control.release_slot(release_id) ON DELETE CASCADE,
    event_id uuid NOT NULL REFERENCES ul_silver.change_event,
    entity_type text NOT NULL CHECK (entity_type IN ('establishment','source','company')),
    event_type text NOT NULL,
    observed_at timestamptz NOT NULL,
    source_event_date date,
    summary text NOT NULL,
    PRIMARY KEY(release_id,event_id)
);
CREATE INDEX gold_activity_time ON ul_gold.activity(release_id,observed_at DESC,event_id);
CREATE TABLE ul_gold.area_metric (
    release_id uuid NOT NULL REFERENCES ul_control.release_slot(release_id) ON DELETE CASCADE,
    area_code text NOT NULL,
    category text NOT NULL,
    establishment_count integer NOT NULL CHECK (establishment_count>=0),
    population integer NOT NULL CHECK (population>=0),
    official_land_km2 numeric(16,6) NOT NULL CHECK (official_land_km2>0),
    PRIMARY KEY(release_id,area_code,category)
);
CREATE TABLE ul_gold.source_health (
    release_id uuid NOT NULL REFERENCES ul_control.release_slot(release_id) ON DELETE CASCADE,
    provider text NOT NULL,
    scope_key text NOT NULL,
    status text NOT NULL CHECK (status IN ('healthy','held','failed','stale')),
    last_success_at timestamptz,
    last_attempt_at timestamptz,
    next_due_at timestamptz,
    accepted_count bigint NOT NULL CHECK (accepted_count>=0),
    PRIMARY KEY(release_id,provider,scope_key)
);
