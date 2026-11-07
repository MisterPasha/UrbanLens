-- One bounded current classification per retained source observation. Full
-- versioned decisions/rules/reviews remain immutable in S3. No canonical counts.
ALTER TABLE ul_silver.observation_version ADD UNIQUE(observation_id,record_hash);
CREATE TABLE ul_control.classification_definition (
    kind text NOT NULL CHECK(kind IN ('taxonomy','reviews')),
    version text NOT NULL,
    sha256 ul_control.sha256 NOT NULL,
    artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    PRIMARY KEY(kind,version)
);
GRANT INSERT ON ul_control.classification_definition TO urbanlens_loader;
CREATE TABLE ul_silver.observation_classification (
    observation_id uuid PRIMARY KEY REFERENCES ul_silver.observation_version ON DELETE CASCADE,
    record_hash ul_control.sha256 NOT NULL,
    taxonomy_version text NOT NULL,
    taxonomy_sha256 ul_control.sha256 NOT NULL,
    reviews_version text NOT NULL,
    reviews_sha256 ul_control.sha256 NOT NULL,
    engine_version text NOT NULL,
    decision_hash ul_control.sha256 NOT NULL,
    applicable boolean NOT NULL,
    eligibility text NOT NULL CHECK(eligibility IN ('eligible','excluded','unresolved')),
    primary_category text CHECK(primary_category IN ('cafe','restaurant','takeaway','bakery','pub_bar','food_service')),
    category_state text NOT NULL CHECK(category_state IN ('specific','broad','ambiguous','unresolved','not_applicable')),
    candidate_categories text[] NOT NULL DEFAULT '{}',
    review_required boolean NOT NULL,
    review_status text NOT NULL CHECK(review_status IN ('none','applied','stale')),
    decision jsonb NOT NULL CHECK(jsonb_typeof(decision)='object' AND pg_column_size(decision)<=65536),
    artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(observation_id,record_hash) REFERENCES ul_silver.observation_version(observation_id,record_hash) ON DELETE CASCADE,
    CHECK((eligibility='eligible')=(primary_category IS NOT NULL)),
    CHECK(applicable OR eligibility='excluded'),
    CHECK(eligibility<>'unresolved' OR review_required),
    CHECK(primary_category IS NULL OR (primary_category='food_service' AND category_state IN ('broad','ambiguous')) OR (primary_category<>'food_service' AND category_state='specific')),
    CHECK(array_position(candidate_categories,NULL) IS NULL AND candidate_categories <@ ARRAY['cafe','restaurant','takeaway','bakery','pub_bar','food_service']::text[])
);
CREATE INDEX classification_eligibility ON ul_silver.observation_classification(eligibility,primary_category);
