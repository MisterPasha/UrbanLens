-- Security barriers prevent caller predicates exposing unpublished model rows.
CREATE VIEW ul_api.release WITH (security_barrier=true) AS
SELECT r.release_id,s.slot,r.published_at,r.geography_version,r.population_edition,
       r.population_reference_date,r.taxonomy_version,r.pipeline_version
FROM ul_control.release_slot s JOIN ul_control.data_release r USING(release_id)
WHERE s.slot IN ('active','previous') AND r.status='published' AND r.quality_passed;

CREATE VIEW ul_api.establishment WITH (security_barrier=true) AS
SELECT g.release_id,g.establishment_id,g.display_name,g.primary_category,g.address,g.postcode,
       g.location,g.location_quality,g.primary_spatial_eligible,g.observation_state,g.area_code
FROM ul_gold.establishment g JOIN ul_api.release r USING(release_id);
CREATE VIEW ul_api.company WITH (security_barrier=true) AS
SELECT g.release_id,g.company_number,g.legal_name,g.legal_status,g.company_type,g.sic_codes,
       g.incorporation_date,g.cessation_date,g.registered_office_postcode,
       g.registered_office_cohort,g.observed_at
FROM ul_gold.company g JOIN ul_api.release r USING(release_id);
CREATE VIEW ul_api.company_establishment WITH (security_barrier=true) AS
SELECT g.release_id,g.company_number,g.establishment_id
FROM ul_gold.company_establishment g JOIN ul_api.release r USING(release_id);
CREATE VIEW ul_api.activity WITH (security_barrier=true) AS
SELECT g.release_id,g.event_id,g.entity_type,g.event_type,g.observed_at,g.source_event_date,g.summary
FROM ul_gold.activity g JOIN ul_api.release r USING(release_id);
CREATE VIEW ul_api.area_metric WITH (security_barrier=true) AS
SELECT g.release_id,g.area_code,g.category,g.establishment_count,g.population,g.official_land_km2
FROM ul_gold.area_metric g JOIN ul_api.release r USING(release_id);
CREATE VIEW ul_api.source_health WITH (security_barrier=true) AS
SELECT g.release_id,g.provider,g.scope_key,g.status,g.last_success_at,g.last_attempt_at,
       g.next_due_at,g.accepted_count
FROM ul_gold.source_health g JOIN ul_api.release r USING(release_id);

-- Keep the spatial predicate inside the trusted function so its native geography
-- GiST index is usable without pushing non-leakproof predicates through a barrier.
CREATE FUNCTION ul_api.establishments_within(
    p_release_id uuid,p_longitude double precision,p_latitude double precision,p_radius integer
) RETURNS TABLE(release_id uuid,establishment_id uuid,display_name text,
    primary_category text,location public.geography,location_quality text)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
    IF p_release_id IS NULL OR p_longitude IS NULL OR p_latitude IS NULL OR p_radius IS NULL
       OR NOT(p_longitude BETWEEN -180 AND 180) OR NOT(p_latitude BETWEEN -90 AND 90)
       OR p_radius NOT IN (250,500,1000,2000) THEN
        RAISE EXCEPTION 'invalid release, coordinate or supported radius' USING ERRCODE='22023';
    END IF;
    RETURN QUERY
    SELECT g.release_id,g.establishment_id,g.display_name,g.primary_category,g.location,g.location_quality
    FROM ul_gold.establishment g
    JOIN ul_control.release_slot s ON s.release_id=g.release_id
    JOIN ul_control.data_release r ON r.release_id=g.release_id
    WHERE g.release_id=p_release_id AND s.slot IN ('active','previous')
      AND r.status='published' AND r.quality_passed AND g.primary_spatial_eligible
      AND g.observation_state IN ('observed','pending_absence')
      AND public.ST_DWithin(g.location,
          public.ST_SetSRID(public.ST_MakePoint(p_longitude,p_latitude),4326)::public.geography,
          p_radius+0.000001);
END $$;
