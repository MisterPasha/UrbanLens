-- Private, source-level spatial evidence. Canonical counting still requires step 23.
CREATE TABLE ul_silver.observation_location (
    observation_id uuid PRIMARY KEY,
    record_hash ul_control.sha256 NOT NULL,
    engine_version text NOT NULL,
    decision_hash ul_control.sha256 NOT NULL,
    quality text NOT NULL CHECK(quality IN ('source','reviewed','approximate','missing','invalid','unresolved','withheld')),
    location public.geography(Point,4326),
    geography_version text,
    area_code text,
    covering_area_codes text[] NOT NULL,
    spatial_usable boolean NOT NULL,
    eligible_spatial_candidate boolean NOT NULL,
    review_required boolean NOT NULL,
    exclusion_scope text NOT NULL CHECK(exclusion_scope IN ('located','city_wide_unknown_membership','outside_supported_coverage')),
    decision jsonb NOT NULL CHECK(pg_column_size(decision)<=65536),
    artifact_id uuid NOT NULL REFERENCES ul_control.artifact,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(observation_id,record_hash) REFERENCES ul_silver.observation_version(observation_id,record_hash) ON DELETE CASCADE,
    FOREIGN KEY(geography_version,area_code) REFERENCES ul_silver.area,
    CHECK((geography_version IS NULL)=(area_code IS NULL)),
    CHECK(NOT spatial_usable OR (quality IN ('source','reviewed') AND location IS NOT NULL AND area_code IS NOT NULL)),
    CHECK(NOT eligible_spatial_candidate OR spatial_usable),
    CHECK(quality NOT IN ('missing','invalid','unresolved','withheld') OR location IS NULL),
    CHECK(quality<>'approximate' OR (area_code IS NULL AND NOT spatial_usable))
);
CREATE INDEX assessed_location_gist ON ul_silver.observation_location USING gist(location);

-- Covers includes shared/outer boundaries; the caller chooses the first sorted code.
CREATE FUNCTION ul_silver.covering_areas(lon double precision,lat double precision)
RETURNS text[] LANGUAGE sql STABLE STRICT SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
 SELECT COALESCE(array_agg(area_code ORDER BY area_code),'{}'::text[])
 FROM ul_silver.area
 WHERE geography_version='LSOA-2021' AND authority_code='E06000043'
 AND public.ST_Covers(geom,public.ST_SetSRID(public.ST_Point(lon,lat),4326))
$$;

CREATE FUNCTION ul_silver.search_coverage(lon double precision,lat double precision,radius integer)
RETURNS TABLE(supported boolean,partial_coverage boolean)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE city public.geometry; centre public.geometry;
BEGIN
 IF lon IS NULL OR lat IS NULL OR radius IS NULL OR
    NOT(lon BETWEEN -180 AND 180 AND lat BETWEEN -90 AND 90) OR
    radius NOT IN (250,500,1000,2000) THEN
   RAISE EXCEPTION 'invalid centre or radius' USING ERRCODE='22023';
 END IF;
 SELECT public.ST_UnaryUnion(public.ST_Collect(geom)) INTO city FROM ul_silver.area
 WHERE geography_version='LSOA-2021' AND authority_code='E06000043';
 IF city IS NULL THEN RAISE EXCEPTION 'coverage geometry unavailable'; END IF;
 centre:=public.ST_SetSRID(public.ST_Point(lon,lat),4326);
 supported:=public.ST_Covers(city,centre);
 -- A circle touches uncovered space iff the nearest boundary is closer than R.
 -- Geography distance is spheroidal metres, including coastline and interior holes.
 partial_coverage:=supported AND public.ST_Distance(
     centre::public.geography,public.ST_Boundary(city)::public.geography)<radius;
 RETURN NEXT;
END $$;
REVOKE ALL ON FUNCTION ul_silver.covering_areas(double precision,double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION ul_silver.search_coverage(double precision,double precision,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ul_silver.covering_areas(double precision,double precision) TO urbanlens_loader,urbanlens_dbt;
GRANT EXECUTE ON FUNCTION ul_silver.search_coverage(double precision,double precision,integer) TO urbanlens_loader,urbanlens_dbt;
