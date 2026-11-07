ALTER TABLE ul_control.data_release
 ADD COLUMN model_schema name UNIQUE,
 ADD COLUMN input_manifest jsonb NOT NULL DEFAULT '{}',
 ADD COLUMN quality_results jsonb NOT NULL DEFAULT '{}',
 ADD COLUMN validated_contents jsonb NOT NULL DEFAULT '{}',
 ADD COLUMN retired_at timestamptz,
 ADD COLUMN cleaned_at timestamptz;
ALTER TABLE ul_control.data_release ADD CHECK(model_schema IS NULL OR
 model_schema::text='ul_candidate_'||replace(release_id::text,'-',''));
CREATE TABLE ul_control.release_transition (
 transition_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 from_release_id uuid REFERENCES ul_control.data_release,
 to_release_id uuid NOT NULL REFERENCES ul_control.data_release,
 action text NOT NULL CHECK(action IN ('publish','rollback')),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Only the allowlisted operations below can read a registered, retained model set.
-- A shared transaction lock prevents cleanup while a request uses its pinned release.
CREATE FUNCTION ul_api.read_release(p_release uuid,p_operation text,p_filters jsonb DEFAULT '{}')
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE r ul_control.data_release; selected uuid; result jsonb; sources jsonb; coverage jsonb;
 schema_name text; categories text[]; areas text[]; lon float8; lat float8; radius integer;
 bbox float8[]; page_size integer; map_limit integer; after_id uuid; detail_id uuid;
 bbox_intersects boolean; bbox_contained boolean;
BEGIN
 IF p_operation NOT IN ('meta','areas','establishments','detail','postcode','postcode_check') THEN
   RAISE EXCEPTION 'Unsupported query' USING ERRCODE='22023';
 END IF;
 selected:=p_release;
 IF selected IS NULL THEN SELECT release_id INTO selected FROM ul_control.release_slot WHERE slot='active'; END IF;
 IF selected IS NULL THEN RAISE EXCEPTION 'No published release' USING ERRCODE='UL503'; END IF;
 PERFORM pg_advisory_xact_lock_shared(192033,hashtext(selected::text));
 SELECT d.* INTO r FROM ul_control.data_release d JOIN ul_control.release_slot s USING(release_id)
 WHERE d.release_id=selected AND s.slot IN ('active','previous')
   AND d.status='published' AND d.quality_passed AND d.cleaned_at IS NULL AND d.model_schema IS NOT NULL;
 IF NOT FOUND THEN RAISE EXCEPTION 'Release expired; refresh metadata' USING ERRCODE='UL410'; END IF;
 schema_name:=r.model_schema::text;
 EXECUTE format('SELECT coalesce(jsonb_agg((to_jsonb(s)-ARRAY[''release_id'',''current_release_published_at''])||jsonb_build_object(''status'',
   CASE WHEN now()>next_due_at+interval ''48 hours'' THEN ''stale'' ELSE status END)
   ORDER BY provider,scope_key),''[]'') FROM %I.mart_source_health s',schema_name) INTO sources;
 EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(c)-''release_id'' ORDER BY universe,dimension,bucket),''[]'')
   FROM %I.mart_coverage c',schema_name) INTO coverage;

 IF p_operation='meta' THEN
   result:=jsonb_build_object('categories',ARRAY['cafe','restaurant','takeaway','bakery','pub_bar','food_service'],
     'radii',ARRAY[250,500,1000,2000],'default_radius',500,'city','Brighton & Hove','industry','Hospitality');
 ELSIF p_operation='areas' THEN
   EXECUTE format('SELECT coalesce(jsonb_agg(jsonb_build_object(''area_code'',area_code,''area_name'',area_name,
     ''geography_version'',geography_version,''display_geometry'',public.ST_AsGeoJSON(
     public.ST_SimplifyPreserveTopology(geom,0.0001))::jsonb) ORDER BY area_code),''[]'')
     FROM %I.dim_area',schema_name) INTO result;
 ELSIF p_operation IN ('postcode','postcode_check') THEN
   IF p_operation='postcode' THEN
     SELECT to_jsonb(c)-ARRAY['source_artifact_id','silver_artifact_id','record_hash'] INTO result
       FROM ul_silver.postcode_cache c WHERE replace(c.postcode,' ','')=p_filters->>'postcode'
       AND c.expires_at>now();
   ELSE result:=p_filters; END IF;
   IF result IS NOT NULL AND result->>'longitude' IS NOT NULL AND result->>'latitude' IS NOT NULL THEN
     EXECUTE format('SELECT area_code FROM %I.dim_area WHERE public.ST_Covers(geom,
       public.ST_SetSRID(public.ST_Point($1,$2),4326)) ORDER BY area_code LIMIT 1',schema_name)
       INTO schema_name USING (result->>'longitude')::float8,(result->>'latitude')::float8;
     result:=result||jsonb_build_object('supported_area_code',schema_name);
   END IF;
 ELSIF p_operation='detail' THEN
   detail_id:=(p_filters->>'establishment_id')::uuid;
   EXECUTE format($query$
     WITH venues AS (SELECT * FROM %1$I.mart_establishment UNION ALL SELECT * FROM %1$I.mart_venue_context)
     SELECT jsonb_build_object('establishment_id',establishment_id,'display_name',display_name,
       'name_unavailable',name_unavailable,'address',CASE WHEN location_quality='withheld' THEN null ELSE address END,
       'postcode',CASE WHEN location_quality='withheld' THEN null ELSE postcode END,
       'primary_category',primary_category,'secondary_categories',secondary_categories,
       'eligibility',eligibility,'duplicate_status',duplicate_status,'observation_state',observation_state,
       'primary_spatial_eligible',primary_spatial_eligible,'location_quality',location_quality,
       'longitude',CASE WHEN location_quality='withheld' THEN null ELSE public.ST_X(location::public.geometry) END,
       'latitude',CASE WHEN location_quality='withheld' THEN null ELSE public.ST_Y(location::public.geometry) END,
       'area_code',CASE WHEN location_quality='withheld' THEN null ELSE area_code END,
       'first_observed_at',first_observed_at,'last_observed_at',last_observed_at,
       'source_mix',source_mix,'exclusion_reasons',exclusion_reasons,'hygiene',hygiene,
       'null_reasons',jsonb_build_object('name',CASE WHEN name_unavailable THEN 'source_name_unavailable' END,
         'location',CASE WHEN location IS NULL THEN location_quality END,
         'address',CASE WHEN address IS NULL THEN CASE WHEN location_quality='withheld' THEN 'withheld' ELSE 'unavailable' END END),
       'sources',(SELECT coalesce(jsonb_agg(jsonb_build_object('provider',s->>'provider',
          'source_key',s->>'key','observed_at',s->'observed_at','state',s->'state',
          'url',CASE s->>'provider' WHEN 'fsa' THEN 'https://ratings.food.gov.uk/business/'||(s->>'key')
             WHEN 'osm' THEN 'https://www.openstreetmap.org/'||(s->>'key') END)), '[]') FROM jsonb_array_elements(sources) s),
       'provenance',(SELECT coalesce(jsonb_object_agg(k,jsonb_build_object(
          'rule_version',v->'rule_version','source_time',v->'source_time',
          'reason',CASE WHEN location_quality='withheld' AND k IN ('address','postcode','location','area_code')
             THEN '"Source withheld"'::jsonb ELSE v->'reason' END,
          'reviewed',v->>'review_id' IS NOT NULL,
          'value',CASE WHEN location_quality='withheld' AND k IN ('address','postcode','location','area_code')
            THEN 'null'::jsonb ELSE v->'detail'->'value' END)), '{}') FROM jsonb_each(provenance) p(k,v)),
       'verified_companies',(SELECT coalesce(jsonb_agg(jsonb_build_object('company_number',v->>'company_number',
          'relationship',v->>'relationship','reviewed_at',v->'reviewed_at','reviewed_on',v->'reviewed_on',
          'url','https://find-and-update.company-information.service.gov.uk/company/'||(v->>'company_number'))),'[]')
          FROM jsonb_array_elements(verified_operators) v))
     FROM venues WHERE establishment_id=$1 AND eligibility<>'excluded'
   $query$,schema_name) INTO result USING detail_id;
   IF result IS NULL THEN RAISE EXCEPTION 'Venue not found' USING ERRCODE='UL404'; END IF;
 ELSE
   IF p_filters ? 'categories' AND jsonb_typeof(p_filters->'categories')='array' THEN
     SELECT array_agg(v) INTO categories FROM jsonb_array_elements_text(p_filters->'categories') v;
     categories:=coalesce(categories,'{}');
   END IF;
   IF p_filters->>'mode'='area' THEN areas:=ARRAY[p_filters->>'area_code'];
   ELSIF p_filters->>'mode'='radius' THEN
     lon:=(p_filters->>'longitude')::float8;lat:=(p_filters->>'latitude')::float8;
     radius:=coalesce((p_filters->>'radius')::integer,500);
   ELSIF p_filters->>'mode' NOT IN ('city','bbox') THEN
     RAISE EXCEPTION 'Invalid mode' USING ERRCODE='22023'; END IF;
   IF p_filters->>'mode'='bbox' THEN
     SELECT array_agg(v::float8) INTO bbox FROM jsonb_array_elements_text(p_filters->'bbox') v;
     IF cardinality(bbox)<>4 OR NOT(bbox[1]<bbox[3] AND bbox[2]<bbox[4])
       OR bbox[3]-bbox[1]>0.1 OR bbox[4]-bbox[2]>0.1
       OR NOT(bbox[1]>=-180 AND bbox[3]<=180 AND bbox[2]>=-90 AND bbox[4]<=90) THEN
       RAISE EXCEPTION 'Invalid viewport' USING ERRCODE='22023'; END IF;
     EXECUTE format('SELECT bool_or(public.ST_Intersects(geom,public.ST_MakeEnvelope($1,$2,$3,$4,4326))),
       public.ST_Covers(public.ST_UnaryUnion(public.ST_Collect(geom)),public.ST_MakeEnvelope($1,$2,$3,$4,4326))
       FROM %I.dim_area',schema_name) INTO bbox_intersects,bbox_contained
       USING bbox[1],bbox[2],bbox[3],bbox[4];
     IF NOT coalesce(bbox_intersects,false) THEN
       RAISE EXCEPTION 'Viewport outside supported coverage' USING ERRCODE='22023'; END IF;
   END IF;
   page_size:=coalesce((p_filters->>'limit')::integer,50);
   map_limit:=coalesce((p_filters->>'map_limit')::integer,500);
   after_id:=(p_filters->>'after_id')::uuid;
   IF page_size NOT BETWEEN 1 AND 100 OR map_limit NOT BETWEEN 1 AND 1000 THEN
     RAISE EXCEPTION 'Invalid page limits' USING ERRCODE='22023'; END IF;
   EXECUTE format($query$
     WITH selected AS MATERIALIZED (
       SELECT * FROM %1$I.selected_venues($1,$2,$3,$4,$5) v
       WHERE $6 IS NULL OR (v.location && public.ST_MakeEnvelope($6[1],$6[2],$6[3],$6[4],4326)::public.geography
          AND public.ST_Covers(public.ST_MakeEnvelope($6[1],$6[2],$6[3],$6[4],4326),v.location::public.geometry))
     ), summary AS (
       SELECT establishment_id,display_name,name_unavailable,primary_category,secondary_categories,address,postcode,
         location_quality,area_code,observation_state,last_observed_at,source_mix,
         public.ST_X(location::public.geometry) longitude,public.ST_Y(location::public.geometry) latitude
       FROM selected
     ), page AS (SELECT * FROM summary WHERE $7 IS NULL OR establishment_id>$7 ORDER BY establishment_id LIMIT $8),
     markers AS (SELECT establishment_id,primary_category,longitude,latitude FROM summary ORDER BY establishment_id LIMIT $9),
     mix AS (SELECT primary_category,count(*) n FROM selected GROUP BY 1)
     SELECT jsonb_build_object('total',(SELECT count(*) FROM selected),
       'items',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY establishment_id),'[]') FROM page p),
       'has_more',(SELECT count(*)>$8 FROM selected WHERE $7 IS NULL OR establishment_id>$7),
       'map_points',(SELECT coalesce(jsonb_agg(to_jsonb(m) ORDER BY establishment_id),'[]') FROM markers m),
       'map_truncated',(SELECT count(*)>$9 FROM selected),
       'category_mix',(SELECT coalesce(jsonb_object_agg(primary_category,n),'{}') FROM mix),
       'partial_coverage',CASE WHEN $5 IS NULL THEN false ELSE (SELECT partial_coverage FROM %1$I.radius_summary($3,$4,$5,$1)) END,
       'unknown_location_scope','city-wide; membership in this search is unknown')
   $query$,schema_name) INTO result USING categories,areas,lon,lat,radius,bbox,after_id,page_size,map_limit;
   IF bbox IS NOT NULL THEN result:=result||jsonb_build_object('partial_coverage',NOT bbox_contained); END IF;
 END IF;
 RETURN jsonb_build_object('api_version','v1','release_id',selected,'applied_filters',p_filters,
   'metadata',jsonb_build_object('published_at',r.published_at,'geography_version',r.geography_version,
     'population_edition',r.population_edition,'population_reference_date',r.population_reference_date,
     'taxonomy_version',r.taxonomy_version,'sources',sources,
     'attribution',ARRAY['Food Standards Agency: Open Government Licence','OpenStreetMap contributors: ODbL',
       'ONS: Open Government Licence','Companies House: public company data','Postcodes.io: underlying dataset licences']),
   'coverage',coverage,'null_reasons','{}'::jsonb,'data',result);
END $$;
REVOKE ALL ON FUNCTION ul_api.read_release(uuid,text,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ul_api.read_release(uuid,text,jsonb) TO urbanlens_api;
