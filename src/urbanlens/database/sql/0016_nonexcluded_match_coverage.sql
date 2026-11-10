-- Keep excluded businesses in audit history, but outside hospitality match-review counts.
-- Derive from the pinned frozen roster, never from live Silver or draft reviews.
ALTER FUNCTION ul_api.read_release(uuid,text,jsonb) RENAME TO read_release_all_coverage;
REVOKE ALL ON FUNCTION ul_api.read_release_all_coverage(uuid,text,jsonb) FROM PUBLIC,urbanlens_api;

CREATE FUNCTION ul_api.read_release(p_release uuid,p_operation text,p_filters jsonb DEFAULT '{}')
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE result jsonb; coverage jsonb; matches jsonb; schema_name text;
BEGIN
 result:=ul_api.read_release_all_coverage(p_release,p_operation,p_filters);
 SELECT model_schema::text INTO schema_name FROM ul_control.data_release
 WHERE release_id=(result->>'release_id')::uuid;
 SELECT coalesce(jsonb_agg(row),'[]') INTO coverage
 FROM jsonb_array_elements(result->'coverage') row
 WHERE NOT(row->>'dimension'='duplicates' AND row->>'universe' IN
   ('current canonical roster','non-excluded venue records'))
 AND NOT(row->>'dimension'='exclusion_reason' AND row->>'bucket'='duplicate_unresolved');
 EXECUTE format($q$
  WITH roster AS (
   SELECT eligibility,duplicate_status FROM %1$I.mart_establishment
   UNION ALL SELECT eligibility,duplicate_status FROM %1$I.mart_venue_context
  ), counts AS (
   SELECT duplicate_status bucket,count(*) n FROM roster
   WHERE eligibility<>'excluded' GROUP BY duplicate_status
  )
  SELECT coalesce(jsonb_agg(jsonb_build_object(
   'universe','non-excluded venue records','dimension','duplicates','bucket',bucket,
   'n',n,'denominator',denominator,'percent',100.0*n/nullif(denominator,0),
   'reasons_overlap',false) ORDER BY bucket),'[]')
  FROM (SELECT *,sum(n) OVER() denominator FROM counts) c
 $q$,schema_name) INTO matches;
 RETURN jsonb_set(result,'{coverage}',coverage||matches);
END $$;
REVOKE ALL ON FUNCTION ul_api.read_release(uuid,text,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ul_api.read_release(uuid,text,jsonb) TO urbanlens_api;
