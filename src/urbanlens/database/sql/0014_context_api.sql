-- Retain the existing release resolver, lock and envelope; never resolve active twice.
ALTER FUNCTION ul_api.read_release(uuid,text,jsonb) RENAME TO read_foundation;
REVOKE ALL ON FUNCTION ul_api.read_foundation(uuid,text,jsonb) FROM PUBLIC,urbanlens_api;

CREATE FUNCTION ul_api.area_metrics(p_schema text,p_area text,p_categories text[])
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
DECLARE result jsonb;
BEGIN
 EXECUTE format($q$
  WITH demographics AS (
   SELECT area_code,area_name,geography_version,population_edition,population_reference_date,
    age_0_15,age_16_64,age_65_plus,age_0_15_percent,age_16_64_percent,age_65_plus_percent
   FROM %1$I.dim_area WHERE area_code=$1
   UNION ALL
   SELECT null,'Brighton & Hove',geography_version,population_edition,population_reference_date,
    age_0_15,age_16_64,age_65_plus,age_0_15_percent,age_16_64_percent,age_65_plus_percent
   FROM %1$I.mart_city WHERE $1 IS NULL AND category='all'
  ), mix AS (
   SELECT primary_category,count(*) n FROM %1$I.selected_venues($2,
    CASE WHEN $1 IS NOT NULL THEN ARRAY[$1] END) GROUP BY 1
  )
  SELECT to_jsonb(d)||to_jsonb(s)||jsonb_build_object(
   'land_area_basis','ONS official land area; geography edition '||d.geography_version,
   'category_mix',(SELECT coalesce(jsonb_object_agg(primary_category,n),'{}') FROM mix),
   'age_mix_unavailable_reason',CASE WHEN d.age_0_15_percent IS NULL OR
     d.age_16_64_percent IS NULL OR d.age_65_plus_percent IS NULL THEN 'population_or_age_data_unavailable' END)
  FROM demographics d CROSS JOIN %1$I.area_summary(
   CASE WHEN $1 IS NOT NULL THEN ARRAY[$1] END,$2) s
 $q$,p_schema) INTO result USING p_area,p_categories;
 IF result IS NULL THEN RAISE EXCEPTION 'Unsupported area' USING ERRCODE='22023'; END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION ul_api.area_metrics(text,text,text[]) FROM PUBLIC;

-- Historical locations/addresses are deliberately absent: current privacy flags do
-- not prove that an old address may be republished. Keep useful semantic context.
CREATE FUNCTION ul_api.activity_context(v jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN jsonb_typeof(v)='object' THEN (
  SELECT coalesce(jsonb_object_agg(k,CASE WHEN k='value' THEN ul_api.activity_context(x) ELSE x END),'{}')
  FROM jsonb_each(v) f(k,x) WHERE k IN ('value','state','display_name','name','eligibility',
   'primary_category','duplicate_status','location_quality','observation_state',
   'legal_name','legal_status','company_type','sic_codes','incorporation_date','cessation_date',
   'retired')) END
$$;
REVOKE ALL ON FUNCTION ul_api.activity_context(jsonb) FROM PUBLIC;

CREATE FUNCTION ul_api.read_release(p_release uuid,p_operation text,p_filters jsonb DEFAULT '{}')
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE envelope jsonb; result jsonb; schema_name text; categories text[]; codes text[];
 selected uuid; city jsonb; company_projection text; page_size integer;
BEGIN
 IF p_operation IN ('meta','areas','establishments','detail','postcode','postcode_check') THEN
  RETURN ul_api.read_foundation(p_release,p_operation,p_filters);
 END IF;
 IF p_operation NOT IN ('summary','compare','companies','company','activity') THEN
  RAISE EXCEPTION 'Unsupported query' USING ERRCODE='22023'; END IF;
 envelope:=ul_api.read_foundation(p_release,'meta',p_filters);
 selected:=(envelope->>'release_id')::uuid;
 SELECT model_schema::text INTO schema_name FROM ul_control.data_release WHERE release_id=selected;
 IF p_operation IN ('summary','compare') THEN
  IF p_filters ? 'categories' THEN
   SELECT coalesce(array_agg(v),'{}') INTO categories
    FROM jsonb_array_elements_text(p_filters->'categories') v;
  END IF;
  city:=ul_api.area_metrics(schema_name,null,categories);
  IF p_operation='summary' THEN
   IF p_filters->>'area_code' IS NULL THEN RAISE EXCEPTION 'Area required' USING ERRCODE='22023'; END IF;
   result:=jsonb_build_object('area',ul_api.area_metrics(schema_name,p_filters->>'area_code',categories),
    'city_benchmark',city);
  ELSE
   SELECT array_agg(v) INTO codes FROM jsonb_array_elements_text(p_filters->'area_codes') v;
   IF cardinality(codes)<>2 OR codes[1]=codes[2] OR codes IS NULL THEN
    RAISE EXCEPTION 'Choose two distinct areas' USING ERRCODE='22023'; END IF;
   result:=jsonb_build_object('areas',jsonb_build_array(
    ul_api.area_metrics(schema_name,codes[1],categories),ul_api.area_metrics(schema_name,codes[2],categories)),
    'city_benchmark',city);
  END IF;
 ELSIF p_operation IN ('companies','company') THEN
  -- No live CH proxy, no raw context/provenance and no registered-office coordinates.
  company_projection:=$p$
   SELECT company_number,legal_name,legal_status,company_type,sic_codes,
    incorporation_date,cessation_date,registered_office,registered_office_postcode,
    ARRAY(SELECT v->>'cohort' FROM jsonb_array_elements(cohorts) v
     WHERE v->>'cohort' IN ('registered_office','verified_operator') ORDER BY v->>'cohort') cohorts,
    office_caveat,first_observed_at,last_observed_at,profile_observed_at,
    'https://find-and-update.company-information.service.gov.uk/company/'||company_number url
   FROM %1$I.mart_company c WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(cohorts) h
    WHERE h->>'cohort' IN ('registered_office','verified_operator'))
  $p$;
  IF p_operation='companies' THEN
   IF coalesce(p_filters->>'cohort','') NOT IN ('registered_office','verified_operator') THEN
    RAISE EXCEPTION 'Explicit cohort required' USING ERRCODE='22023'; END IF;
   page_size:=coalesce((p_filters->>'limit')::integer,50);
   IF page_size NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'Invalid limit' USING ERRCODE='22023'; END IF;
   EXECUTE format($q$
    WITH supported AS (%2$s), selected AS MATERIALIZED (
     SELECT * FROM supported WHERE $1->>'cohort'=ANY(cohorts)
     AND ($1->>'legal_status' IS NULL OR legal_status=$1->>'legal_status')
     AND ($1->>'sic_code' IS NULL OR $1->>'sic_code'=ANY(sic_codes))
     AND ($1->>'name' IS NULL OR strpos(lower(legal_name),lower($1->>'name'))>0)
    ), page AS (SELECT * FROM selected WHERE $1->>'after_id' IS NULL
     OR company_number>$1->>'after_id' ORDER BY company_number LIMIT $2)
    SELECT jsonb_build_object('total',(SELECT count(*) FROM selected),
     'items',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY company_number),'[]') FROM page p),
     'has_more',(SELECT count(*)>$2 FROM selected WHERE $1->>'after_id' IS NULL
       OR company_number>$1->>'after_id'),
     'interpretation','Supported legal-company cohort; company counts are not venue counts')
   $q$,schema_name,format(company_projection,schema_name)) INTO result USING p_filters,page_size;
  ELSE
   EXECUTE format($q$
    WITH supported AS (%2$s), venues AS (
     SELECT * FROM %1$I.mart_establishment UNION ALL SELECT * FROM %1$I.mart_venue_context)
    SELECT to_jsonb(c)||jsonb_build_object('verified_premises',(
     SELECT coalesce(jsonb_agg(jsonb_build_object('establishment_id',l.establishment_id,
      'display_name',v.display_name,'relationship',l.relationship,'reviewed_at',l.reviewed_at,
      'reviewed_on',l.reviewed_on,'primary_spatial_eligible',l.primary_spatial_eligible,
      'evidence_url',CASE WHEN l.evidence_uri ~ '^https://' THEN l.evidence_uri END)
      ORDER BY l.establishment_id,l.relationship),'[]')
     FROM %1$I.mart_company_link l JOIN venues v USING(establishment_id)
     WHERE l.company_number=c.company_number))
    FROM supported c WHERE company_number=$1
   $q$,schema_name,format(company_projection,schema_name)) INTO result USING p_filters->>'company_number';
   IF result IS NULL THEN RAISE EXCEPTION 'Unsupported company' USING ERRCODE='UL405'; END IF;
  END IF;
 ELSE
  page_size:=coalesce((p_filters->>'limit')::integer,50);
  IF page_size NOT BETWEEN 1 AND 100 OR
   (p_filters ? 'entity_type' AND p_filters->>'entity_type' NOT IN ('source','establishment','company')) OR
   (p_filters ? 'event_type' AND p_filters->>'event_type' NOT IN ('newly_observed','no_longer_observed',
    'reappeared','source_changed','company_incorporated','company_dissolved','processing_correction')) THEN
   RAISE EXCEPTION 'Unsupported activity filter' USING ERRCODE='22023'; END IF;
  EXECUTE format($q$
   WITH selected AS MATERIALIZED (
    SELECT event_id,entity_type,establishment_id,company_number,event_type,observed_at,source_event_date,
     ul_api.activity_context(before_value) before_value,ul_api.activity_context(after_value) after_value,
     jsonb_build_object('provider',evidence_fields->'provider',
       'processing_version',evidence_fields->'processing_version',
       'record_hash',event_key,'retained_evidence_available',retained_evidence_uri IS NOT NULL) evidence,
     rule_version,baseline_established_at,interpretation
    FROM %1$I.mart_activity WHERE ($1->>'entity_type' IS NULL OR entity_type=$1->>'entity_type')
     AND ($1->>'event_type' IS NULL OR event_type=$1->>'event_type')
   ), remaining AS (
    SELECT * FROM selected WHERE $1->>'after_time' IS NULL OR
     (observed_at,event_id)<(($1->>'after_time')::timestamptz,($1->>'after_id')::uuid)
   ), page AS (SELECT * FROM remaining ORDER BY observed_at DESC,event_id DESC LIMIT $2)
   SELECT jsonb_build_object('total',(SELECT count(*) FROM selected),
    'items',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY observed_at DESC,event_id DESC),'[]') FROM page p),
    'has_more',(SELECT count(*)>$2 FROM remaining),
    'baselines',(SELECT coalesce(jsonb_agg(to_jsonb(b)-'release_id' ORDER BY entity_type),'[]')
      FROM %1$I.mart_activity_status b),
    'interpretation','Observed changes are not verified premises openings or closures',
    'context_note','Historical addresses and coordinates are omitted; private retained evidence is not a public download')
  $q$,schema_name) INTO result USING p_filters,page_size;
 END IF;
 RETURN jsonb_set(envelope,'{data}',result);
END $$;
REVOKE ALL ON FUNCTION ul_api.read_release(uuid,text,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ul_api.read_release(uuid,text,jsonb) TO urbanlens_api;
