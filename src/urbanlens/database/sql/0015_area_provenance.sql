-- Add the exact retained reference artifact identities, rather than treating
-- the geographic year alone as a land-area product edition.
ALTER FUNCTION ul_api.area_metrics(text,text,text[]) RENAME TO area_metrics_base;
CREATE FUNCTION ul_api.area_metrics(p_schema text,p_area text,p_categories text[])
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
DECLARE result jsonb; hashes jsonb;
BEGIN
 result:=ul_api.area_metrics_base(p_schema,p_area,p_categories);
 EXECUTE format('SELECT coalesce(jsonb_agg(DISTINCT a.sha256),''[]'')
  FROM %I.dim_area d JOIN ul_control.artifact a ON a.artifact_id=d.geography_artifact_id
  WHERE $1 IS NULL OR d.area_code=$1',p_schema) INTO hashes USING p_area;
 RETURN result||jsonb_build_object('land_area_edition',jsonb_build_object(
  'product','SAM March 2021 EW V2','unit_conversion','official land hectares / 100',
  'boundary_product','December 2021 BFC V10','reference_artifact_sha256',hashes));
END $$;
REVOKE ALL ON FUNCTION ul_api.area_metrics(text,text,text[]) FROM PUBLIC;
