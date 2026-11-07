CREATE VIEW ul_silver.current_establishment AS
 SELECT e.* FROM ul_silver.establishment e
 WHERE EXISTS (SELECT 1 FROM ul_silver.establishment_source_link l
   JOIN ul_silver.source_identity i USING(identity_id)
   WHERE l.establishment_id=e.establishment_id AND l.valid_to IS NULL
   AND l.decision='accepted' AND i.retired_at IS NULL)
 OR EXISTS (SELECT 1 FROM ul_silver.reviewed_replacement r
   WHERE r.establishment_id=e.establishment_id);

CREATE VIEW ul_silver.establishment_coverage AS
 WITH sources AS (
   SELECT l.establishment_id,bool_or(i.provider='fsa') fsa,bool_or(i.provider='osm') osm
   FROM ul_silver.establishment_source_link l JOIN ul_silver.source_identity i USING(identity_id)
   WHERE l.valid_to IS NULL AND l.decision='accepted' AND i.retired_at IS NULL GROUP BY 1
 ), roster AS (
   SELECT e.*,CASE WHEN s.fsa AND s.osm THEN 'fsa+osm' WHEN s.fsa THEN 'fsa_only'
     WHEN s.osm THEN 'osm_only' ELSE 'owner_report_only' END source_mix,
     EXISTS(SELECT 1 FROM ul_silver.company_establishment_link l
       WHERE l.establishment_id=e.establishment_id AND l.valid_to IS NULL
       AND l.relationship IN ('operator','franchisee')) verified_company
   FROM ul_silver.current_establishment e LEFT JOIN sources s USING(establishment_id)
 ), populations AS (
   SELECT 'current canonical roster'::text universe,r.* FROM roster r
   UNION ALL SELECT 'eligible resolved supported venues',r.* FROM roster r
     WHERE eligibility='eligible' AND duplicate_status='resolved'
     AND observation_state<>'no_longer_observed'
   UNION ALL SELECT 'primary spatial venues',r.* FROM roster r WHERE primary_spatial_eligible
 ), counts AS (
   SELECT universe,d.dimension,d.bucket,count(*) n FROM populations p
   CROSS JOIN LATERAL (VALUES
     ('eligibility',eligibility),('duplicates',duplicate_status),
     ('coordinate_quality',location_quality),('accepted_source_mix',source_mix),
     ('primary_category',coalesce(primary_category,'unavailable')),
     ('category_detail',CASE WHEN primary_category='food_service' THEN 'broad'
       WHEN primary_category IS NULL THEN 'unavailable' ELSE 'specific' END),
     ('observation_status',observation_state),
     ('primary_spatial_inclusion',CASE WHEN primary_spatial_eligible THEN 'included' ELSE 'excluded' END),
     ('verified_company_link',CASE WHEN verified_company THEN 'linked' ELSE 'unlinked' END)
   ) d(dimension,bucket) GROUP BY 1,2,3
 ) SELECT *,sum(n) OVER(PARTITION BY universe,dimension)::bigint denominator,
   100.0*n/nullif(sum(n) OVER(PARTITION BY universe,dimension),0) percent FROM counts;
