with links as (
    select l.establishment_id,
           jsonb_agg(jsonb_build_object('provider',o.provider,'key',o.source_key,
             'generation',o.generation,'observation_id',o.observation_id,
             'snapshot_id',o.last_snapshot_id,'state',o.source_observation_state,
             'observed_at',o.last_observed_at,'method',l.method,
             'tags',o.source_fields->'tags',
             'record_hash',o.record_hash,'raw_record_locator',o.raw_record_locator,
             'normalised_artifact',o.source_fields->'silver_artifact',
             'evidence_uri',l.evidence_uri) order by o.provider,o.source_key) as sources,
           bool_or(o.provider='fsa') as has_fsa, bool_or(o.provider='osm') as has_osm,
           bool_or(o.source_observation_state<>'no_longer_observed') as has_current_support,
           coalesce(jsonb_agg(jsonb_build_object('source_key',o.source_key,
             'rating',o.source_fields->'rating','observed_at',o.last_observed_at)
             order by o.source_key) filter(where o.provider='fsa'
             and o.source_observation_state<>'no_longer_observed'), '[]'::jsonb) as hygiene
    from {{ source('silver', 'establishment_source_link') }} l
    join {{ ref('stg_observation') }} o using(identity_id)
    where l.valid_to is null and l.decision='accepted'
    group by l.establishment_id
), provenance as (
    select p.establishment_id,
      jsonb_object_agg(p.field_name,jsonb_build_object('observation_id',p.observation_id,
        'review_id',p.review_id,'rule_version',p.rule_version,'source_time',p.source_time,
        'reason',p.selection_reason,'detail',p.detail)) as provenance
    from {{ source('silver', 'field_provenance') }} p where valid_to is null group by 1
), operators as (
    select l.establishment_id,jsonb_agg(jsonb_build_object('company_number',l.company_number,
       'relationship',l.relationship,'review_id',l.review_id,'evidence_uri',l.evidence_uri,
       'reviewed_at',r.reviewed_at,'reviewed_on',r.reviewed_on) order by l.company_number) as companies
    from {{ source('silver', 'company_establishment_link') }} l
    join {{ source('control', 'review') }} r using(review_id)
    where l.valid_to is null and l.relationship in ('operator','franchisee')
      and r.decision='approved' and r.review_type='company_link' group by 1
)
select '{{ var("release_id") }}'::uuid as release_id, e.*,
       e.display_name is null or btrim(e.display_name)='' as name_unavailable,
       coalesce(l.sources,'[]'::jsonb) as sources, coalesce(l.hygiene,'[]'::jsonb) as hygiene,
       coalesce(p.provenance,'{}'::jsonb) as provenance,
       coalesce(o.companies,'[]'::jsonb) as verified_operators,
       case when l.has_fsa and l.has_osm then 'fsa+osm'
            when l.has_fsa then 'fsa_only' when l.has_osm then 'osm_only'
            else 'owner_report_only' end as source_mix,
       coalesce(l.has_current_support,false) as has_current_support,
       e.eligibility='eligible' and e.duplicate_status='resolved'
         and e.observation_state in ('observed','pending_absence')
         and coalesce(l.has_current_support,false) as resolved_eligible_supported,
       array_remove(array[
         case when e.eligibility='excluded' then 'eligibility_excluded' end,
         case when e.eligibility='unresolved' then 'eligibility_unresolved' end,
         case when e.duplicate_status='unresolved' then 'duplicate_unresolved' end,
         case when e.observation_state='no_longer_observed' then 'no_longer_observed' end,
         case when not coalesce(l.has_current_support,false) then 'no_accepted_source_support' end,
         case when not e.primary_spatial_eligible then 'not_primary_spatial' end
       ],null) as exclusion_reasons
from {{ ref('stg_establishment') }} e left join links l using(establishment_id)
left join provenance p using(establishment_id) left join operators o using(establishment_id)
