with failed_attempts as (
 select * from jsonb_to_recordset(
  '{{ tojson(var("failed_attempts")) | replace("'", "''") }}'::jsonb)
 as t(provider text,scope_key text,status text,started_at timestamptz)
), attempts as (
 select sc.scope_id,f.status,f.started_at from failed_attempts f
 join {{ source('control','source_scope') }} sc using(provider,scope_key)
 union all select scope_id,status,started_at from {{ source('control','ingestion_run') }}
 where mode='collect'
), latest_attempt as (
 select distinct on (scope_id) scope_id,status,started_at
 from attempts order by scope_id,started_at desc,status
), scopes as (
 select sc.provider,sc.scope_key,sc.cadence,s.snapshot_id,
   s.fetched_at as last_success_at,
   r.started_at as last_attempt_at,r.status as attempt_status,
   s.source_published_at,s.source_extract_date,cy.source_edition,
   s.accepted_count,s.rejected_count,s.raw_count,s.parsed_count,
   {{ next_monthly_due('s.fetched_at',8) }} + case sc.provider when 'fsa' then interval '0 hours'
      when 'osm' then interval '1 hour'
      when 'companies-house' then interval '2 hours' when 'ons' then interval '3 hours'
      else interval '4 hours' end as next_due_at,
   case when sc.provider in ('fsa','osm') or sc.scope_key like '%-profiles'
        then 'deployed_monthly' else 'monthly_manual_until_step49' end as scheduling
 from {{ source('control','source_scope') }} sc
 left join {{ source('control','source_head') }} h using(scope_id)
 left join {{ source('control','source_snapshot') }} s on s.snapshot_id=h.snapshot_id
 left join {{ source('control','collection_cycle') }} cy on cy.cycle_id=s.cycle_id
 left join latest_attempt r on r.scope_id=sc.scope_id
), postcodes as (
 select 'postcodes-io'::text as provider,'postcode-enrichment'::text as scope_key,'monthly'::text as cadence,
   null::uuid as snapshot_id,max(observed_at) as last_success_at,max(observed_at) as last_attempt_at,
   'accepted'::text as attempt_status,null::timestamptz as source_published_at,
   null::date as source_extract_date,string_agg(distinct dataset_version,', ' order by dataset_version) as source_edition,
   count(*)::bigint as accepted_count,0::bigint as rejected_count,
   count(*)::bigint as raw_count,count(*)::bigint as parsed_count,
   {{ next_monthly_due('max(observed_at)',12) }} as next_due_at,
   'monthly_manual_until_step49'::text as scheduling
 from {{ source('silver','postcode_cache') }}
), all_sources as (select * from scopes union all select * from postcodes)
select '{{ var("release_id") }}'::uuid as release_id,*,
   {{ health_state('last_success_at','attempt_status','last_attempt_at','next_due_at',
                   "'" ~ var('assessed_at') ~ "'::timestamptz") }} as status,
   coalesce(last_attempt_at>last_success_at and attempt_status in ('failed','rejected'),false)
      as retained_older_snapshot,
   '{{ var("assessed_at") }}'::timestamptz as assessed_at,
   null::timestamptz as current_release_published_at
from all_sources
