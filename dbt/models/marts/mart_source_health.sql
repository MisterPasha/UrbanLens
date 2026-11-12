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
   case when {{ 'true' if var('automated_workflow', false) else 'false' }} then 'deployed_monthly'
        when sc.provider in ('fsa','osm') or sc.scope_key like '%-profiles'
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
   '{{ "deployed_monthly" if var("automated_workflow", false) else "monthly_manual_until_step49" }}'::text as scheduling
 from {{ source('silver','postcode_cache') }}
), all_sources as (select * from scopes union all select * from postcodes), checked_sources as (
 select *, case when provider='ons' then
   '{{ var("reference_check", {}).get("checked_at", "") }}'::text end as edition_checked_at,
   case when provider='ons' then {{ 'true' if var('reference_check', {}).get('check_passed') else 'false' }}
        else false end as edition_check_passed
 from all_sources
), dated_sources as (
 select provider,scope_key,cadence,snapshot_id,last_success_at,last_attempt_at,attempt_status,
   source_published_at,source_extract_date,source_edition,accepted_count,rejected_count,raw_count,parsed_count,
   case when edition_check_passed then {{ next_monthly_due("nullif(edition_checked_at,'')::timestamptz",11) }}
        else next_due_at end as next_due_at,
   scheduling,edition_checked_at,edition_check_passed
 from checked_sources
)
select '{{ var("release_id") }}'::uuid as release_id,*,
   {{ health_state("case when edition_check_passed then nullif(edition_checked_at,'')::timestamptz else last_success_at end",'attempt_status','last_attempt_at','next_due_at',
                   "'" ~ var('assessed_at') ~ "'::timestamptz") }} as status,
   coalesce(last_attempt_at>last_success_at and attempt_status in ('failed','rejected'),false)
      as retained_older_snapshot,
   '{{ var("assessed_at") }}'::timestamptz as assessed_at,
   null::timestamptz as current_release_published_at
from dated_sources
