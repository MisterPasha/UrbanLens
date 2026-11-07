with domains as (select * from (values ('establishment'),('company')) t(entity_type))
select '{{ var("release_id") }}'::uuid as release_id,d.entity_type,b.established_at,
       case when b.established_at is null then null else count(e.event_id) end as retained_event_count,
       case when b.established_at is null then 'baseline_unavailable'
            when count(e.event_id)=0 then 'no_observed_changes' else 'observed_changes' end as state,
       '{{ var("assessed_at") }}'::timestamptz-interval '12 months' as retention_boundary,
       (select count(*) from {{ source('silver','history_scope') }} h
        join {{ source('control','source_scope') }} s using(scope_id)
        where (d.entity_type='company' and s.provider='companies-house')
           or (d.entity_type='establishment' and s.provider in ('fsa','osm'))) as baseline_source_scopes
from domains d left join {{ source('silver','history_baseline') }} b using(entity_type)
left join {{ ref('mart_activity') }} e using(entity_type) group by 1,2,3
