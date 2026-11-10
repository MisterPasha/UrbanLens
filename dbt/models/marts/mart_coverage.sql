-- Reuse the single existing SQL coverage definition; overlapping reasons are a separate dimension.
select '{{ var("release_id") }}'::uuid as release_id, universe, dimension, bucket,
       n, denominator, percent, false as reasons_overlap
from {{ source('silver', 'establishment_coverage') }}
where not (universe='current canonical roster' and dimension='duplicates')
union all
select '{{ var("release_id") }}'::uuid, 'non-excluded venue records','duplicates',duplicate_status,
       count(*),sum(count(*)) over()::bigint,
       100.0*count(*)/nullif(sum(count(*)) over(),0),false
from {{ ref('int_venue') }} where eligibility<>'excluded' group by duplicate_status
union all
select '{{ var("release_id") }}'::uuid, 'current canonical roster','exclusion_reason',r,
       count(*),(select count(*) from {{ ref('int_venue') }}),
       {{ ratio('count(*)', '(select count(*) from ' ~ ref('int_venue') ~ ')', 100) }},true
from {{ ref('int_venue') }} cross join lateral unnest(exclusion_reasons) r
where r<>'duplicate_unresolved' group by r
