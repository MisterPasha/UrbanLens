select '{{ var("release_id") }}'::uuid as release_id,e.*,
       b.established_at as baseline_established_at,
       'Observed changes are not verified premises openings or closures'::text as interpretation
from {{ source('silver','change_event') }} e
left join {{ source('silver','history_baseline') }} b using(entity_type)
where e.observed_at >= '{{ var("assessed_at") }}'::timestamptz-interval '12 months'
