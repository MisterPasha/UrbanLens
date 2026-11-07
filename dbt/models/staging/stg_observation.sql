select o.*, i.source_key, i.generation,
       coalesce(s.observation_state, 'observed') as source_observation_state
from {{ source('silver', 'observation_version') }} o
join {{ source('silver', 'source_identity') }} i using (identity_id)
left join {{ source('silver', 'identity_scope_state') }} s
  on s.current_observation_id=o.observation_id and s.scope_id=o.scope_id
where o.valid_to is null and i.retired_at is null
