select '{{ var("release_id") }}'::uuid as release_id,l.link_id,l.company_number,l.establishment_id,
       l.relationship,l.evidence_uri,l.valid_from,r.review_id,r.reviewer,r.reviewed_at,r.reviewed_on,
       e.primary_spatial_eligible
from {{ source('silver','company_establishment_link') }} l
join {{ source('control','review') }} r using(review_id)
join {{ ref('int_venue') }} e on e.establishment_id=l.establishment_id
where l.valid_to is null and l.relationship in ('operator','franchisee')
  and r.decision='approved' and r.review_type='company_link' and e.resolved_eligible_supported
