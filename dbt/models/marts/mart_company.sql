select '{{ var("release_id") }}'::uuid as release_id,c.*,
       coalesce((select jsonb_agg(jsonb_build_object('cohort',h.cohort,'basis',h.basis)
          order by h.cohort) from {{ source('silver','company_cohort') }} h
          where h.company_number=c.company_number),'[]'::jsonb) as cohorts,
       'Registered office is not evidence of a trading premises'::text as office_caveat
from {{ source('silver','company') }} c
