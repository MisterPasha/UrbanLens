with counts as (
    select k.category,count(e.establishment_id) as establishment_count
    from {{ categories() }} k(category)
    left join {{ ref('mart_establishment') }} e on e.primary_category=k.category group by 1
)
select '{{ var("release_id") }}'::uuid as release_id, category,establishment_count,
       sum(establishment_count) over()::bigint as denominator,
       {{ ratio('establishment_count','sum(establishment_count) over()',100) }} as percent
from counts
