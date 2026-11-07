with totals as (
 select release_id,geography_version,population_edition,population_reference_date,category,
    count(*) as area_count,sum(establishment_count)::bigint as establishment_count,
    case when count(official_land_km2)=count(*) then sum(official_land_km2) end as official_land_km2,
    case when count(population)=count(*) then sum(population) end as population,
    case when count(age_0_15)=count(*) then sum(age_0_15) end as age_0_15,
    case when count(age_16_64)=count(*) then sum(age_16_64) end as age_16_64,
    case when count(age_65_plus)=count(*) then sum(age_65_plus) end as age_65_plus,
    sum(category_denominator) as category_denominator
 from {{ ref('mart_area') }} group by 1,2,3,4,5
)
select *,{{ measures('establishment_count','official_land_km2','population') }},
       {{ ratio('establishment_count','category_denominator',100) }} as category_percent,
       {{ ratio('age_0_15','population',100) }} as age_0_15_percent,
       {{ ratio('age_16_64','population',100) }} as age_16_64_percent,
       {{ ratio('age_65_plus','population',100) }} as age_65_plus_percent
from totals
