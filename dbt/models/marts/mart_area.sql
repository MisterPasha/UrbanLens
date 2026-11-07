with counts as (
 select a.*, k.category,count(e.establishment_id) as establishment_count,
        (select count(*) from {{ ref('mart_establishment') }} v
           where v.area_code=a.area_code) as category_denominator
 from {{ ref('dim_area') }} a
 cross join (select column1 as category from (values ('all')) t(column1)
             union all select column1 from {{ categories() }} t(column1)) k
 left join {{ ref('mart_establishment') }} e on e.area_code=a.area_code
   and (k.category='all' or e.primary_category=k.category)
 group by a.release_id,a.geography_version,a.area_code,a.area_name,a.authority_code,a.geom,
   a.official_land_km2,a.population,a.age_0_15,a.age_16_64,a.age_65_plus,
   a.population_edition,a.population_reference_date,a.geography_artifact_id,
   a.population_artifact_id,a.age_0_15_percent,a.age_16_64_percent,a.age_65_plus_percent,k.category
)
select *,{{ measures('establishment_count','official_land_km2','population') }},
       {{ ratio('establishment_count','category_denominator',100) }} as category_percent
from counts
