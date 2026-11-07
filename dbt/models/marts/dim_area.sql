select '{{ var("release_id") }}'::uuid as release_id,
       a.geography_version,a.area_code,a.area_name,a.authority_code,a.geom,a.official_land_km2,
       p.population,p.age_0_15,p.age_16_64,p.age_65_plus,
       '{{ var("population_edition") }}'::text as population_edition,
       '{{ var("population_reference_date") }}'::date as population_reference_date,
       a.source_artifact_id as geography_artifact_id,p.source_artifact_id as population_artifact_id,
       {{ ratio('p.age_0_15','p.population',100) }} as age_0_15_percent,
       {{ ratio('p.age_16_64','p.population',100) }} as age_16_64_percent,
       {{ ratio('p.age_65_plus','p.population',100) }} as age_65_plus_percent
from {{ source('silver','area') }} a
left join {{ source('silver','population') }} p
  on p.geography_version=a.geography_version and p.area_code=a.area_code
  and p.population_edition='{{ var("population_edition") }}'
  and p.reference_date='{{ var("population_reference_date") }}'::date
where a.geography_version='{{ var("geography_version") }}' and a.authority_code='E06000043'
