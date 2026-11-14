-- Cross-population checks catch lost venues, source-join multiplication and incompatible filters.
select 'spatial_population' as failure
where (select count(*) from {{ ref('mart_establishment') }}) <>
      (select count(*) from {{ source('silver','current_establishment') }} where primary_spatial_eligible)
union all select 'roster_partition'
where (select count(*) from {{ ref('mart_establishment') }}) +
      (select count(*) from {{ ref('mart_venue_context') }}) <>
      (select count(*) from {{ source('silver','current_establishment') }})
union all select 'partition_overlap' where exists(
 select 1 from {{ ref('mart_establishment') }} e join {{ ref('mart_venue_context') }} c using(establishment_id))
union all select 'category_count'
where (select coalesce(sum(establishment_count),0) from {{ ref('mart_category') }}) <>
      (select count(*) from {{ ref('mart_establishment') }})
union all select 'area_city_count' where exists(
 select 1 from {{ ref('mart_city') }} c where c.establishment_count <>
  (select count(*) from {{ ref('mart_establishment') }} e where c.category='all' or e.primary_category=c.category))
union all select 'age_totals' where exists(
 select 1 from {{ ref('dim_area') }} where population<>age_0_15+age_16_64+age_65_plus)
union all select 'unreviewed_operator' where exists(
 select 1 from {{ ref('mart_company_link') }} l left join {{ source('control','review') }} r using(review_id)
 where r.decision is distinct from 'approved' or r.review_type<>'company_link')
union all select 'untraceable_event' where exists(
 select 1 from {{ ref('mart_activity') }} where retained_evidence_uri is null)
union all select 'release_mismatch' where exists(
 select 1 from {{ ref('mart_establishment') }} e join {{ ref('mart_release') }} r on e.release_id<>r.release_id)
union all select 'area_coverage'
where (select count(*) from {{ ref('dim_area') }}) <>
      (select count(*) from {{ source('silver','area') }}
       where geography_version='{{ var("geography_version") }}' and authority_code='E06000043')
union all select 'population_coverage' where exists(
 select 1 from {{ ref('dim_area') }}
 where population is null or age_0_15 is null or age_16_64 is null or age_65_plus is null)
union all select 'venue_area_relationship' where exists(
 select 1 from {{ ref('mart_establishment') }} e
 left join {{ ref('dim_area') }} a using(area_code) where a.area_code is null)
union all select 'reference_edition' where exists(
 select 1 from (
   select geography_version,population_edition,population_reference_date from {{ ref('dim_area') }}
   union all select geography_version,population_edition,population_reference_date from {{ ref('mart_area') }}
   union all select geography_version,population_edition,population_reference_date from {{ ref('mart_city') }}
 ) a cross join {{ ref('mart_release') }} r
 where a.geography_version is distinct from r.geography_version
    or a.population_edition is distinct from r.population_edition
    or a.population_reference_date is distinct from r.population_reference_date)
union all select 'company_link_relationship' where exists(
 select 1 from {{ ref('mart_company_link') }} l
 left join {{ ref('mart_company') }} c using(company_number)
 left join (
   select establishment_id from {{ ref('mart_establishment') }}
   union all select establishment_id from {{ ref('mart_venue_context') }}
 ) e using(establishment_id)
 where c.company_number is null or e.establishment_id is null
    or l.relationship not in ('operator','franchisee'))
union all select 'source_counts' where exists(
 select 1 from {{ ref('mart_source_health') }}
 where accepted_count<0 or rejected_count<0 or parsed_count<0 or raw_count<0
    or accepted_count+rejected_count<>parsed_count or parsed_count>raw_count)
