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
