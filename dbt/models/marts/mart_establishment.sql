{{ config(indexes=[{'columns': ['establishment_id'], 'unique': true},
                   {'columns': ['location'], 'type': 'gist'},
                   {'columns': ['area_code', 'primary_category']}]) }}
select * from {{ ref('int_venue') }}
where resolved_eligible_supported and primary_spatial_eligible
  and location is not null and location_quality in ('source_point','point_on_surface')
  and geography_version='{{ var("geography_version") }}' and area_code is not null
