select * from {{ ref('int_venue') }}
where establishment_id not in (select establishment_id from {{ ref('mart_establishment') }})
