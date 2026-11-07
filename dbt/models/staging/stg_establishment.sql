select * from {{ source('silver', 'current_establishment') }}
