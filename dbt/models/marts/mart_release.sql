select '{{ var("release_id") }}'::uuid as release_id,
       '{{ var("assessed_at") }}'::timestamptz as built_at,
       null::timestamptz as published_at, 'candidate'::text as status,
       '{{ var("geography_version") }}'::text as geography_version,
       '{{ var("population_edition") }}'::text as population_edition,
       '{{ var("population_reference_date") }}'::date as population_reference_date,
       '{{ tojson(var("input_manifest")) | replace("'", "''") }}'::jsonb as input_manifest
