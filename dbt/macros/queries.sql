{% macro install_queries() %}
{% set venues=ref('mart_establishment') %}
{% set areas=ref('dim_area') %}
{% set selected=adapter.quote(target.schema) ~ '.selected_venues' %}
{% call statement('selected_venues', auto_begin=True) %}
create or replace function {{ selected }}(
 p_categories text[] default null,p_area_codes text[] default null,
 p_lon double precision default null,p_lat double precision default null,p_radius integer default null
) returns setof {{ venues }} language plpgsql stable security invoker
set search_path=pg_catalog,public as $body$
begin
 if p_categories is not null and (array_position(p_categories,null) is not null or
   not p_categories <@ array['cafe','restaurant','takeaway','bakery','pub_bar','food_service']) then
   raise exception 'Invalid categories' using errcode='22023';
 end if;
 if p_area_codes is not null and (array_position(p_area_codes,null) is not null or exists(
   select 1 from unnest(p_area_codes) code where not exists(
     select 1 from {{ areas }} a where a.area_code=code))) then
   raise exception 'Invalid supported area' using errcode='22023';
 end if;
 if num_nonnulls(p_lon,p_lat,p_radius) not in (0,3) or
    (p_radius is not null and (not(p_lon between -180 and 180) or
      not(p_lat between -90 and 90) or p_radius not in (250,500,1000,2000))) then
   raise exception 'Invalid centre or supported radius' using errcode='22023';
 end if;
 if p_radius is not null and not exists(select 1 from {{ areas }} a
     where public.ST_Covers(a.geom,public.ST_SetSRID(public.ST_Point(p_lon,p_lat),4326))) then
   raise exception 'Centre outside supported coverage' using errcode='22023';
 end if;
 return query select v.* from {{ venues }} v
 where (p_categories is null or v.primary_category=any(p_categories))
   and (p_area_codes is null or v.area_code=any(p_area_codes))
   and (p_radius is null or public.ST_DWithin(v.location,
       public.ST_SetSRID(public.ST_Point(p_lon,p_lat),4326)::public.geography,p_radius+0.000001));
end $body$;
{% endcall %}

{% call statement('radius_summary', auto_begin=True) %}
create or replace function {{ adapter.quote(target.schema) }}.radius_summary(
 p_lon double precision,p_lat double precision,p_radius integer,p_categories text[] default null
) returns table(establishment_count bigint,category_mix jsonb,supported boolean,partial_coverage boolean)
language plpgsql stable security invoker set search_path=pg_catalog,public as $body$
declare city public.geometry; centre public.geometry;
begin
 if p_lon is null or p_lat is null or p_radius is null or
    not(p_lon between -180 and 180) or not(p_lat between -90 and 90)
    or p_radius not in (250,500,1000,2000) then
   raise exception 'Invalid centre or supported radius' using errcode='22023';
 end if;
 -- Validation and selection share the same function as list/map/summary consumers.
 perform 1 from {{ selected }}(p_categories,null,p_lon,p_lat,p_radius) limit 1;
 select public.ST_UnaryUnion(public.ST_Collect(a.geom)) into city from {{ areas }} a;
 if city is null then raise exception 'Coverage geometry unavailable'; end if;
 centre:=public.ST_SetSRID(public.ST_Point(p_lon,p_lat),4326);
 supported:=public.ST_Covers(city,centre);
 if not supported then raise exception 'Centre outside supported coverage' using errcode='22023'; end if;
 partial_coverage:=public.ST_Distance(centre::public.geography,
     public.ST_Boundary(city)::public.geography)<p_radius;
 return query with counts as (
   select k.category,count(v.establishment_id) n
   from {{ categories() }} k(category)
   left join {{ selected }}(p_categories,null,p_lon,p_lat,p_radius) v
      on v.primary_category=k.category
   where p_categories is null or k.category=any(p_categories) group by 1
 ) select coalesce(sum(n),0)::bigint,
     coalesce(jsonb_object_agg(category,n),'{}'::jsonb),supported,partial_coverage from counts;
end $body$;
{% endcall %}

{% call statement('area_summary', auto_begin=True) %}
create or replace function {{ adapter.quote(target.schema) }}.area_summary(
 p_area_codes text[] default null,p_categories text[] default null
) returns table(establishment_count bigint,official_land_km2 numeric,population bigint,
 establishments_per_land_km2 numeric,land_rate_unavailable_reason text,
 establishments_per_1000_residents numeric,resident_rate_unavailable_reason text,
 population_per_land_km2 numeric,population_density_unavailable_reason text)
language sql stable security invoker set search_path=pg_catalog,public as $body$
 with counts as (select count(*) n from {{ selected }}(p_categories,p_area_codes)),
 denominators as (
   select case when count(*)>0 and count(a.official_land_km2)=count(*) then sum(a.official_land_km2) end a,
          case when count(*)>0 and count(a.population)=count(*) then sum(a.population)::bigint end p
   from {{ areas }} a where p_area_codes is null or a.area_code=any(p_area_codes)
 ) select n,a,p,{{ measures('n','a','p') }} from counts cross join denominators
 $body$;
{% endcall %}
{% call statement('query_verifier_grants', auto_begin=True) %}
grant execute on all functions in schema {{ adapter.quote(target.schema) }} to urbanlens_migrator;
{% endcall %}
{% do adapter.commit() %}
{% endmacro %}
