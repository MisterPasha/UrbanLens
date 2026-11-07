-- Hand-computable cases exercise the SAME macros as area/city/query functions.
with fixture as (
 select * from (values
   ('one',2::bigint,1::numeric,100::bigint,2::numeric,20::numeric,null::text,null::text),
   ('two',1,9,900,1::numeric/9,1000::numeric/900,null,null),
   ('zero_n',0,2,100,0,0,null,null),
   ('zero_population',0,2,0,0,null,null,'non_positive_population'),
   ('missing',1,null,null,null,null,'missing_land_area','missing_population'),
   ('zero_area',1,0,100,null,10,'non_positive_land_area',null)
 ) v(label,n,a,p,expected_land,expected_resident,land_reason,resident_reason)
), computed as (
 select *,{{ measures('n','a','p') }} from fixture
), weighted as (
 select sum(n) n,sum(a) a,sum(p) p,
   sum(case when label='one' then 10 else 270 end) age_0_15,
   sum(case when label='one' then 80 else 540 end) age_16_64,
   sum(case when label='one' then 10 else 90 end) age_65_plus
 from fixture where label in ('one','two')
), result as (select *,{{ measures('n','a','p') }} from weighted)
select label as failure from computed
where establishments_per_land_km2 is distinct from expected_land
   or establishments_per_1000_residents is distinct from expected_resident
   or land_rate_unavailable_reason is distinct from land_reason
   or resident_rate_unavailable_reason is distinct from resident_reason
union all select 'weighted_city' from result
where n<>3 or a<>10 or p<>1000 or establishments_per_land_km2<>0.3
   or establishments_per_1000_residents<>3 or population_per_land_km2<>100
union all select 'age_shares'
from result
where age_0_15+age_16_64+age_65_plus<>p
  or {{ ratio('age_0_15','p',100) }}<>28 or {{ ratio('age_16_64','p',100) }}<>62
  or {{ ratio('age_65_plus','p',100) }}<>10 or {{ ratio('20','0',100) }} is not null
