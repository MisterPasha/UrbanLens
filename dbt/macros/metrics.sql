{% macro ratio(n, d, scale=1) -%}
case when ({{ d }}) > 0 then ({{ n }})::numeric * {{ scale }} / ({{ d }}) end
{%- endmacro %}

{% macro rate_reason(d, name) -%}
case when ({{ d }}) is null then 'missing_{{ name }}'
     when ({{ d }}) <= 0 then 'non_positive_{{ name }}' end
{%- endmacro %}

{% macro measures(n, a, p) -%}
{{ ratio(n, a) }} as establishments_per_land_km2,
{{ rate_reason(a, 'land_area') }} as land_rate_unavailable_reason,
{{ ratio(n, p, 1000) }} as establishments_per_1000_residents,
{{ rate_reason(p, 'population') }} as resident_rate_unavailable_reason,
case when ({{ a }}) > 0 and ({{ p }}) is not null
     then ({{ p }})::numeric / ({{ a }}) end as population_per_land_km2,
case when ({{ p }}) is null then 'missing_population'
     else {{ rate_reason(a, 'land_area') }} end as population_density_unavailable_reason
{%- endmacro %}

{% macro next_monthly_due(t, hour=12) -%}
date_trunc('month', {{ t }} at time zone 'UTC') at time zone 'UTC'
    + interval '1 month 6 days {{ hour }} hours'
{%- endmacro %}

{% macro health_state(success, attempt_status, attempt_time, due, now) -%}
case when {{ success }} is null then 'unavailable'
     when {{ now }} > {{ due }} + interval '48 hours' then 'stale'
     when {{ attempt_time }} > {{ success }} and {{ attempt_status }} = 'failed' then 'failed'
     when {{ attempt_time }} > {{ success }} and {{ attempt_status }} = 'rejected' then 'held'
     else 'healthy' end
{%- endmacro %}

{% macro categories() -%}
(values ('cafe'),('restaurant'),('takeaway'),('bakery'),('pub_bar'),('food_service'))
{%- endmacro %}

{% test grain(model, columns) %}
select {{ columns | join(', ') }} from {{ model }}
group by {{ columns | join(', ') }}
having count(*) <> 1 or {% for col in columns %}{{ col }} is null{% if not loop.last %} or {% endif %}{% endfor %}
{% endtest %}
