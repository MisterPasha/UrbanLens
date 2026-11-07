{% macro require_candidate() %}
  {% if not target.schema.startswith('ul_candidate_') %}
    {{ exceptions.raise_compiler_error('Gold models require a private ul_candidate_ schema; use dev.py gold build.') }}
  {% endif %}
  {{ return('select 1') }}
{% endmacro %}
