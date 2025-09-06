{% macro normalize_country_key(col) -%}
  -- Trim, lowercase, remove punctuation/extra spaces
  regexp_replace(lower(trim({{ col }})), r'[^a-z]+', '')
{%- endmacro %}
