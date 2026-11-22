{% macro margin_pct(revenue, cost, precision=2) -%}
    round((({{ revenue }}) - ({{ cost }})) / nullif({{ revenue }}, 0) * 100, {{ precision }})
{%- endmacro %}
