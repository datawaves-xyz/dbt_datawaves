{% macro cte_import(tuple_list) -%}

with{% for cte_ref in tuple_list %} {{cte_ref[0]}} as (
  select * 
  from {{ ref(cte_ref[1]) }}
)
{%- if not loop.last -%}
,
{%- endif -%}

{%- endfor -%}

{%- endmacro %}
