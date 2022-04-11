{% macro displayed_amount(orginal_amount, decimals) %}
  {{ return(adapter.dispatch('displayed_amount', 'ethereum') (orginal_amount, decimals)) }}
{% endmacro %}


{% macro default__displayed_amount(orginal_amount, decimals) -%}

  {{ orginal_amount }} / power(10, {{ decimals }})

{%- endmacro %}

{% macro postgres__displayed_amount(orginal_amount, decimals) -%}

  {{ orginal_amount }} / 10 ^ {{ decimals }}

{%- endmacro %}

{% macro spark__displayed_amount(orginal_amount, decimals) -%}

  {{ orginal_amount }} / power(10, {{ decimals }})

{%- endmacro %}
