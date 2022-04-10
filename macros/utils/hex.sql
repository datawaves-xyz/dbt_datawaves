{% macro hex(expression) %}
  {{ return(adapter.dispatch('hex', 'ethereum') (expression)) }}
{% endmacro %}


{% macro default__hex(expression) %}

  hex(
    {{ expression }}
  )

{% endmacro %}


{% macro spark__hex(expression) %}

  hex(
    {{ expression }}
  )

{% endmacro %}
