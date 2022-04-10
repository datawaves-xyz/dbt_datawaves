{% test assert_equal(model, actual, expected) %}
select * from {{ model }} where {{ actual }} != {{ expected }}

{% endtest %}