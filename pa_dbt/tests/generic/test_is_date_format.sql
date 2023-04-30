{% test is_date_format(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} LIKE '^\d{4}-\d{2}-\d{2}$'
{% endtest %}