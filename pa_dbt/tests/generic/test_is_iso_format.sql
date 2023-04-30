{% test is_iso_format(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} LIKE '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?([+-]\\d{2}:\\d{2}|Z)$'
{% endtest %}