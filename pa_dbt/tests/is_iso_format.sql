{% set column_name = args.column_name|string %}
{% set column = ref(column_name) %}

SELECT *
FROM {{ column.table }}
WHERE {{ column.name }} LIKE '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?([+-]\\d{2}:\\d{2}|Z)$';
