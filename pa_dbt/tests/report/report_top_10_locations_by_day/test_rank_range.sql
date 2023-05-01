{{ config(schema='report') }}

SELECT * 
FROM {{ ref('report_top_10_locations_by_day') }}
WHERE rank NOT IN (1,2,3,4,5,6,7,8,9,10)