select *
from {{ ref('sensor_counts') }}
where sensor_id >= 1000