select *
from {{ ref('sensor_counts') }}
where hourly_count < 0