select
    trip_date,
    count(*) as trips_cnt,
    sum(coalesce(passenger_count, 0)) as passengers_sum,
    sum(coalesce(trip_distance, 0)) as distance_sum,
    sum(coalesce(total_amount, 0)) as revenue_sum
from {{ ref("stg_nyc_taxi_trips") }}
group by trip_date
order by trip_date
