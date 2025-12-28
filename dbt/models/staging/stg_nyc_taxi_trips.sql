select
    cast(trip_id as bigint) as trip_id,
    cast(pickup_datetime as date) as trip_date,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    case when cast(fare_amount as numeric) > 0 then cast(fare_amount as numeric) else null end as fare_amount,
    case when cast(total_amount as numeric) > 0 then cast(total_amount as numeric) else null end as total_amount,
    cast(payment_type as integer) as payment_type
from {{ source("raw", "nyc_taxi_trips") }}
where pickup_datetime is not null
