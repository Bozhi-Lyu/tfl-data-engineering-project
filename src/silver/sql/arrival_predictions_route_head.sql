with bronze as (
    select *
    from read_parquet({{ bronze_path }})
),

normalized as (
    select
        *,
        case
            when dayname(expected_arrival) in ('Monday','Tuesday','Wednesday','Thursday')
                then 'Monday to Thursday'
            when dayname(expected_arrival) = 'Friday'
                then 'Friday'
            when dayname(expected_arrival) = 'Saturday'
                then 'Saturday'
            else 'Sunday'
        end as service_day_group,
        extract(hour from expected_arrival) * 60 + extract(minute from expected_arrival)
            as expected_arrival_minutes
    from bronze
),

with_stop_sequence as (
    select
        line_stop.stop_sequence,
        n.ingestion_ts,
        n.vehicle_id,
        n.line_id,
        n.direction,
        n.stop_id,
        n.expected_arrival,
        n.expected_arrival_minutes,
        n.service_day_group
    from normalized n
    join read_parquet({{ line_stop_path }}) as line_stop
      on n.line_id = line_stop.line_id
     and n.stop_id = line_stop.stop_id
     and n.direction = line_stop.direction
),

ranked as (
    select
        *,
        row_number() over (
            partition by
                ingestion_ts,
                vehicle_id,
                line_id,
                direction
            order by
                stop_sequence
        ) as rn
    from with_stop_sequence
)

select
    ingestion_ts,
    vehicle_id,
    line_id,
    direction,
    stop_id,
    stop_sequence,
    expected_arrival,
    expected_arrival_minutes,
    service_day_group,
from ranked
where rn = 1
order by
    line_id,
    ingestion_ts,
    direction,
    stop_sequence,
    expected_arrival;
