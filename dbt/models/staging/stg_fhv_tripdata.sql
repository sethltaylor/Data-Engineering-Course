{{ config(materialized='view') }}

select 
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag,
    affiliated_base_number,
    --timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime

from {{ source('staging','fhv') }}
    -- dbt build --m <model.sql> --vars 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}