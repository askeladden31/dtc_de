with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number,
        {{ dbt.date_trunc("year", "pickup_datetime") }} as pickup_year
    from source

)

select * from renamed 
    where pickup_year = '2019-01-01'