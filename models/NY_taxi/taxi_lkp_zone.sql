{{ config(materialized='table') }}

with taxi_lkp as (

    select locationid as id,
    borough, 
    t_zone 
    from {{source('taxy_nyc', 'tzl')}} 
    where 
    service_zone != 'N/A'
)

select *
from taxi_lkp
