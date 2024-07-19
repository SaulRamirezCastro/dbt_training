{{ config(materialized="table") }}

with
    get_pick_location as (

        select
            case
                when vendorid = 1
                then 'Creative Mobile Technologies LLC'
                when vendorid = 2
                then 'VeriFone Inc.'
                else 'Not Vendor'
            end as vendor_name,
            case
                when ratecodeid = 1
                then 'Standard Rate'
                when ratecodeid = 2
                then 'JFK'
                when ratecodeid = 3
                then 'Newark'
                when ratecodeid = 4
                then 'Westchester'
                when ratecodeid = 5
                then 'Negotiated fare'
                when ratecodeid = 6
                then 'Group ride'
                else 'N/A'
            end as trip_rate,
            lpep_pickup as pickup_time,
            lpep_dropff as drpff_time,
            passenger_count,
            trip_distance as distance,
            lkp.t_zone as pickup_location,
            dolocationid,
            total_amount,
            case
                when payment_type = 1
                then 'Flex Fare trip'
                when payment_type = 2
                then 'Credit card'
                when payment_type = 3
                then 'Cash'
                else 'Others '
            end as payment_type
        from {{ source("taxy_nyc", "green_taxi") }}  as gt
        -- inner join tzl as lkp on gt.putlocationid = lkp.locationid 
        inner join {{ ref("taxi_lkp_zone") }} as lkp on gt.putlocationid = lkp.id

    ),
    get_droff_location as (
        select gpl.*, lkp.t_zone as drop_location
        from get_pick_location  as gpl
        -- inner join tzl as lkp on gpl.dolocationid = lkp.locationid 
        inner join {{ ref("taxi_lkp_zone") }} as lkp on gpl.dolocationid = lkp.id
        where payment_type != 'Others'
    )
select *
from get_droff_location
