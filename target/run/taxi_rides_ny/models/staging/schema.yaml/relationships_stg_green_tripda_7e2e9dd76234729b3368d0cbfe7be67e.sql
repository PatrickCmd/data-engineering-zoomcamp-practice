select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select dropoff_locationid as from_field
    from `dtc-de-course-347010`.`dbt_pwalukagga`.`stg_green_tripdata`
    where dropoff_locationid is not null
),

parent as (
    select locationid as to_field
    from `dtc-de-course-347010`.`dbt_pwalukagga`.`taxi_zone_lookup`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test