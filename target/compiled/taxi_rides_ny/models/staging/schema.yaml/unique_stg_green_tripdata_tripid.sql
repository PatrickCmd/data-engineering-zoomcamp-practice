
    
    

with dbt_test__target as (
  
  select tripid as unique_field
  from `dtc-de-course-347010`.`dbt_pwalukagga`.`stg_green_tripdata`
  where tripid is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


