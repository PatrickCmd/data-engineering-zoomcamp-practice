select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from `dtc-de-course-347010`.`dbt_pwalukagga`.`stg_green_tripdata`
where tripid is null



      
    ) dbt_internal_test