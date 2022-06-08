

  create or replace view `dtc-de-course-347010`.`dbt_pwalukagga`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `dtc-de-course-347010`.`dbt_pwalukagga`.`my_first_dbt_model`
where id = 1;

