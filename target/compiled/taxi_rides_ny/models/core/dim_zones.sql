


SELECT 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone
FROM `dtc-de-course-347010`.`dbt_pwalukagga`.`taxi_zone_lookup`