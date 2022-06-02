CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-347010.trips_data_all.fhv_tripdata`
(
  dispatching_base_num	STRING,		
  pickup_datetime	TIMESTAMP,		
  dropOff_datetime	TIMESTAMP,		
  PUlocationID	FLOAT64,	
  DOlocationID	FLOAT64,		
  SR_Flag	INTEGER,	
  Affiliated_base_number	STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_dtc-de-course-347010/fhv/fhv_tripdata_2019-*.parquet']
);

--- What is count for fhv vehicles data for year 2019
SELECT count(*) FROM `dtc-de-course-347010.trips_data_all.fhv_tripdata`;

SELECT * FROM `dtc-de-course-347010.trips_data_all.fhv_tripdata` LIMIT 1000;


--- How many distinct dispatching_base_num we have in fhv for 2019
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `dtc-de-course-347010.trips_data_all.fhv_tripdata`;

--- Non Partitioned Table
CREATE OR REPLACE TABLE `dtc-de-course-347010.trips_data_all.fhv_nonpartitioned_tripdata`
AS SELECT 
  dispatching_base_num,		
  pickup_datetime,		
  dropOff_datetime,		
  CAST(PUlocationID AS FLOAT64) PUlocationID,	
  CAST(DOlocationID AS	FLOAT64) DOlocationID,		
  CAST(SR_Flag AS	INTEGER) SR_Flag,	
  Affiliated_base_number
FROM `dtc-de-course-347010.trips_data_all.fhv_tripdata`;