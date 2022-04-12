-- Inner joins
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- Basic data quality checks
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t
LIMIT 100;

-- Check for NULL entries
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t
WHERE "PULocationID" is NULL
LIMIT 100;

SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t
WHERE "DOLocationID" is NULL
LIMIT 100;

-- Checking for missing LocationIDs
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t
WHERE "PULocationID" NOT IN (SELECT "LocationID" FROM zones)
LIMIT 100;

SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t
WHERE "DOLocationID" NOT IN (SELECT "LocationID" FROM zones)
LIMIT 100;

-- Left, Right and Outer joins
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t
LEFT JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
LEFT JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t
RIGHT JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
RIGHT JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- SQL FULL OUTER JOIN Keyword
-- The FULL OUTER JOIN keyword returns all records when there is a match in left (table1) or right (table2) table records.
-- Tip: FULL OUTER JOIN and FULL JOIN are the same.

SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t
FULL OUTER JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
FULL OUTER JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_trips t
FULL JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
FULL JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;


-- GROUP BY AND ORDER BY
SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips
FROM
	yellow_taxi_trips t
GROUP BY
	CAST(tpep_dropoff_datetime AS DATE);

SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips
FROM
	yellow_taxi_trips t
GROUP BY 1;

SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips,
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_trips t
GROUP BY 1;

-- ORDER BY
SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips
FROM
	yellow_taxi_trips t
GROUP BY 1
ORDER BY 1 ASC;

SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips
FROM
	yellow_taxi_trips t
GROUP BY 1
ORDER BY 1 DESC;

-- DAY WITH HIGHEST NO. OF TRIPS
SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips
FROM
	yellow_taxi_trips t
GROUP BY 1
ORDER BY 2 DESC
LIMIT 2;

-- DAY WITH LOWEST NO. OF TRIPS
SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	COUNT(1) AS no_trips
FROM
	yellow_taxi_trips t
GROUP BY 1
ORDER BY 2 ASC
LIMIT 2;


SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	"DOLocationID",
	COUNT(1) AS no_trips,
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_trips t
GROUP BY 1, 2
ORDER BY 1;

SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS day,
	"DOLocationID",
	COUNT(1) AS no_trips,
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_trips t
GROUP BY 1, 2
ORDER BY 1, 2 DESC;