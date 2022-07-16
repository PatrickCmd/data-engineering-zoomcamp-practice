DROP TABLE IF EXISTS "citibike-tripdata-all";

CREATE TABLE IF NOT EXISTS "citibike-tripdata-all" AS
(
	SELECT * FROM "JC-202102-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202103-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202104-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202105-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202106-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202107-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202108-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202109-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202110-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202111-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202112-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202201-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202202-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202203-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202204-citibike-tripdata"
	UNION
	SELECT * FROM "JC-202205-citibike-tripdata"
);

SELECT COUNT(*) FROM "citibike-tripdata-all";
