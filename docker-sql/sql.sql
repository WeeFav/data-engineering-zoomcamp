-- implicit inner join
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM
	yellow_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- explicit inner join
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM yellow_taxi_data t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- group by and aggregation
SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS "day",
	COUNT(1) AS "count",
	AVG(total_amount) AS "avg_cost"
FROM yellow_taxi_data t
GROUP BY "day"
ORDER BY "day" ASC;

-- multiple group by
SELECT
	CAST(tpep_dropoff_datetime AS DATE) AS "day",
	"DOLocationID",
	COUNT(1) AS "count",
	AVG(total_amount) AS "avg_cost"
FROM yellow_taxi_data t
GROUP BY "day", "DOLocationID"
ORDER BY "day" ASC, "DOLocationID" ASC;