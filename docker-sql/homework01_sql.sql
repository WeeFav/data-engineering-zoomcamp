SELECT
	CASE
		WHEN trip_distance <= 1 THEN 'Up to 1 mile'
		WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1~3 miles'
		WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3~7 miles'
		WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7~10 miles'
		ELSE 'Over 10 miles'
	END AS segments,
		COUNT(*)
FROM green_tripdata_2019_10
WHERE lpep_pickup_datetime >= '2019-10-1' AND lpep_pickup_datetime < '2019-11-1' AND
	  lpep_dropoff_datetime >= '2019-10-1' AND lpep_dropoff_datetime < '2019-11-1'
GROUP BY segments;

SELECT lpep_pickup_datetime::date, trip_distance
FROM green_tripdata_2019_10
ORDER BY trip_distance DESC
LIMIT 1;


