INSERT INTO restriction(
	guid,
	vehicle_type,
	nodes
)
SELECT 
	guid,
	vehicle_type,
	nodes
FROM temp_restriction;

DROP TABLE temp_restriction;
