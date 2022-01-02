INSERT INTO restriction(
	guid,
	vehicle_type,
	from_nodes,
	to_nodes,
	via_nodes,
	tags
) SELECT 
	guid,
	vehicle_type,
	from_nodes,
	to_nodes,
	via_nodes,
	tags
FROM temp_restriction;

DROP TABLE temp_restriction;
