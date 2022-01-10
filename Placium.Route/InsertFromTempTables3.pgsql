INSERT INTO edge(
	guid,
	from_node,
	to_node,
	from_latitude, 
	from_longitude, 
	to_latitude, 
	to_longitude, 
	distance,
	coordinates,
	location,
	tags,
	direction,
	weight,
	nodes
) SELECT 
	guid,
	from_node,
	to_node,
	from_latitude, 
	from_longitude, 
	to_latitude, 
	to_longitude, 
	distance,
	coordinates,
	location,
	tags,
	direction,
	weight,
	ARRAY[from_node,to_node]
FROM temp_edge;

DROP TABLE temp_edge;
