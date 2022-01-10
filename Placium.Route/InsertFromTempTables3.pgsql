CREATE INDEX ON temp_edge (guid,from_node,to_node);

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
) WITH cte AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY guid,from_node,to_node) AS rn FROM temp_edge
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
FROM cte WHERE rn=1
ON CONFLICT DO NOTHING;

DROP TABLE temp_edge;

