CREATE INDEX ON temp_edge (guid,from_node,to_node);

INSERT INTO edge(
	guid,
	from_node,
	to_node,
	distance,
	profile,
	coordinates,
	tags
)
WITH cte AS
(
   SELECT *,ROW_NUMBER() OVER (PARTITION BY guid,from_node,to_node) AS rn FROM temp_edge
)
SELECT 
	guid,
	from_node,
	to_node,
	distance,
	profile,
	coordinates,
	tags
FROM cte
WHERE rn = 1
ON CONFLICT (guid,from_node,to_node) DO NOTHING;


DROP TABLE temp_edge;
