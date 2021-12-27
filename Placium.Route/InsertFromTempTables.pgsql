CREATE INDEX ON temp_node (guid,id);

INSERT INTO node(
	guid,
	id,
	latitude,
	longitude,
	is_core
)
SELECT 
	guid,
	id,
	MAX(latitude),
	MAX(longitude),
	BOOL_OR(is_core) OR COUNT(*)>1
FROM temp_node
GROUP BY guid,id
ON CONFLICT (guid,id) DO UPDATE SET
	is_core=true;

DROP TABLE temp_node;
