CREATE INDEX ON temp_node (guid,id);

INSERT INTO node(
	guid,
	id,
	latitude,
	longitude,
	tags,
	is_core
) WITH cte AS (
	SELECT 
		guid,
		id,
		BOOL_OR(is_core) OR COUNT(*)>1 AS is_core
	FROM temp_node
	GROUP BY guid,id
), cte1 AS (
	SELECT 
		 *, ROW_NUMBER() OVER (PARTITION BY guid,id) rn
	FROM temp_node
) SELECT 
	cte.guid,
	cte.id,
	cte1.latitude,
	cte1.longitude,
	cte1.tags,
	cte.is_core
FROM cte JOIN cte1 ON cte.guid=cte1.guid AND cte.id=cte1.id
WHERE cte1.rn=1
ON CONFLICT (guid,id) DO UPDATE SET
	is_core=true;

DROP TABLE temp_node;
