INSERT INTO temp_restriction2 (
	guid,
	vehicle_type,
	from_edge,
	to_edge,
	via_node,
	tags
) SELECT
	t.guid,
	t.vehicle_type,
	ef.id AS from_edge,
	et.id AS to_edge,
	t.via_node,
	t.tags
FROM temp_restriction t JOIN edge ef ON t.from_way=ef.way JOIN edge et ON t.to_way=et.way
WHERE ef.guid=t.guid AND et.guid=t.guid
AND (ef.from_node=t.via_node OR ef.to_node=t.via_node)
AND (et.from_node=t.via_node OR et.to_node=t.via_node);

CREATE INDEX ON temp_restriction2 (guid,vehicle_type,from_edge,to_edge,via_node);

INSERT INTO restriction(
	guid,
	vehicle_type,
	from_edge,
	to_edge,
	via_node,
	tags
) WITH cte AS (
	SELECT *,ROW_NUMBER() OVER (PARTITION BY guid,vehicle_type,from_edge,to_edge,via_node) AS rn FROM (SELECT * FROM temp_restriction2) q
) SELECT
	guid,
	vehicle_type,
	from_edge,
	to_edge,
	via_node,
	tags
FROM cte WHERE rn=1
ON CONFLICT (guid,vehicle_type,from_edge,to_edge,via_node) DO NOTHING;

DROP TABLE temp_restriction2;
DROP TABLE temp_restriction;
