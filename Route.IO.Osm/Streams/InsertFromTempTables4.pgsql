CREATE INDEX ON temp_meta (guid,node);

INSERT INTO meta(
	guid,
	node,
	tags
)
WITH cte AS
(
   SELECT *,ROW_NUMBER() OVER (PARTITION BY guid,node) AS rn FROM temp_meta
)
SELECT 
	guid,
	node,
	tags
FROM cte
WHERE rn = 1
ON CONFLICT (guid,node) DO NOTHING;

DROP TABLE temp_meta;
