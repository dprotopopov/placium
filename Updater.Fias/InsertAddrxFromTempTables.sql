INSERT INTO addrx(
	id,
	title,
	priority,
	lon,
	lat,
	building
)
SELECT 
	id,
	title,
	priority,
	lon,
	lat,
	building
FROM temp_addrx
ON CONFLICT (id) DO UPDATE SET
	title=EXCLUDED.title,
	priority=EXCLUDED.priority,
	lon=EXCLUDED.lon,
	lat=EXCLUDED.lat,
	building=EXCLUDED.building;
DROP TABLE temp_addrx;
CREATE INDEX addrx_title_idx ON addrx USING GIN(to_tsvector('russian',title));
