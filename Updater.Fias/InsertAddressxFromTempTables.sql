INSERT INTO addressx(
	id,
	title,
	priority,
	addressString,
	postalCode,
	regionCode,
	country,
	geoLon,
	geoLat,
	geoExists,
	building,
	guid)
SELECT 
	id,
	title,
	priority,
	addressString,
	postalCode,
	regionCode,
	country,
	geoLon,
	geoLat,
	geoExists,
	building,
	guid
FROM temp_addressx
ON CONFLICT (id) DO UPDATE SET
	title=EXCLUDED.title,
	priority=EXCLUDED.priority,
	addressString=EXCLUDED.addressString,
	postalCode=EXCLUDED.postalCode,
	regionCode=EXCLUDED.regionCode,
	country=EXCLUDED.country,
	geoLon=EXCLUDED.geoLon,
	geoLat=EXCLUDED.geoLat,
	geoExists=EXCLUDED.geoExists,
	building=EXCLUDED.building,
	guid=EXCLUDED.guid;
DROP TABLE temp_addressx;
