DROP TABLE IF EXISTS temp_addresx;
CREATE TEMP TABLE temp_addresx (
	title TEXT,
	priority INTEGER,
	addressString TEXT,
	postalCode VARCHAR(6),
	regionCode VARCHAR(3),
	country VARCHAR(2),
	geoLon REAL,
	geoLat REAL,
	geoExists INTEGER,
	building INTEGER,
	guid VARCHAR(255)
);