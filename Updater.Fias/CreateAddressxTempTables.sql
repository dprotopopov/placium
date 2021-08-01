DROP TABLE IF EXISTS temp_addressx;
CREATE TEMP TABLE temp_addressx (
	id BIGINT,
	title TEXT,
	priority INTEGER,
	addressString TEXT,
	postalCode VARCHAR(6),
	regionCode VARCHAR(3),
	country VARCHAR(2),
	geoLon REAL,
	geoLat REAL,
	geoExists INTEGER,
	housenumber VARCHAR(127),
	building INTEGER,
	guid VARCHAR(255)
);