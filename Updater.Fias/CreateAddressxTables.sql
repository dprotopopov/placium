DROP TABLE IF EXISTS addressx;
CREATE TABLE addressx (
	id BIGINT PRIMARY KEY,
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