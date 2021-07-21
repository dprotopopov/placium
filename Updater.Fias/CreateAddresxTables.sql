﻿DROP TABLE IF EXISTS addresx;
CREATE TABLE addresx (
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
	guid VARCHAR(255) PRIMARY KEY
);