DROP TABLE IF EXISTS temp_addrx;
CREATE TEMP TABLE temp_addrx (
	id BIGINT,
	title TEXT,
	priority INTEGER,
	lon REAL,
	lat REAL,
	building INTEGER
);