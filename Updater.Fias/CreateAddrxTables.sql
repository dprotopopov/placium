DROP TABLE IF EXISTS addrx;
CREATE TABLE addrx (
	id BIGINT PRIMARY KEY,
	title TEXT,
	priority INTEGER,
	lon REAL,
	lat REAL,
	housenumber VARCHAR(120),
	building INTEGER
);