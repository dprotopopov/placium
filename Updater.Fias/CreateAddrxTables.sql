DROP TABLE IF EXISTS addrx;
CREATE TABLE addrx (
	id BIGINT PRIMARY KEY,
	title TEXT,
	priority INTEGER,
	lon REAL,
	lat REAL,
	housenumber VARCHAR(100),
	building INTEGER
);