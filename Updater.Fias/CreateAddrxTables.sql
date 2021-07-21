DROP TABLE IF EXISTS addrx;
CREATE TABLE addrx (
	id BIGINT PRIMARY KEY,
	title TEXT,
	priority INTEGER,
	lon REAL,
	lat REAL,
	building INTEGER
);