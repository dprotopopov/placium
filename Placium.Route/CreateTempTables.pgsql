CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS temp_node;
DROP TABLE IF EXISTS node;

CREATE TEMP TABLE temp_node (
	guid UUID NOT NULL, 
	id BIGINT NOT NULL, 
	latitude REAL NOT NULL, 
	longitude REAL NOT NULL, 
	tags hstore,
	is_core BOOLEAN
);

CREATE TABLE IF NOT EXISTS node (
	guid UUID NOT NULL, 
	id BIGINT NOT NULL, 
	latitude REAL NOT NULL, 
	longitude REAL NOT NULL, 
	tags hstore,
	is_core BOOLEAN, 
	PRIMARY KEY (guid, id)
);

