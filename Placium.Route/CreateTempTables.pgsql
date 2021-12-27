DROP TABLE IF EXISTS temp_node;
DROP TABLE IF EXISTS node;

CREATE TEMP TABLE temp_node (
	guid UUID NOT NULL, 
	id BIGINT NOT NULL, 
	latitude REAL, 
	longitude REAL,
	is_core BOOLEAN
);

CREATE TABLE IF NOT EXISTS node (
	guid UUID NOT NULL, 
	id BIGINT NOT NULL, 
	latitude REAL, 
	longitude REAL,
	is_core BOOLEAN,
	PRIMARY KEY (guid, id)
);

