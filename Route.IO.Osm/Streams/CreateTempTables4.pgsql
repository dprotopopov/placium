CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS temp_meta;

CREATE TEMP TABLE temp_meta (
	guid UUID NOT NULL,
	node BIGINT, 
	tags hstore
);

CREATE TABLE IF NOT EXISTS meta (
	id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	node BIGINT, 
	tags hstore,
	PRIMARY KEY (id, guid)
);

CREATE UNIQUE INDEX IF NOT EXISTS meta_guid_node_idx ON meta (guid,node);
