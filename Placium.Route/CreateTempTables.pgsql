CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS node;

CREATE TABLE IF NOT EXISTS node (
	guid UUID NOT NULL, 
	id BIGINT NOT NULL, 
	latitude REAL NOT NULL, 
	longitude REAL NOT NULL, 
	tags hstore,
	is_core BOOLEAN, 
	PRIMARY KEY (guid, id)
);

CREATE INDEX IF NOT EXISTS node_latitude_idx ON node (latitude);
CREATE INDEX IF NOT EXISTS node_longitude_idx ON node (longitude);
CREATE INDEX IF NOT EXISTS node_guid_latitude_idx ON node (guid,latitude);
CREATE INDEX IF NOT EXISTS node_guid_longitude_idx ON node (guid,longitude);
CREATE INDEX IF NOT EXISTS node_guid_latitude_is_core_idx ON node (guid,latitude,is_core);
CREATE INDEX IF NOT EXISTS node_guid_longitude_is_core_idx ON node (guid,longitude,is_core);
CREATE INDEX IF NOT EXISTS node_guid_is_core_idx ON node (guid,is_core);

