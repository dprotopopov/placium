CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS region_edge;
DROP TABLE IF EXISTS region;

CREATE TABLE IF NOT EXISTS region (
	guid UUID NOT NULL, 
	id BIGSERIAL NOT NULL, 
	min_latitude REAL NOT NULL, 
	min_longitude REAL NOT NULL, 
	max_latitude REAL NOT NULL, 
	max_longitude REAL NOT NULL, 
	PRIMARY KEY (guid, id)
);

CREATE TABLE IF NOT EXISTS region_edge (
	id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	from_region BIGINT NOT NULL, 
	to_region BIGINT NOT NULL,
	edges BIGINT[],
	weight hstore,
	PRIMARY KEY (id, guid)
);

CREATE UNIQUE INDEX IF NOT EXISTS region_edge_guid_from_region_to_region_idx ON region_edge (guid,from_region,to_region);
CREATE INDEX IF NOT EXISTS region_edge_edges_idx ON region_edge USING GIN (edges);
