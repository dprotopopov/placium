CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;

DROP TABLE IF EXISTS temp_edge;
DROP TABLE IF EXISTS edge;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'coordinate') THEN
		CREATE TYPE coordinate AS (
			latitude REAL, 
			longitude REAL
		);
    END IF;
END$$;

CREATE TEMP TABLE temp_edge (
	guid UUID NOT NULL,
	from_node BIGINT, 
	to_node BIGINT,
	from_latitude REAL NOT NULL, 
	from_longitude REAL NOT NULL, 
	to_latitude REAL NOT NULL, 
	to_longitude REAL NOT NULL, 
	distance REAL,
	coordinates coordinate[],
	location GEOMETRY,
	tags hstore,
	direction hstore,
	weight hstore
);

CREATE TABLE IF NOT EXISTS edge (
	id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	from_node BIGINT, 
	to_node BIGINT,
	from_latitude REAL NOT NULL, 
	from_longitude REAL NOT NULL, 
	to_latitude REAL NOT NULL, 
	to_longitude REAL NOT NULL, 
	distance REAL, 
	coordinates coordinate[],
	location GEOMETRY,
	tags hstore,
	direction hstore,
	weight hstore,
	nodes bigint[],
	PRIMARY KEY (id, guid)
);

CREATE UNIQUE INDEX IF NOT EXISTS edge_guid_from_node_to_node_idx ON edge (guid,from_node,to_node);
CREATE INDEX IF NOT EXISTS edge_nodes_idx ON edge USING GIN (nodes);
