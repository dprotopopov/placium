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
	id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	from_node BIGINT NOT NULL, 
	to_node BIGINT NOT NULL,
	way BIGINT NOT NULL,
	from_latitude REAL NOT NULL, 
	from_longitude REAL NOT NULL, 
	to_latitude REAL NOT NULL, 
	to_longitude REAL NOT NULL, 
	distance REAL NOT NULL,
	coordinates coordinate[],
	location GEOMETRY,
	tags hstore,
	direction hstore,
	weight hstore 
);

CREATE TABLE IF NOT EXISTS edge (
	id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	from_node BIGINT NOT NULL, 
	to_node BIGINT NOT NULL,
	way BIGINT NOT NULL,
	from_latitude REAL NOT NULL, 
	from_longitude REAL NOT NULL, 
	to_latitude REAL NOT NULL, 
	to_longitude REAL NOT NULL, 
	distance REAL NOT NULL, 
	coordinates coordinate[],
	location GEOMETRY,
	tags hstore,
	direction hstore,
	weight hstore,
	nodes bigint[]
);

CREATE UNIQUE INDEX IF NOT EXISTS edge_guid_from_node_to_node_way_idx ON edge (guid,from_node,to_node,way);
CREATE INDEX IF NOT EXISTS edge_way_idx ON edge (way);
CREATE INDEX IF NOT EXISTS edge_nodes_idx ON edge USING GIN (nodes);
CREATE INDEX IF NOT EXISTS edge_from_latitude_idx ON edge (from_latitude);
CREATE INDEX IF NOT EXISTS edge_from_longitude_idx ON edge (from_longitude);
CREATE INDEX IF NOT EXISTS edge_to_latitude_idx ON edge (to_latitude);
CREATE INDEX IF NOT EXISTS edge_to_longitude_idx ON edge (to_longitude);
CREATE INDEX IF NOT EXISTS edge_direction_idx ON edge USING HASH (direction);
CREATE INDEX IF NOT EXISTS edge_weight_idx ON edge USING HASH (weight);
CREATE INDEX IF NOT EXISTS edge_guid_weight_direction_idx ON edge (guid,weight,direction);
CREATE INDEX IF NOT EXISTS edge_guid_from_node_idx ON edge (guid,from_node);
CREATE INDEX IF NOT EXISTS edge_guid_to_node_idx ON edge (guid,to_node);
CREATE INDEX IF NOT EXISTS edge_guid_from_node_weight_direction_idx ON edge (guid,from_node,weight,direction);
CREATE INDEX IF NOT EXISTS edge_guid_to_node_weight_direction_idx ON edge (guid,to_node,weight,direction);
