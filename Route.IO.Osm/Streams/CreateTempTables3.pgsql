CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS temp_edge;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'coordinate') THEN
		CREATE TYPE coordinate AS (
			latitude DOUBLE PRECISION, 
			longitude DOUBLE PRECISION
		);
    END IF;
END$$;

CREATE TEMP TABLE temp_edge (
	guid UUID NOT NULL,
	from_node BIGINT, 
	to_node BIGINT,
	distance DOUBLE PRECISION,
	coordinates coordinate[],
	meta_tags hstore,
	profile_tags hstore
);

CREATE TABLE IF NOT EXISTS edge (
	id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	from_node BIGINT, 
	to_node BIGINT,
	distance DOUBLE PRECISION, 
	coordinates coordinate[],
	meta_tags hstore,
	profile_tags hstore,
	PRIMARY KEY (id, guid)
);

CREATE UNIQUE INDEX IF NOT EXISTS edge_guid_from_node_to_node_idx ON edge (guid,from_node,to_node);
