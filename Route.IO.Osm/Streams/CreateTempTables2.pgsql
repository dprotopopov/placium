DROP TABLE IF EXISTS temp_restriction;

CREATE TEMP TABLE temp_restriction (
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	nodes BIGINT[]
);

CREATE TABLE IF NOT EXISTS restriction (
	id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	nodes BIGINT[], 
	PRIMARY KEY (id, guid, vehicle_type)
);

CREATE INDEX IF NOT EXISTS restriction_nodes_idx ON restriction USING GIN (nodes);