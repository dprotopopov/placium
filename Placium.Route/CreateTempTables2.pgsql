CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS temp_restriction;
DROP TABLE IF EXISTS restriction_from_edge;
DROP TABLE IF EXISTS restriction_to_edge;
DROP TABLE IF EXISTS restriction_via_node;
DROP TABLE IF EXISTS restriction;

CREATE TEMP TABLE temp_restriction (
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	from_nodes BIGINT[],
	to_nodes BIGINT[],
	via_nodes BIGINT[],
	tags hstore
);

CREATE TABLE IF NOT EXISTS restriction (
	id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	from_nodes BIGINT[],
	to_nodes BIGINT[],
	via_nodes BIGINT[],
	tags hstore, 
	PRIMARY KEY (id, guid, vehicle_type)
);

CREATE TABLE IF NOT EXISTS restriction_from_edge (
    id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
    rid BIGINT NOT NULL, 
    edge BIGINT NOT NULL, 
	PRIMARY KEY (id, guid, vehicle_type),
	FOREIGN KEY (rid, guid, vehicle_type) REFERENCES restriction (id, guid, vehicle_type)
);

CREATE TABLE IF NOT EXISTS restriction_to_edge (
    id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
    rid BIGINT NOT NULL, 
    edge BIGINT NOT NULL, 
	PRIMARY KEY (id, guid, vehicle_type),
	FOREIGN KEY (rid, guid, vehicle_type) REFERENCES restriction (id, guid, vehicle_type)
);

CREATE TABLE IF NOT EXISTS restriction_via_node (
    id BIGSERIAL NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
    rid BIGINT NOT NULL, 
    node BIGINT NOT NULL, 
	PRIMARY KEY (id, guid, vehicle_type),
	FOREIGN KEY (rid, guid, vehicle_type) REFERENCES restriction (id, guid, vehicle_type)
);

CREATE INDEX IF NOT EXISTS restriction_from_nodes_idx ON restriction USING GIN (from_nodes);
CREATE INDEX IF NOT EXISTS restriction_to_nodes_idx ON restriction USING GIN (to_nodes);
CREATE INDEX IF NOT EXISTS restriction_via_nodes_idx ON restriction USING GIN (via_nodes);
