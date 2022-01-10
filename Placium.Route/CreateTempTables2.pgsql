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
	id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	from_nodes BIGINT[],
	to_nodes BIGINT[],
	via_nodes BIGINT[],
	tags hstore
);

CREATE TABLE IF NOT EXISTS restriction_from_edge (
    id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
    rid BIGINT NOT NULL, 
    edge BIGINT NOT NULL, 
	FOREIGN KEY (rid) REFERENCES restriction (id)
);

CREATE TABLE IF NOT EXISTS restriction_to_edge (
    id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
    rid BIGINT NOT NULL, 
    edge BIGINT NOT NULL, 
	FOREIGN KEY (rid) REFERENCES restriction (id)
);

CREATE TABLE IF NOT EXISTS restriction_via_node (
    id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
    rid BIGINT NOT NULL, 
    node BIGINT NOT NULL, 
	FOREIGN KEY (rid) REFERENCES restriction (id)
);

CREATE INDEX IF NOT EXISTS restriction_from_nodes_idx ON restriction USING GIN (from_nodes);
CREATE INDEX IF NOT EXISTS restriction_to_nodes_idx ON restriction USING GIN (to_nodes);
CREATE INDEX IF NOT EXISTS restriction_via_nodes_idx ON restriction USING GIN (via_nodes);
CREATE INDEX IF NOT EXISTS restriction_via_node_node_idx ON restriction_via_node (node);
CREATE INDEX IF NOT EXISTS restriction_from_edge_rid_edge_idx ON restriction_from_edge (rid,edge);
CREATE INDEX IF NOT EXISTS restriction_to_edge_edge_idx ON restriction_to_edge (edge);
CREATE INDEX IF NOT EXISTS restriction_guid_vehicle_type_idx ON restriction (guid,vehicle_type);
CREATE INDEX IF NOT EXISTS restriction_from_edge_rid_guid_vehicle_type_idx ON restriction_from_edge (rid,guid,vehicle_type);
CREATE INDEX IF NOT EXISTS restriction_to_edge_rid_guid_vehicle_type_idx ON restriction_to_edge (rid,guid,vehicle_type);
CREATE INDEX IF NOT EXISTS restriction_via_node_rid_guid_vehicle_type_idx ON restriction_via_node (rid,guid,vehicle_type);
CREATE INDEX IF NOT EXISTS restriction_from_edge_guid_vehicle_type_edge_idx ON restriction_from_edge (guid,vehicle_type,edge);
CREATE INDEX IF NOT EXISTS restriction_to_edge_guid_vehicle_type_edge_idx ON restriction_to_edge (guid,vehicle_type,edge);
CREATE INDEX IF NOT EXISTS restriction_via_node_guid_vehicle_type_node_idx ON restriction_via_node (guid,vehicle_type,node);
