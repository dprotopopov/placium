CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

DROP TABLE IF EXISTS temp_restriction;
DROP TABLE IF EXISTS restriction_way;
DROP TABLE IF EXISTS restriction;

CREATE TEMP TABLE temp_restriction (
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	from_way BIGINT NOT NULL,
	to_way BIGINT NOT NULL,
	via_node BIGINT NOT NULL,
	tags hstore
);

CREATE TABLE IF NOT EXISTS restriction (
	id BIGSERIAL PRIMARY KEY NOT NULL, 
	guid UUID NOT NULL,
	vehicle_type VARCHAR(255),
	from_edge BIGINT NOT NULL,
	to_edge BIGINT NOT NULL,
	via_node BIGINT NOT NULL,
	tags hstore
);


CREATE UNIQUE INDEX IF NOT EXISTS restriction_guid_vehicle_type_from_edge_to_edge_via_node_idx ON restriction (guid,vehicle_type,from_edge,to_edge,via_node);
CREATE INDEX IF NOT EXISTS restriction_from_edge_idx ON restriction (from_edge);
CREATE INDEX IF NOT EXISTS restriction_to_edge_idx ON restriction (to_edge);
CREATE INDEX IF NOT EXISTS restriction_via_node_idx ON restriction (via_node);

