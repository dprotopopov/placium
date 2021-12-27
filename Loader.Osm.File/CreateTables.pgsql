CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;

DROP TABLE IF EXISTS service_history;
DROP TABLE IF EXISTS addrx;
DROP TABLE IF EXISTS placex;
DROP TABLE IF EXISTS node;
DROP TABLE IF EXISTS way;
DROP TABLE IF EXISTS relation;
DROP TABLE IF EXISTS temp_node;
DROP TABLE IF EXISTS temp_way;
DROP TABLE IF EXISTS temp_relation;

DROP TYPE IF EXISTS relation_member;
DROP TYPE IF EXISTS osm_type;
DROP TYPE IF EXISTS service_type;

DROP SEQUENCE IF EXISTS record_number_seq;
DROP SEQUENCE IF EXISTS record_id_seq;

CREATE SEQUENCE record_number_seq;
CREATE SEQUENCE record_id_seq;

CREATE TYPE osm_type AS ENUM ('node', 'way', 'relation');
CREATE TYPE service_type AS ENUM ('node', 'way', 'relation', 'placex', 'addrx');
CREATE TYPE relation_member AS (
	id BIGINT, 
    role VARCHAR(255),
    type INTEGER
);


CREATE TABLE service_history(
	service_type service_type PRIMARY KEY,
	last_record_number BIGINT
);

CREATE TABLE addrx (
	id BIGINT PRIMARY KEY,
	tags hstore,
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);

CREATE TABLE placex (
	id BIGSERIAL PRIMARY KEY,
	osm_id BIGINT, 
	osm_type osm_type,
	tags hstore,
	location GEOMETRY,
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);

CREATE TABLE node (
	id BIGINT, 
	version INTEGER, 
	latitude REAL, 
	longitude REAL,
	change_set_id BIGINT, 
	time_stamp TIMESTAMP,
	user_id INT, 
	user_name VARCHAR(255), 
	visible BOOLEAN, 
	tags hstore,
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);

CREATE TABLE way (
	id BIGINT, 
	version INTEGER, 
	change_set_id BIGINT, 
	time_stamp TIMESTAMP,
	user_id INTEGER, 
	user_name VARCHAR(255), 
	visible BOOLEAN, 
	tags hstore,
	nodes BIGINT[],
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);

CREATE TABLE relation (
	id BIGINT, 
	version INTEGER, 
	change_set_id BIGINT, 
	time_stamp TIMESTAMP,
	user_id INTEGER, 
	user_name VARCHAR(255), 
	visible BOOLEAN, 
	tags hstore,
	members relation_member[],
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);

