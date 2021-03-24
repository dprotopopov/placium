DROP TABLE IF EXISTS temp_node;
DROP TABLE IF EXISTS temp_way;
DROP TABLE IF EXISTS temp_relation;

CREATE TABLE temp_node (
	id BIGINT, 
	version INTEGER, 
	latitude DOUBLE PRECISION, 
	longitude DOUBLE PRECISION,
	change_set_id BIGINT, 
	time_stamp TIMESTAMP,
	user_id INT, 
	user_name VARCHAR(255), 
	visible BOOLEAN, 
	tags hstore
);

CREATE TABLE temp_way (
	id BIGINT, 
	version INTEGER, 
	change_set_id BIGINT, 
	time_stamp TIMESTAMP,
	user_id INTEGER, 
	user_name VARCHAR(255), 
	visible BOOLEAN, 
	tags hstore,
	nodes BIGINT[]
);

CREATE TABLE temp_relation (
	id BIGINT, 
	version INTEGER, 
	change_set_id BIGINT, 
	time_stamp TIMESTAMP,
	user_id INTEGER, 
	user_name VARCHAR(255), 
	visible BOOLEAN, 
	tags hstore,
	members relation_member[]
);

