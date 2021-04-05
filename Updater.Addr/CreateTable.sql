DROP TABLE IF EXISTS addr;
CREATE TABLE addr (
	id BIGINT PRIMARY KEY,
	tags hstore,
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);
