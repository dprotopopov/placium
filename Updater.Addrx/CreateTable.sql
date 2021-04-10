DROP TABLE IF EXISTS addrx;
CREATE TABLE addrx (
	id BIGINT PRIMARY KEY,
	tags hstore,
	record_number BIGINT DEFAULT nextval('record_number_seq'),
	record_id BIGINT DEFAULT nextval('record_id_seq')
);
