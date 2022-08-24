DROP TABLE IF EXISTS service_history;
DROP TYPE IF EXISTS service_type;

DROP SEQUENCE IF EXISTS record_number_seq;
DROP SEQUENCE IF EXISTS record_id_seq;

CREATE SEQUENCE record_number_seq;
CREATE SEQUENCE record_id_seq;

CREATE TYPE service_type AS ENUM ('addrob', 'house', 'stead', 'room', 'carplace');

CREATE TABLE service_history(
	service_type service_type PRIMARY KEY,
	last_record_number BIGINT
);
