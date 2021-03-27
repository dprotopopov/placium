DROP TABLE IF EXISTS service_history;
DROP TYPE IF EXISTS service_type;

CREATE SEQUENCE IF NOT EXISTS record_number_seq;

CREATE TYPE service_type AS ENUM ('addrob');

CREATE TABLE service_history(
	service_type service_type PRIMARY KEY,
	last_record_number BIGINT
);
