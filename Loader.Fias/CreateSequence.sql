DROP TABLE IF EXISTS service_history;
DROP TABLE IF EXISTS service_history2;
DROP TABLE IF EXISTS service_history3;
DROP TYPE IF EXISTS service_type;
DROP TYPE IF EXISTS service_type2;
DROP TYPE IF EXISTS service_type3;

DROP SEQUENCE IF EXISTS record_number_seq;
DROP SEQUENCE IF EXISTS record_id_seq;

CREATE SEQUENCE record_number_seq;
CREATE SEQUENCE record_id_seq;

CREATE TYPE service_type AS ENUM ('addrob', 'house', 'stead');
CREATE TYPE service_type2 AS ENUM ('addrob', 'house', 'stead', 'room');
CREATE TYPE service_type3 AS ENUM ('addrob', 'house', 'stead', 'room');

CREATE TABLE service_history(
	service_type service_type PRIMARY KEY,
	last_record_number BIGINT
);

CREATE TABLE service_history2(
	service_type2 service_type2 PRIMARY KEY,
	last_record_number BIGINT
);

CREATE TABLE service_history3(
	service_type3 service_type3 PRIMARY KEY,
	last_record_number BIGINT
);
