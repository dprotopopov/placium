CREATE UNIQUE INDEX ON place (osm_id,osm_type);
ALTER TABLE node ADD PRIMARY KEY (id);
ALTER TABLE way ADD PRIMARY KEY (id);
ALTER TABLE relation ADD PRIMARY KEY (id);

CREATE UNIQUE INDEX ON place (record_number);
CREATE UNIQUE INDEX ON node (record_number);
CREATE UNIQUE INDEX ON way (record_number);
CREATE UNIQUE INDEX ON relation (record_number);
CREATE UNIQUE INDEX ON place (record_id);
CREATE UNIQUE INDEX ON node (record_id);
CREATE UNIQUE INDEX ON way (record_id);
CREATE UNIQUE INDEX ON relation (record_id);
