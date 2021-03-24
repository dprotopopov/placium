CREATE UNIQUE INDEX ON place (osm_id,osm_type);
ALTER TABLE node ADD PRIMARY KEY (id);
ALTER TABLE way ADD PRIMARY KEY (id);
ALTER TABLE relation ADD PRIMARY KEY (id);

CREATE INDEX ON place (record_number);
CREATE INDEX ON node (record_number);
CREATE INDEX ON way (record_number);
CREATE INDEX ON relation (record_number);

CREATE INDEX ON place USING HASH (tags);
CREATE INDEX ON node USING HASH (tags);
CREATE INDEX ON way USING HASH (tags);
CREATE INDEX ON relation USING HASH (tags);

CREATE INDEX ON way USING HASH (nodes);
CREATE INDEX ON relation USING GIN (members);
