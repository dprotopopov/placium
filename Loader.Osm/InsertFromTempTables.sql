INSERT INTO node(
	id,
	version,
	latitude,
	longitude,
	change_set_id,
	time_stamp,
	user_id,
	user_name,
	visible,
	tags
)
SELECT 
	id,
	version,
	latitude,
	longitude,
	change_set_id,
	time_stamp,
	user_id,
	user_name,
	visible,
	tags
FROM temp_node
ON CONFLICT (id) DO UPDATE SET
	version=EXCLUDED.version,
	latitude=EXCLUDED.latitude,
	longitude=EXCLUDED.longitude,
	change_set_id=EXCLUDED.change_set_id,
	time_stamp=EXCLUDED.time_stamp,
	user_id=EXCLUDED.user_id,
	user_name=EXCLUDED.user_name,
	visible=EXCLUDED.visible,
	tags=EXCLUDED.tags,
	record_number=nextval('record_number_seq');
INSERT INTO way(
	id,
	version,
	change_set_id,
	time_stamp,
	user_id,
	user_name,
	visible,
	tags,
	nodes
)
SELECT 
	id,
	version,
	change_set_id,
	time_stamp,
	user_id,
	user_name,
	visible,
	tags,
	nodes
FROM temp_way
ON CONFLICT (id) DO UPDATE SET
	version=EXCLUDED.version,
	change_set_id=EXCLUDED.change_set_id,
	time_stamp=EXCLUDED.time_stamp,
	user_id=EXCLUDED.user_id,
	user_name=EXCLUDED.user_name,
	visible=EXCLUDED.visible,
	tags=EXCLUDED.tags,
	nodes=EXCLUDED.nodes,
	record_number=nextval('record_number_seq');
INSERT INTO relation(
	id,
	version,
	change_set_id,
	time_stamp,
	user_id,
	user_name,
	visible,
	tags,
	members
)
SELECT 
	id,
	version,
	change_set_id,
	time_stamp,
	user_id,
	user_name,
	visible,
	tags,
	members
FROM temp_relation
ON CONFLICT (id) DO UPDATE SET
	version=EXCLUDED.version,
	change_set_id=EXCLUDED.change_set_id,
	time_stamp=EXCLUDED.time_stamp,
	user_id=EXCLUDED.user_id,
	user_name=EXCLUDED.user_name,
	visible=EXCLUDED.visible,
	tags=EXCLUDED.tags,
	members=EXCLUDED.members,
	record_number=nextval('record_number_seq');

INSERT INTO place(osm_id,osm_type,tags) SELECT id,'relation',tags FROM temp_relation WHERE tags IS NOT NULL AND array_length(akeys(tags),1)>0 ON CONFLICT (osm_id,osm_type) DO UPDATE SET tags=EXCLUDED.tags,record_number=nextval('record_number_seq');
INSERT INTO place(osm_id,osm_type,tags) SELECT id,'way',tags FROM temp_way WHERE tags IS NOT NULL AND array_length(akeys(tags),1)>0 ON CONFLICT (osm_id,osm_type) DO UPDATE SET tags=EXCLUDED.tags,record_number=nextval('record_number_seq');
INSERT INTO place(osm_id,osm_type,tags) SELECT id,'node',tags FROM temp_node WHERE tags IS NOT NULL AND array_length(akeys(tags),1)>0 ON CONFLICT (osm_id,osm_type) DO UPDATE SET tags=EXCLUDED.tags,record_number=nextval('record_number_seq');

DROP TABLE temp_node;
DROP TABLE temp_way;
DROP TABLE temp_relation;
