UPDATE region SET geom = ST_Multi(osm_polygon.way),  actual = True, simpl_buf = ${simpl_buf}, simpl_dp = ${simpl_dp}
FROM osm_polygon
WHERE region.id = '${region_id}' AND osm_polygon.osm_id = -region.relation_id AND ST_IsValid(osm_polygon.way);

UPDATE region SET
  geom_in = ST_Buffer(ST_SimplifyPreserveTopology(ST_Buffer(geom, -simpl_buf), simpl_dp), 0),
  geom_out = ST_Buffer(ST_SimplifyPreserveTopology(ST_Buffer(geom, simpl_buf), simpl_dp), 0)
WHERE region.id = '${region_id}';

DELETE FROM intersection_point WHERE region_id = '${region_id}';
DELETE FROM intersection_line WHERE region_id = '${region_id}';
DELETE FROM intersection_polygon WHERE region_id = '${region_id}';