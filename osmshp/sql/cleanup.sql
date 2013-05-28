DROP TABLE IF EXISTS intersection_point, intersection_line, intersection_polygon, obj_version CASCADE;

DROP TABLE IF EXISTS dump_version, region_group, region, layer_version, layer_stat CASCADE;

DROP FUNCTION IF EXISTS region_clean_itersections();
DROP FUNCTION IF EXISTS relation(bigint);
DROP FUNCTION IF EXISTS eval(text);

DROP SCHEMA IF EXISTS layer CASCADE;

DROP TABLE IF EXISTS buffer_delete, buffer_insert CASCADE;
DROP TYPE IF EXISTS nwr_enum, plp_enum CASCADE;

DROP TABLE IF EXISTS osm_nodes, osm_ways, osm_rels,
  osm_point, osm_line, osm_polygon, osm_roads;