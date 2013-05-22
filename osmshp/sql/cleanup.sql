DROP TABLE IF EXISTS intersection_point, intersection_line, intersection_polygon, obj_version CASCADE;

DROP SCHEMA IF EXISTS layer CASCADE;

DROP TABLE IF EXISTS region, dump_version, layer_stat, layer_version, buffer_delete, buffer_insert CASCADE;
DROP TYPE IF EXISTS nwr_enum, plp_enum CASCADE;

DROP TABLE IF EXISTS osm_nodes, osm_ways, osm_rels,
  osm_point, osm_line, osm_polygon, osm_roads;