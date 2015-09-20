DROP TABLE IF EXISTS intersection_point, intersection_line, intersection_polygon;

CREATE TABLE intersection_point (
  tab plp_enum,
  osm_id bigint,
  ver int,
  region_id int,
  intersects boolean,
  buffer boolean,
  geom geometry,
  CONSTRAINT intersection_point_pk PRIMARY KEY (tab, osm_id, ver, region_id)
);

CREATE TABLE intersection_line (
  tab plp_enum,
  osm_id bigint,
  ver int,
  region_id int,
  intersects boolean,
  buffer boolean,
  geom geometry,
  f_points int,
  f_length float,
  CONSTRAINT intersection_line_pk PRIMARY KEY (tab, osm_id, ver, region_id)
);

CREATE TABLE intersection_polygon (
  tab plp_enum,
  osm_id bigint,
  ver int,
  region_id int,
  intersects boolean,
  buffer boolean,
  geom geometry,
  f_points int,
  f_length float,
  f_area float,
  CONSTRAINT intersection_polygon_pk PRIMARY KEY (tab, osm_id, ver, region_id)
);

DROP TRIGGER IF EXISTS region_clean_itersections ON region;

CREATE TRIGGER region_clean_itersections
  AFTER UPDATE ON region FOR EACH ROW
  EXECUTE PROCEDURE region_clean_itersections();