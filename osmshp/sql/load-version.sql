DROP TABLE IF EXISTS obj_version;

CREATE TABLE obj_version (  
  tab plp_enum,
  osm_id bigint, 
  latest int,
  CONSTRAINT obj_version_pk PRIMARY KEY (tab, osm_id)
);

INSERT INTO obj_version
  SELECT DISTINCT 'point'::plp_enum, osm_id, 1 FROM osm_point
UNION ALL
  SELECT DISTINCT 'line'::plp_enum, osm_id, 1 FROM osm_line
UNION ALL
  SELECT DISTINCT 'polygon'::plp_enum, osm_id, 1 FROM osm_polygon;
