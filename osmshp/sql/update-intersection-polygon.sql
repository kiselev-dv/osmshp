/* polygon {region.code} chunk {chunk_no} of {chunk_count} */

DROP TABLE IF EXISTS tmp_intersection_polygon;

CREATE TEMP TABLE tmp_intersection_polygon AS
SELECT tab AS tab, sub.osm_id AS osm_id, sub.ver AS ver, sub.region_id,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) OR geom IS NULL THEN false ELSE intersects END,
  buffer,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) THEN NULL ELSE geom END,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) THEN NULL ELSE ST_NPoints(geom) END AS f_points,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) THEN NULL ELSE ST_Perimeter(geography(geom)) END AS g_length,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) THEN NULL ELSE ST_Area(geography(geom)) END AS g_area
FROM ( 
  SELECT src.tab, src.osm_id, src.ver AS ver,
    rgn.id AS region_id,
    CASE
      WHEN NOT src.is_valid THEN false
      WHEN (geom_in IS NOT NULL AND ST_Within(src.geom, rgn.geom_in)) THEN true
      WHEN (geom_out IS NOT NULL AND NOT ST_Intersects(src.geom, rgn.geom_out)) THEN false
      ELSE ST_Intersects(src.geom, rgn.geom)
    END AS intersects,
    CASE
      WHEN NOT src.is_valid THEN false
      WHEN geom_in IS NOT NULL AND ST_Within(src.geom, rgn.geom_in) THEN false
      WHEN geom_out IS NOT NULL AND NOT ST_Intersects(src.geom, rgn.geom_out) THEN false
      ELSE true
    END buffer,    
    CASE
      WHEN NOT src.is_valid THEN NULL
      WHEN geom_in IS NOT NULL AND ST_Within(src.geom, rgn.geom_in) THEN src.geom
      WHEN geom_out IS NOT NULL AND NOT ST_Intersects(src.geom, rgn.geom_out) THEN NULL
      WHEN ST_Intersects(src.geom, rgn.geom) THEN ST_CollectionExtract(ST_Intersection(src.geom, rgn.geom), 3)
      ELSE NULL
    END geom
  FROM
    (
      SELECT 'polygon'::plp_enum AS tab, osm_id, ver, way AS geom, is_valid
      FROM osm_polygon
      WHERE (osm_id % {chunk_count} = {chunk_no} OR osm_id % {chunk_count} = -{chunk_no}) AND (
        {filter_polygon}
      )
    ) src
    INNER JOIN region rgn ON rgn.geom && src.geom
  WHERE rgn.id = {region.id} AND NOT EXISTS(
      SELECT * FROM intersection_polygon c WHERE c.tab = src.tab AND c.osm_id = src.osm_id 
        AND c.ver = src.ver AND c.region_id = rgn.id)
  )  sub;

/* NB: Number of affected rows is used to calculate chunk size */
INSERT INTO intersection_polygon 
SELECT * FROM tmp_intersection_polygon;