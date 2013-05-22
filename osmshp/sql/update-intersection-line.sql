/* line {region.code} chunk {chunk_no} of {chunk_count} */

DROP TABLE IF EXISTS tmp_intersection_line;

CREATE TEMP TABLE tmp_intersection_line AS
SELECT tab AS tab, sub.osm_id AS osm_id, sub.ver AS ver, sub.region_id,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) OR geom IS NULL THEN false ELSE intersects END,
  buffer,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) THEN NULL ELSE geom END,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) OR NOT intersects THEN NULL ELSE ST_NPoints(geom) END AS f_points,
  CASE WHEN (intersects AND ST_IsEmpty(geom)) OR NOT intersects THEN NULL ELSE ST_Length(geography(geom)) END AS f_length
FROM ( 
    SELECT src.tab, src.osm_id, src.ver AS ver,
      rgn.id AS region_id,
      CASE
        WHEN geom_in IS NOT NULL AND ST_Within(src.geom, rgn.geom_in) THEN true
        WHEN geom_out IS NOT NULL AND NOT ST_Intersects(src.geom, rgn.geom_out) THEN false
        ELSE ST_Intersects(src.geom, rgn.geom)
      END AS intersects,
      CASE
        WHEN geom_in IS NOT NULL AND ST_Within(src.geom, rgn.geom_in) THEN false
        WHEN geom_out IS NOT NULL AND NOT ST_Intersects(src.geom, rgn.geom_out) THEN false
        ELSE true
      END buffer,
      CASE
        WHEN geom_in IS NOT NULL AND ST_Within(src.geom, rgn.geom_in) THEN src.geom
        WHEN geom_out IS NOT NULL AND NOT ST_Intersects(src.geom, rgn.geom_out) THEN NULL
        WHEN ST_Intersects(src.geom, rgn.geom) THEN ST_CollectionExtract(ST_Intersection(src.geom, rgn.geom), 2)
        ELSE NULL
      END geom
    FROM
      (
        SELECT 'line'::plp_enum AS tab, osm_id, ver, way AS geom, is_simple
        FROM osm_line
        WHERE osm_id > 0 AND osm_id % {chunk_count} = {chunk_no} AND (
          {filter_line}
        )
      ) src
      INNER JOIN region rgn ON rgn.geom && src.geom
    WHERE rgn.id = {region.id} AND NOT EXISTS(
      SELECT * FROM intersection_line c WHERE c.tab = src.tab AND c.osm_id = src.osm_id 
      AND c.ver = src.ver AND c.region_id = rgn.id)
  ) sub;


/* NB: Number of affected rows is used to calculate chunk size */
INSERT INTO intersection_line
SELECT * FROM tmp_intersection_line;