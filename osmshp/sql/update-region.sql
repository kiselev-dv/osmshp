DROP TABLE IF EXISTS tmp_region_update;

CREATE TEMP TABLE tmp_region_update AS
SELECT region.id AS region_id,
  eval(region.expression) AS geom_new,
  geom AS geom_curr,
  geom_in,
  geom_out,
  NULL::geometry AS sym_diff,
  ts AS tstamp
FROM region
  LEFT JOIN dump_version ON 1 = 1;

DELETE FROM tmp_region_update
WHERE geom_new IS NULL
  OR NOT ST_IsValid(geom_new)
  OR NOT ST_Within(geom_in, geom_new)
  OR NOT ST_Within(geom_new, geom_out);

UPDATE tmp_region_update SET sym_diff = ST_SymDifference(geom_new, geom_curr);

DELETE FROM intersection_point it
WHERE it.buffer AND EXISTS(
  SELECT true FROM tmp_region_update
  WHERE NOT ST_IsEmpty(sym_diff)
    AND tmp_region_update.region_id = it.region_id
    AND ST_Intersects(sym_diff, it.geom)
);

DELETE FROM intersection_line it
WHERE it.buffer AND EXISTS(
  SELECT true FROM tmp_region_update
  WHERE NOT ST_IsEmpty(sym_diff)
    AND tmp_region_update.region_id = it.region_id
    AND ST_Intersects(sym_diff, it.geom)
);

DELETE FROM intersection_polygon it
WHERE it.buffer AND EXISTS(
  SELECT true FROM tmp_region_update
  WHERE NOT ST_IsEmpty(sym_diff)
    AND tmp_region_update.region_id = it.region_id AND ST_Intersects(sym_diff, it.geom));

UPDATE region SET
  geom = src.geom_new,
  geom_tstamp = src.tstamp
FROM
  tmp_region_update src
WHERE region.id = src.region_id;