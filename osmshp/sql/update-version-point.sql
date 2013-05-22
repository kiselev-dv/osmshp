UPDATE obj_version v SET latest = latest + 1
WHERE EXISTS(
  SELECT osm_id FROM osm_point c 
  WHERE v.tab = 'point'::plp_enum
    AND c.osm_id = v.osm_id 
    AND c.flag = false
);

UPDATE osm_point src SET
  ver = obj_version.latest
FROM obj_version
WHERE NOT src.flag AND obj_version.tab = 'point'::plp_enum
  AND obj_version.osm_id = src.osm_id
  AND obj_version.latest <> src.ver;
