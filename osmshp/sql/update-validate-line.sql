UPDATE osm_line SET 
  is_simple = ST_IsSimple(way), 
  is_closed = ST_IsClosed(way)
WHERE is_simple IS NULL 
  OR is_closed IS NULL;