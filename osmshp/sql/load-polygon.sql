ALTER TABLE osm_polygon 
  ADD COLUMN flag boolean NOT NULL DEFAULT true,
  ADD COLUMN ver int NOT NULL DEFAULT 1,
  ADD COLUMN is_valid boolean;

ALTER TABLE osm_polygon
  ALTER COLUMN ver SET DEFAULT 0,
  ALTER COLUMN flag SET DEFAULT false;
