ALTER TABLE osm_line 
  ADD COLUMN flag boolean NOT NULL DEFAULT true,
  ADD COLUMN ver int NOT NULL DEFAULT 1,
  ADD COLUMN is_simple boolean,
  ADD COLUMN is_closed boolean;

ALTER TABLE osm_line 
  ALTER COLUMN ver SET DEFAULT 0,
  ALTER COLUMN flag SET DEFAULT false;
