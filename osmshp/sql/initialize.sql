CREATE TYPE plp_enum AS ENUM('point', 'line', 'polygon');
CREATE TYPE nwr_enum AS ENUM('n', 'w', 'r');


CREATE TABLE buffer_delete (
  tab plp_enum,
  osm_id bigint
);


CREATE TABLE buffer_insert (
  tab plp_enum,
  osm_id bigint
); 


CREATE SCHEMA layer;
GRANT USAGE ON SCHEMA layer TO public;


CREATE OR REPLACE FUNCTION relation(id bigint) RETURNS geometry AS $$
DECLARE
  result geometry;
BEGIN
  EXECUTE 'SELECT ST_Multi(way) FROM osm_polygon WHERE osm_id = ' || quote_literal(-$1)
    INTO result;

  RETURN result;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION eval(expression text) RETURNS geometry AS $$
DECLARE
  result geometry;
BEGIN
  BEGIN
    EXECUTE 'SELECT ST_Multi(' || expression || ')' INTO result;
    RETURN result;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
END
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION region_clean_itersections() RETURNS TRIGGER AS $$
BEGIN
  IF NOT ST_Equals(NEW.geom_in, OLD.geom_in) OR NOT ST_Equals(NEW.geom_out, OLD.geom_out) THEN
    DELETE FROM intersection_point WHERE region_id = NEW.id;
    DELETE FROM intersection_line WHERE region_id = NEW.id;
    DELETE FROM intersection_polygon WHERE region_id = NEW.id;
  END IF;

  RETURN NEW;
END
$$ LANGUAGE plpgsql;