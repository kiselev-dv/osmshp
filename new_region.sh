#!/bin/sh

wkt=$1

name="RU-SVE-TEST"
simpl_buf=0.2
simpl_dp_scale=0.9

sql="insert into region (code, name, region_group_id, simpl_buf, simpl_dp, geom, geom_in, geom_out, geom_tstamp, reference_tstamp, expression) values ('$name', '$name', 0, $simpl_buf, 0.01, ST_SetSRID(ST_Multi(ST_GeomFromText('$wkt')), 4326), ST_SetSRID(ST_GeomFromText('MULTIPOLYGON EMPTY'), 4326), ST_SetSRID(ST_GeomFromText('MULTIPOLYGON EMPTY'), 4326), now(), now(), 'custom($name)');"

psql -U osmshp  -h localhost -d osmshp-dkiselev -c "$sql"

sql_update="update region r set geom_in=ST_Multi(ST_Buffer(ST_SimplifyPreserveTopology(ST_Buffer(r.geom, -r.simpl_buf), r.simpl_dp), 0)), geom_out=ST_Multi(ST_Buffer(ST_SimplifyPreserveTopology(ST_Buffer(r.geom, r.simpl_buf), r.simpl_dp), 0)) where code='$name';"

psql -U osmshp  -h localhost -d osmshp-dkiselev -c "$sql_update"
