#!/usr/bin/python

from osmshp.models import DBSession
from osmshp import Env
from sqlalchemy import DDL

env = Env('/opt/projects/nextgis/osmshp-gislab/config.yaml')
conn = env.connection

def columnExists(table, column):
    cur = conn.cursor()
    cur.execute("""\
    SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
            AND table_name = '%(table)s'
            AND column_name = '%(column)s'
    """ % {'column': column, 'table': table})
    for row in cur:
        return True
    
    return False

cur = conn.cursor()
cur.execute("""\
CREATE TABLE IF NOT EXISTS ngusers (
    id serial PRIMARY KEY,
    name character varying(100) NOT NULL
)
""")

nguseres = conn.cursor()
nguseres.execute("select count(*) from ngusers where id = 0")
if nguseres.fetchone() == 0:
    conn.cursor().execute("insert into ngusers (id, name) values (0, 'nextgis')")

if not columnExists('region', 'active'):
    conn.cursor().execute("ALTER TABLE region ADD COLUMN active boolean")

if not columnExists('region', '_user'):
    conn.cursor().execute("ALTER TABLE region ADD COLUMN _user integer")

if not columnExists('region', 'build_frequency'):
    conn.cursor().execute("ALTER TABLE region ADD COLUMN build_frequency integer")

if not columnExists('region', 'last_success'):
    conn.cursor().execute("ALTER TABLE region ADD COLUMN last_success timestamp")
    
if columnExists('region', 'active'):
    conn.cursor().execute("update region set active = True")
    conn.cursor().execute("ALTER TABLE region ALTER COLUMN active SET NOT NULL")   

if columnExists('region', '_user'):
    conn.cursor().execute("update region set _user = 0")
    conn.cursor().execute("ALTER TABLE region ALTER COLUMN _user SET NOT NULL") 
    conn.cursor().execute("ALTER TABLE region DROP CONSTRAINT IF EXISTS region_user")  
    conn.cursor().execute("ALTER TABLE region ADD CONSTRAINT region_user FOREIGN KEY (_user) REFERENCES ngusers (id)")   

if columnExists('region', 'build_frequency'):        
    conn.cursor().execute("update region set build_frequency = 7")
    conn.cursor().execute("ALTER TABLE region ALTER COLUMN build_frequency SET NOT NULL")   

conn.commit()

print "successfull"