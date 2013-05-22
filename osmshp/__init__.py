import os
import os.path
import shlex
import shutil
import subprocess
import re
import tempfile
from datetime import datetime
import logging
import logging.config
import yaml
from xml.etree.ElementTree import ElementTree
from importlib import import_module
from multiprocessing.pool import ThreadPool

from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text

import psycopg2
import psycopg2.extensions

from .layer import Layer
from .sql import SqlStatement
from .models import Base, DBSession, DumpVersion, RegionGroup, Region, LayerVersion


def _sql_template(name, data=None, log=None):
    if data is None:
        data = dict()
    if log is None:
        log = 'SQL %s' % name

    filename = os.path.join(os.path.split(__file__)[0], 'sql', name + '.sql')
    with open(filename, 'r') as fd:
        template = fd.read()

    sql = template.format(**data)

    return SqlStatement(sql=sql, log=log)


class YAMLLoader(yaml.Loader):

    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(YAMLLoader, self).__init__(stream)

    def include(self, node):
        filename = self.construct_scalar(node)
        if not filename.startswith('/'):
            filename = os.path.join(self._root, filename)
        with open(filename, 'r') as fp:
            return yaml.load(fp, YAMLLoader)

    def path(self, node):
        filename = self.construct_scalar(node)
        if not filename.startswith('/'):
            filename = os.path.abspath(os.path.join(self._root, filename))
        return filename


YAMLLoader.add_constructor('!include', YAMLLoader.include)
YAMLLoader.add_constructor('!path', YAMLLoader.path)


class Env(object):

    def __init__(self, config, configure_logging=True):
        self._logger = None

        with open(config, 'r') as fp:
            self.config = yaml.load(fp, YAMLLoader)

        # Default settings
        if not 'database' in self.config:
            self.config['database'] = dict()

        database = self.config['database']
        database['host'] = database.get('host', 'localhost')
        database['user'] = database.get('user', 'osmshp')
        database['name'] = database.get('name', 'osmshp')

        if not 'export' in self.config:
            self.config['export'] = dict()

        export = self.config['export']
        export['pgsql2shp'] = 'pgsql2shp'
        export['shptree'] = 'shptree'
        export['7z'] = '7z'

        if not 'options' in self.config:
            self.config['options'] = dict()

        options = self.config['options']
        options['worker_count'] = int(options.get('worker_count', 4))
        options['chunk_size'] = int(options.get('chunk_size', 10000))

        if not 'logging' in self.config:
            self.config['logging'] = dict(
                version=1,
                formatters={
                    'simple': {
                        'format': '%(asctime)s - %(levelname)s - %(threadName)s: %(message)s',
                        'datefmt': '%Y/%m/%d %H:%M:%S'
                    }
                },
                handlers={
                    'console': {
                        'class': 'logging.StreamHandler',
                        'formatter': 'simple',
                        'level': 'DEBUG',
                        'stream': 'ext://sys.stderr'
                    }
                },
                root={
                    'level': 'INFO',
                    'handlers': ['console', ]
                }
            )

        if configure_logging:
            logging.config.dictConfig(self.config['logging'])

        self.layers = {}
        for lname, ldef in self.config['layers'].iteritems():
            self.layers[lname] = Layer(lname, ldef, self.config['fieldmap'])

        # Datasource setup
        self.dsoptions = self.config['datasource']
        self.dsmod = import_module(self.dsoptions['driver'])

        # SQLAclhemy engine initialization
        self.engine = create_engine(
            'postgresql://%(user)s%(password)s@%(host)s/%(name)s' % dict(
                self.config['database'],
                password=(':' + self.config['database']['password'])
                if 'password' in self.config['database'] else ''
            )
        )

        DBSession.configure(bind=self.engine)
        Base.metadata.bind = self.engine

        # Default DB-API connection
        self.connection = self.get_connection()

    @property
    def logger(self):
        if not self._logger:
            self._logger = logging.getLogger('osmshp')

        return self._logger

    def initialize(self):
        self.logger.info("Initializing database")
        Base.metadata.create_all()
        self.execute_sql(_sql_template('initialize'))

    def cleanup(self):
        self.logger.info("Cleaning database")
        self.execute_sql(_sql_template('cleanup'))
        Base.metadata.drop_all()

    def load(self, version=None):
        dump, dump_version = self.dsmod.dump(version=version, options=self.dsoptions)
        self.logger.info('Loading dump from %s (%s)', dump.name, dump_version)

        for v in DumpVersion.query():
            v.delete()

        self.commit()

        osm2pgsql = self._osm2pgsql() + ['--slim', '--create', dump.name]
        self.logger.debug('%s', ' '.join(osm2pgsql))

        if 'log' in self.config['osm2pgsql']:
            log = open(self.config['osm2pgsql']['log'], 'w')
        else:
            log = open(os.devnull, 'w')
        subprocess.check_call(osm2pgsql, stdout=log, stderr=log)

        self.post_load()

        DumpVersion(ts=dump_version, ready=False).add()
        self.commit()

        self.post_update()

        self.logger.info('Dump loaded')

        # Prevent temporary file deletion
        dump.close()

        return dump_version

    def update(self):
        version = DumpVersion.query().one()
        data = self.dsmod.diff(version.ts, self.dsoptions)

        if data is None:
            return None

        diff, diff_version = data

        osm2pgsql = self._osm2pgsql() + ['--slim', '--append', diff.name]

        self.logger.info('Updating database using %s to version %s...', diff.name, diff_version)
        self.logger.debug('%s', ' '.join(osm2pgsql))

        if 'log' in self.config['osm2pgsql']:
            log = open(self.config['osm2pgsql']['log'], 'w')
        else:
            log = open(os.devnull, 'w')
        subprocess.check_call(osm2pgsql, stdout=log, stderr=log)

        version.ts = diff_version
        version.ready = False
        self.commit()

        self.logger.info('Database updated.')

        self.post_update()

        # Prevent temporary file deletion
        diff.close()

        return version.ts

    def forward(self, to_version=None, update_stat=True):
        while True:
            version = DumpVersion.query().one()
            if to_version is not None and version.ts >= to_version:
                return version.ts

            if self.update() is None:
                return version.ts

            if update_stat:
                self.update_stat()

    def commit(self):
        DBSession.commit()

    def get_tag_columns(self):
        if not hasattr(self, 'tag_columns'):
            curr = self.connection.cursor()
            curr.execute("""SELECT column_name FROM information_schema.columns
                         WHERE table_name = 'osm_point' AND NOT column_name IN ('way', 'osm_id', 'ver', 'flag')
                         ORDER BY ordinal_position;""")
            self.tag_columns = [c for (c, ) in curr]

        return self.tag_columns

    def expand_tag_columns(self, sql):
        self.get_tag_columns()

        def repl(m):
            if m.group(1) in self.tag_columns:
                return '"%s"' % m.group(1)
            else:
                return "tags->'%s'" % m.group(1)
        return re.sub('\<([\w\:\_]+)\>', repl, sql)

    def get_connection(self):
        params = dict()
        for k, v in self.config['database'].iteritems():
            if k == 'name':
                k = 'database'
            params[k] = v

        conn = psycopg2.connect(**params)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def post_load(self):
        self.logger.info('Starting post-load operations.')

        self.execute_queries([
            _sql_template('load-point'),
            _sql_template('load-line'),
            _sql_template('load-polygon')
        ])

        self.execute_queries([
            _sql_template('load-version'),
            _sql_template('load-intersection')
        ])

        self.logger.info('Post-load operations completed.')

    def post_update(self):
        self.logger.info('Starting post-update operations...')

        version = DumpVersion().query().one()
        self.get_tag_columns()

        context = {
            'version_timestamp': version.ts,
            'filter_point': ' OR '.join(['(%s)' % self.expand_tag_columns(l.filter) for l in self.layers.itervalues() if l.type == 'point']),
            'filter_line': ' OR '.join(['(%s)' % self.expand_tag_columns(l.filter) for l in self.layers.itervalues() if l.type == 'line']),
            'filter_polygon': ' OR '.join(['(%s)' % self.expand_tag_columns(l.filter) for l in self.layers.itervalues() if l.type == 'polygon']),
        }

        self.logger.debug("Point SQL filter:\n%s", context['filter_point'])
        self.logger.debug("Line SQL filter:\n%s", context['filter_line'])
        self.logger.debug("Polygon SQL filter:\n%s", context['filter_polygon'])

        self.execute_sql(_sql_template('update-region'))

        self.execute_queries([
            _sql_template('update-validate-line'),
            _sql_template('update-validate-polygon')
        ])

        self.execute_queries([
            _sql_template('update-version-point', context),
            _sql_template('update-version-line', context),
            _sql_template('update-version-polygon', context)
        ])

        regions = Region.query().all()
        cc_queries = []
        cc_keys = []

        for region in regions:
            for objtype in ('polygon', 'point', 'line'):
                chunk_size = self.config['options']['chunk_size']

                subcontext = dict(
                    context,
                    region=region,
                    chunk_no=0,
                    chunk_count=chunk_size
                )

                cc_keys.append((region.id, objtype))
                cc_queries.append(_sql_template(
                    'update-intersection-%s' % objtype,
                    data=subcontext,
                    log="Chunk size calculation table=%s; region=%s; chunk_size=%d" % (objtype, region.code, chunk_size)
                ))

        chunk_counts = dict(zip(cc_keys, self.execute_queries(cc_queries)))

        queries = []
        query_no = 0

        for region in regions:
            for objtype in ('polygon', 'point', 'line'):
                chunk_count = max(1, chunk_counts[(region.id, objtype)])

                for chunk_no in range(chunk_count):
                    query_no += 1

                    subcontext = dict(
                        context,
                        region=region,
                        chunk_no=chunk_no,
                        chunk_count=chunk_count
                    )

                    queries.append(_sql_template(
                        'update-intersection-%s' % objtype,
                        data=subcontext,
                        log="Geometry intersections #%d table=%s; region=%s; chunk=%d/%d" % (query_no, objtype, region.code, chunk_no+1, chunk_count))
                    )

        self.execute_queries(queries)

        self.execute_queries([
            _sql_template('update-flag-point'),
            _sql_template('update-flag-line'),
            _sql_template('update-flag-polygon')
        ])

        DumpVersion().query().one().ready = True
        self.commit()

        self.logger.info('Post-update operations completed.')

    def update_layers(self):
        self.logger.info('Updating layers...')
        version = DumpVersion().query().one()

        self.get_tag_columns()

        queries = []
        for lid, lobj in self.layers.iteritems():
            for region in Region.query():
                layer_version = LayerVersion.filter_by(
                    region_id=region.id, layer_id=lid
                ).first()
                if layer_version is None or layer_version.ts < version.ts:
                    queries.append(SqlStatement(
                        sql=lobj.sql_update_layer(region, self.expand_tag_columns),
                        log="Update layer=%s region=%s" % (lid, region.code)
                    ))

        self.execute_queries(queries)

    def _osm2pgsql(self):
        f = [self.config['osm2pgsql']['path'] if 'path' in self.config['osm2pgsql'] else 'osm2pgsql', ] + \
            shlex.split(self.config['osm2pgsql']['opts']) if 'opts' in self.config['osm2pgsql'] else []
        f.extend(['--host', self.config['database']['host']])
        f.extend(['--database', self.config['database']['name']])
        f.extend(['--username', self.config['database']['user']])
        f.extend(['--style', self.config['osm2pgsql']['style']])
        f.extend(['--output', 'pgsql'])
        f.extend(['--hstore', '--multi-geometry', '--latlong', '--prefix', 'osm'])
        return f

    def update_stat(self):
        tstamp = DumpVersion.query().one().ts
        self.logger.info('Updating statistics for %s ...', tstamp)

        queries = []
        for l_id, l_obj in self.layers.iteritems():
            queries.append(SqlStatement(
                sql=l_obj.sql_stat(self.expand_tag_columns),
                log="Update stat layer=%s" % l_id
            ))

        self.execute_queries(queries)

        self.logger.info('Statistics updated.')

    def export(self):
        version = DumpVersion().query().one()
        devnull = open(os.devnull, 'w')

        for region in Region.query():
            tmpdir = tempfile.mkdtemp()

            datadir = os.path.join(tmpdir, 'data')
            os.mkdir(datadir)

            self.logger.info('Region [%s]: exporting to %s...', region.code, datadir)
            files = [datadir, ]

            for l_id, b_obj in self.layers.iteritems():
                cur = self.connection.cursor()
                cur.execute('SELECT COUNT(*) FROM layer."%s %s"' % (region.code, l_id))
                (feature_count, ) = cur.fetchone()

                if feature_count == 0:
                    self.logger.info('Layer [%s %s] is empty.', region.code, l_id)
                    continue

                self.logger.info('Layer [%s %s]: exporting %d features...', region.code, l_id, feature_count)
                filename = os.path.join(datadir, l_id)
                args = [self.config['export']['pgsql2shp'], ] \
                        + ['-h', self.config['database']['host'], '-u', self.config['database']['user']] \
                        + ['-f', filename, '-b'] \
                        + [self.config['database']['name'], '%s.%s %s' % ('layer', region.code, l_id)]

                subprocess.check_call(args, stdout=devnull, stderr=devnull)

                if 'shptree' in self.config['export']:
                    self.logger.info('Layer [%s %s]: building index using shptree...', region.code, l_id)
                    subprocess.check_call([self.config['export']['shptree'], filename], stdout=devnull, stderr=devnull)

                # dbf unicode file patch
                with open(filename + '.dbf', 'r+b') as fd:
                    fd.seek(0x1D)
                    fd.write('\x00')

                # cpg file for arcgis
                cpgfile = filename + '.cpg'
                with open(cpgfile, 'w') as fd:
                    fd.write('65001')
                
            if 'readme' in self.config['export']:
                readme = os.path.split(self.config['export']['readme'])[1]
                shutil.copy(
                    self.config['export']['readme'],
                    os.path.join(tmpdir, readme)
                )
                files.append(readme)

            # qgis projects
            if 'qgis_projects' in self.config['export']:
                self.logger.info('Region [%s]: building QGIS projects...', region.code)

                curr = self.connection.cursor()
                curr.execute("""
                    SELECT
                        ST_XMin(ST_Transform(geom_out, 3857)),
                        ST_XMax(ST_Transform(geom_out, 3857)),
                        ST_YMin(ST_Transform(geom_out, 3857)),
                        ST_YMax(ST_Transform(geom_out, 3857))
                        FROM region WHERE id = %d
                """ % region.id)

                (xmin, xmax, ymin, ymax) = curr.fetchone()

                self.tag_columns = [c for (c, ) in curr]
                for proj_template in self.config['export']['qgis_projects']:
                    et = ElementTree()
                    et.parse(proj_template)
                    
                    et.find('mapcanvas/extent/xmin').text = str(xmin)
                    et.find('mapcanvas/extent/xmax').text = str(xmax)
                    et.find('mapcanvas/extent/ymin').text = str(ymin)
                    et.find('mapcanvas/extent/ymax').text = str(ymax)

                    target_file = os.path.join(tmpdir, os.path.split(proj_template)[1])
                    et.write(target_file)
                    files.append(os.path.split(proj_template)[1])

                # include_dirs
                if 'include_dirs' in self.config['export']:
                    self.logger.info('Region [%s]: copying include_dirs...', region.code)
                    for idir in self.config['export']['include_dirs']:
                        shutil.copytree(idir, os.path.join(tmpdir, os.path.split(idir)[1]))
                        files.append(os.path.split(idir)[1])

            self.logger.info('Region [%s]: compressing...', region.code)

            current_name = os.path.join(
                self.config['export']['path'], region.code,
                region.code + '-' + version.ts.strftime('%y%m%d') + '.7z')

            latest_name = os.path.join(
                self.config['export']['path'],
                'latest', region.code + '.7z')

            current_path = os.path.split(current_name)[0]
            if not os.path.exists(current_path):
                os.makedirs(current_path)

            latest_path = os.path.split(latest_name)[0]
            if not os.path.exists(latest_path):
                os.makedirs(latest_path)

            os.chdir(tmpdir)
            subprocess.check_call([
                self.config['export']['7z'], 'a',
                os.path.join(
                    self.config['export']['path'], region.code,
                    region.code + '-' + version.ts.strftime('%y%m%d') + '.7z'
                )
            ] + files, stdout=devnull, stderr=devnull)

            if os.path.exists(latest_name):
                os.remove(latest_name)
            os.symlink(current_name, latest_name)
            
            shutil.rmtree(tmpdir)

            self.logger.info('Region [%s]: export completed', region.code)

    def _execute_query(self, query, connection):
        if isinstance(query, SqlStatement):
            start = datetime.now()

            sql = query.sql
            result = connection.execute(text(sql))

            end = datetime.now()
            if result.rowcount > 0:
                self.logger.info(query.log + ' (t=%s; rows=%d)' % (end - start, result.rowcount))
            else:
                self.logger.info(query.log + ' (t=%s)' % (end - start))

        else:
            sql = query
            result = connection.execute(text(sql))

        return result.rowcount

    def execute_sql(self, query):
        connection = DBSession.connection()

        try:
            result = self._execute_query(query, connection)
            DBSession.commit()
        finally:
            connection.close()

        return result

    def execute_queries(self, queries):
        def worker(query):
            connection = DBSession.connection()
            try:
                result = self._execute_query(query, connection)
                DBSession.commit()
            finally:
                connection.close()

            return result

        pool = ThreadPool(processes=self.config['options']['worker_count'])
        return pool.map(worker, queries)
