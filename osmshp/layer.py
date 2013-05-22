import logging
import re

from .sql import dedent, indent

_logger = logging.getLogger(__name__)


def _mk_name(yaml, default):
    return yaml['n'] if 'n' in yaml else default


def _mk_type(yaml):
    return yaml['type']


def _mk_filter(yaml):
    if 'filter' in yaml:
        return yaml['filter']
    elif 'f' in yaml:
        return yaml['f']
    else:
        return 'True = True'


class Layer:

    def __init__(self, id, yaml, fieldmap=None):
        self.id = id
        self.type = _mk_type(yaml)
        self.name = _mk_name(yaml, id)
        self.filter = _mk_filter(yaml)
        self.fields = [
            Field(fdef, fieldmap=fieldmap)
            for fdef in yaml['fields']
        ]

        self.classification = {}
        if 'classification' in yaml:
            for cname, cdef in yaml['classification'].iteritems():
                self.classification[cname] = Classification(cname, cdef)

    def sql_source(self, expand_tags):
        tabfilter = []

        if self.type == 'line':
            tabfilter.append('osm_id > 0')

        if self.type == 'polygon':
            tabfilter.append('is_valid')

        if len(tabfilter) == 0:
            tabfilter = ['true', ]

        return dedent("""
            SELECT '%(type)s'::plp_enum AS tab, * FROM osm_%(type)s
            WHERE %(tabfilter)s AND (
              %(filter)s
            ) """) % dict(
            type=self.type,
            filter=indent(expand_tags(self.filter), 2),
            tabfilter=' AND '.join(tabfilter)
        )

    def sql_stat(self, expand_tags):
        criteria_join = []
        criteria_class = []

        for cname, criteria in self.classification.iteritems():
            criteria_join.append("OR (criteria = '%s' AND %s)" % (
                cname, expand_tags(criteria.filter)))

            class_case = []
            for cls in criteria.classes:
                clsname = cls.id
                class_case.append("WHEN %s THEN '%s'" % (
                    expand_tags(cls.filter), clsname))

            criteria_class.append(dedent("""
                WHEN criteria = '%s' THEN
                CASE
                  %s
                END """) % (cname, '\n  '.join(class_case)))

        params = {
            'layer': self,
            'criteria_list': ', '.join(
                ['NULL', ] + ["'%s'" % cname for cname in self.classification]
            ),
            'criteria_join': indent('\n'.join(criteria_join), 3),
            'criteria_class': indent('\n'.join(criteria_class), 4),
            'filter': expand_tags(self.filter),
            'source': indent(self.sql_source(expand_tags), 3)
        }

        agg = ['COUNT(osm_id) AS f_count', ]
        agg_f = ['src.osm_id', ]

        if self.type in ('line', 'polygon'):
            agg.extend([
                'SUM(f_points) AS f_points',
                'SUM(f_length) AS f_length'
            ])
            agg_f.extend(['f_points', 'f_length'])
        else:
            agg.extend(['NULL AS f_points', 'NULL AS f_length'])

        if self.type in ('polygon', ):
            agg.append('SUM(f_area) AS f_area')
            agg_f.append('f_area')
        else:
            agg.append('NULL AS f_area')

        params['agg'] = indent(', \n'.join(agg), 1)
        params['agg_f'] = indent(', '.join(agg_f), 2)

        return dedent("""
            /* {layer.id} */
            DROP TABLE IF EXISTS tmp_layer_stat;

            CREATE TEMP TABLE tmp_layer_stat AS
            SELECT * FROM layer_stat LIMIT 0;

            INSERT INTO tmp_layer_stat
            SELECT region_id, layer,
              (SELECT ts FROM dump_version LIMIT 1) AS ts,
              criteria, cls,
              {agg}
            FROM (
              SELECT
                region_id,
                '{layer.id}'::text AS layer,
                COALESCE(criteria, '') AS criteria,
                COALESCE(CASE
                  WHEN criteria IS NULL THEN NULL
                  {criteria_class}
                END, '') AS cls,
                {agg_f}
              FROM
                (SELECT id, geom FROM region) region
                INNER JOIN (
                  {source}
                ) src ON region.geom && src.way
                LEFT JOIN
                  (SELECT unnest(ARRAY[{criteria_list}]) AS criteria)
                  classification ON (criteria IS NULL)
                    {criteria_join}
                INNER JOIN intersection_{layer.type} ck ON
                    region.id = ck.region_id
                  AND src.tab = ck.tab
                  AND src.osm_id = ck.osm_id
                  AND src.ver = ck.ver
                  AND ck.intersects
            ) sub
            GROUP BY region_id, layer, criteria, cls;

            DELETE FROM layer_stat
            WHERE ts = (SELECT ts FROM dump_version LIMIT 1)
              AND layer_id = '{layer.id}'
              AND EXISTS(
                SELECT * FROM region
                WHERE layer_stat.region_id = region.id
            );

            INSERT INTO layer_stat
            SELECT * FROM tmp_layer_stat; """).format(**params)

    def sql_update_layer(self, region, expand_tags, drop=True):
        sql = ['/* %s %s */' % (region.code, self.id)]

        params = {
            'layer': self,
            'region': region,
            'table': '"%s %s"' % (region.code, self.id),
            'fields': ', '.join([
                '%s AS "%s"' % (expand_tags(f.definition), f.name)
                for f in self.fields
            ]) + ', ',
            'field_names': ', '.join([
                '"%s"' % f.name for f in self.fields
            ]) + ', ',
            'geom_type': {
                'point': 'POINT',
                'line': 'MULTILINESTRING',
                'polygon': 'MULTIPOLYGON'
            }[self.type],
            'source': indent(self.sql_source(expand_tags), 3),
            'force_multi': '' if self.type == 'point' else 'ST_Multi'
        }

        if drop:
            sql.append("DROP TABLE IF EXISTS layer.{table};")

        sql.append(dedent("""
          CREATE TABLE layer.{table} WITH OIDS AS
          SELECT
            0::bigint AS osm_id,
            {fields}
            NULL::geometry({geom_type}, 4326, 2) AS geom
          FROM osm_{layer.type}
          LIMIT 0;

          CREATE INDEX "{region.code} {layer.id} geom_idx"
            ON layer.{table} USING gist (geom);

          GRANT SELECT ON TABLE layer.{table} TO public;"""))

        sql.append(dedent("""
          INSERT INTO layer.{table} (osm_id, {field_names} geom)
            SELECT source.osm_id,
               {fields}
               {force_multi}(ST_Force_2D(ck.geom)) AS geom
            FROM
              (SELECT id, geom FROM region WHERE id = {region.id}) region
              INNER JOIN (
                {source}
              ) source ON source.way && region.geom
              INNER JOIN intersection_{layer.type} ck ON
                  ck.region_id = region.id
                AND source.tab = ck.tab
                AND source.osm_id = ck.osm_id
                AND source.ver = ck.ver
                AND ck.intersects
            ORDER BY ck.geom; """))

        sql.append(dedent("""
            DELETE FROM layer_version
            WHERE region_id={region.id} AND layer_id = '{layer.id}'; """))

        sql.append(dedent("""
            INSERT INTO layer_version (region_id, layer_id, ts, row_count)
            SELECT
              '{region.id}',
              '{layer.id}',
              ts,
              (SELECT COUNT(*) FROM layer.{table})
            FROM dump_version; """))

        return '\n'.join(sql).format(**params) + '\n\n'


class Classification:

    def __init__(self, id, yaml):
        self.id = id
        self.name = _mk_name(yaml, id)
        self.filter = _mk_filter(yaml)
        self.classes = []

        for cdef in yaml['classes'] if 'classes' in yaml else {}:
            ckey = cdef['k']
            self.classes.append(ClassificationClass(ckey, cdef))


class ClassificationClass:

    def __init__(self, id, yaml):
        self.id = id
        self.name = _mk_name(yaml, id)
        self.filter = _mk_filter(yaml)


class Field:

    def __init__(self, yaml, fieldmap=None):
        if isinstance(yaml, basestring):
            self.definition = yaml
            m = re.match('\<([\w\:\_]+)\>', yaml)
            if m:
                if fieldmap and m.group(1) in fieldmap:
                    self.name = fieldmap[m.group(1)].upper()
                else:
                    self.name = m.group(1).upper().replace(':', '_')
            else:
                self.name = yaml.replace(':', '_')
        if len(self.name) > 10:
            _logger.warning(
                "Field name '%s' longer than 10 characters!",
                self.name
            )
