import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy import (
    GeometryDDL,
    GeometryColumn,
    MultiPolygon,
    WKTSpatialElement,
)

DBSession = orm.scoped_session(orm.sessionmaker())


class BaseClass(object):

    def add(self):
        DBSession.add(self)

    def delete(self):
        DBSession.delete(self)

    @classmethod
    def query(cls):
        return DBSession.query(cls)

    @classmethod
    def filter_by(cls, **kwargs):
        return cls.query().filter_by(**kwargs)

    @classmethod
    def filter(cls, *args):
        return cls.query().filter(*args)


Base = declarative_base(cls=BaseClass)


class DumpVersion(Base):
    __tablename__ = 'dump_version'
    ts = sa.Column(sa.DateTime, primary_key=True, nullable=False)
    ready = sa.Column(sa.Boolean, nullable=False, default=False)


class RegionGroup(Base):
    __tablename__ = 'region_group'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Unicode(100))
    sort = sa.Column(sa.Integer)


class Region(Base):
    __tablename__ = 'region'
    id = sa.Column(sa.Integer, primary_key=True)
    code = sa.Column(sa.Unicode(25), nullable=False, unique=True)
    name = sa.Column(sa.Unicode(100), nullable=False)
    expression = sa.Column(sa.Unicode, nullable=False)
    simpl_buf = sa.Column(sa.Float, nullable=False)
    simpl_dp = sa.Column(sa.Float, nullable=False)
    geom = GeometryColumn(MultiPolygon(2), nullable=False)
    geom_in = GeometryColumn(MultiPolygon(2), nullable=False)
    geom_out = GeometryColumn(MultiPolygon(2), nullable=False)
    geom_tstamp = sa.Column(sa.DateTime, nullable=False)
    reference_tstamp = sa.Column(sa.DateTime, nullable=False)
    region_group_id = sa.Column(sa.Integer, sa.ForeignKey('region_group.id'))

    def __init__(self, **kwargs):
        kwargs['simpl_buf'] = kwargs.get('simpl_buf', 0.2)
        kwargs['simpl_dp'] = kwargs.get('simpl_dp', 0.9 * kwargs['simpl_buf'])

        super(Region, self).__init__(**kwargs)
        self.update_geom()

    def update_geom(self):
        conn = DBSession.connection()
        result = conn.execute(
            """ SELECT
                ST_AsText(ST_Multi(%(expr)s)) AS geom,
                ST_AsText(ST_Multi(ST_Buffer(ST_SimplifyPreserveTopology(ST_Buffer(%(expr)s, -%(simpl_buf)s), %(simpl_dp)s), 0))) AS geom_in,
                ST_AsText(ST_Multi(ST_Buffer(ST_SimplifyPreserveTopology(ST_Buffer(%(expr)s, %(simpl_buf)s), %(simpl_dp)s), 0))) AS geom_out
            """ % dict(expr=self.expression, simpl_buf=self.simpl_buf, simpl_dp=self.simpl_dp)
        )

        for row in result:
            self.geom = WKTSpatialElement(row['geom'])
            self.geom_in = WKTSpatialElement(row['geom_in'])
            self.geom_out = WKTSpatialElement(row['geom_out'])

        self.reference_tstamp = DumpVersion.query().one().ts
        self.geom_tstamp = self.reference_tstamp


GeometryDDL(Region.__table__)


class LayerVersion(Base):
    __tablename__ = 'layer_version'
    region_id = sa.Column(sa.ForeignKey(Region.id), primary_key=True)
    layer_id = sa.Column(sa.Unicode(50), primary_key=True)
    ts = sa.Column(sa.DateTime, nullable=False)
    row_count = sa.Column(sa.Integer, nullable=False)


class LayerStat(Base):
    __tablename__ = 'layer_stat'
    region_id = sa.Column(sa.Integer(), primary_key=True)
    layer_id = sa.Column(sa.Unicode(50), primary_key=True)
    ts = sa.Column(sa.DateTime, primary_key=True)
    criterion = sa.Column(sa.Unicode(20), primary_key=True)
    category = sa.Column(sa.Unicode(20), primary_key=True)
    f_count = sa.Column(sa.Integer)
    f_points = sa.Column(sa.Integer)
    f_length = sa.Column(sa.Float)
    f_area = sa.Column(sa.Float)


def load_region_groups(dbsession):

    region_groups = dbsession.query(RegionGroup.id, RegionGroup.name) \
        .order_by(RegionGroup.sort).all()

    for i in region_groups:
        i.regions = dbsession.query(Region.id, Region.name_ru.label('name')) \
            .filter_by(region_group_id=i.id) \
            .order_by(Region.name_ru).all()

    return region_groups


def initialize_sql(engine):
    DBSession.configure(bind=engine)
    Base.metadata.bind = engine
