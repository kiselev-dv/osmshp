from flask import Flask, render_template

from osmshp.models import DBSession, Region, DumpVersion

app = Flask('osmshp')


@app.route('/')
def index():
    return render_template(
        'index.html',
        regions=Region.query().order_by(Region.code),
        version=DumpVersion.query().one(),
        export_url=app.env.config['web'].get('export_url', None)
    )


@app.route('/region/<code>')
def region(code):
    region = Region.filter_by(code=code).one()
    qgeom = DBSession.execute(
        """SELECT
            ST_AsGeoJSON(ST_Difference(geom_out, geom_in )) AS ring,
            ST_AsGeoJSON(geom) AS geom,
            ST_AsGeoJSON(eval(expression)) AS current,
            ST_XMin(geom_out) AS min_x,
            ST_XMax(geom_out) AS max_x,
            ST_YMin(geom_out) AS min_y,
            ST_YMax(geom_out) AS max_y
        FROM region WHERE id = :id""",
        dict(id=region.id)
    ).fetchone()
    return render_template(
        'region.html',
        region=region,
        qgeom=qgeom,
        version=DumpVersion.query().one(),
        export_url=app.env.config['web'].get('export_url', None),
    )


@app.route('/layer/')
def layers():
    return render_template(
        'layers.html',
        layers=app.env.layers
    )


if __name__ == '__main__':
    from osmshp import Env
    app.env = Env()
    app.run(host='0.0.0.0', debug=True)
