from flask import Flask, render_template

from osmshp.models import Region, DumpVersion

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
    return render_template(
        'region.html',
        region=region,
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
