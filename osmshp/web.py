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


@app.route('/layers/')
def layers():
    return render_template(
        'layers.html',
        layers=app.env.layers
    )


if __name__ == '__main__':
    from osmshp import Env
    app.env = Env()
    app.run(host='0.0.0.0', debug=True)
