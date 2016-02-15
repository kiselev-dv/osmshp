from flask import Flask, render_template, request

from osmshp.models import DBSession, RegionGroup, Region, DumpVersion
import flask

app = Flask('osmshp')


@app.teardown_appcontext
def shutdown_session(exception=None):
    DBSession.remove()


@app.route('/')
def index():
    return render_template(
        'index.html',
        region_groups=RegionGroup.query(),
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


@app.route('/layer/<id>')
def layer(id):
    return render_template(
        'layer.html',
        layer=app.env.layers[id]
    )
    
@app.route('/new_region')
def newRegion():
    """
    Creates new region with given code and geometry in region table 
    """
    
    code = request.args.get('code')
    
    if Region.filter_by(code = code).count() > 0:
        return flask.jsonify({
            'result': 'failed',
            'error': 'already_exists',
            'errorMsg': 'Region with same code already exists' }), 500
    
    wkt  = request.args.get('wkt')
    gjson  = request.args.get('geo_json')
    
    if wkt is None and gjson is not None:
        conn = DBSession.connection()
        result = conn.execute("SELECT ST_AsText(ST_Multi(ST_GeomFromGeoJSON('%s'))) as g" % gjson)
        for row in result:
            wkt = row['g']
    
    name  = request.args.get('name')
    if name is None:
        name = code
        
    simpl_buf = request.args.get('simpl_buf')
    simpl_dp  = request.args.get('simpl_dp')    
    
    if code != None and wkt != None :
        expression = "ST_SetSRID(ST_Multi(ST_GeomFromText('%s')), 4326)" % wkt 
        
        newR = Region(name = name, code = code, 
               expression = expression, 
               simpl_buf = simpl_buf,
               simpl_dp = simpl_dp)
        
        bf = request.args.get('build_frequency')
        if bf is not None:
            newR.build_frequency = int(bf)
        
        newR.add()       
        DBSession.commit()
        
        region = Region.filter_by(code=code).one()
        
        return flask.jsonify({
            'id': region.id, 
            'result': 'created' })
    
    else:    
        return flask.jsonify({
            'result': 'failed',
            'error': 'not_enough_arguments',
            'errorMsg': 'You need to specify code and wkt or geo_json at least' }), 500    
    
@app.route('/remove/<code>')            
def archive(code):
    region = Region.filter_by(code=code).one()
    if region is None:
        return flask.jsonify({
            'result': 'failed',
            'error': 'not_found',
            'errorMsg': 'Region ' + code + ' not found'}), 500
        
    region.active = False
    region.update()
    DBSession.commit()           

    return flask.jsonify({'result': 'success'})

@app.route('/regions/byuser/<login>')            
def regions_by_user(login):
    conn = DBSession.connection()
    
    regions = Region.join(Region.user).filter(User.login==usr).list()
    if region is None:
        return flask.jsonify({
            'result': 'failed',
            'error': 'not_found',
            'errorMsg': 'Region ' + code + ' not found'}), 500
        
    region.active = False
    region.update()
    DBSession.commit()           

    return flask.jsonify({'result': 'success'})
    

if __name__ == '__main__':
    from osmshp import Env
    app.env = Env()
    app.run(host='0.0.0.0', debug=True)
