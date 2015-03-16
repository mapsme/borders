#!/usr/bin/python
from flask import Flask, g, request, json, jsonify, abort, Response
from flask.ext.cors import CORS
from flask.ext.compress import Compress
import psycopg2
from xml.sax.saxutils import quoteattr

TABLE = 'borders'
OSM_TABLE = 'osm_borders'
READONLY = False

app = Flask(__name__)
#app.debug=True
Compress(app)
CORS(app)

@app.route('/')
def hello_world():
	return 'Hello World!'

@app.before_request
def before_request():
	g.conn = psycopg2.connect('dbname=borders')

@app.teardown_request
def teardown(exception):
	conn = getattr(g, 'conn', None)
	if conn is not None:
		conn.close()

@app.route('/bbox')
def query_bbox():
	xmin = request.args.get('xmin')
	xmax = request.args.get('xmax')
	ymin = request.args.get('ymin')
	ymax = request.args.get('ymax')
	simplify_l = request.args.get('simplify')
	if simplify_l == '2':
		simplify = 0.1
	elif simplify_l == '1':
		simplify = 0.01
	else:
		simplify = 0
	cur = g.conn.cursor()
	cur.execute('SELECT name, ST_AsGeoJSON({geom}, 7) as geometry, ST_NPoints(geom), modified, disabled, count_k, cmnt, round(ST_Area(geography(geom))) as area FROM {table} WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s));'.format(table=TABLE, geom='ST_SimplifyPreserveTopology(geom, {})'.format(simplify) if simplify > 0 else 'geom'), (xmin, ymin, xmax, ymax))
	result = []
	for rec in cur:
		props = { 'name': rec[0], 'nodes': rec[2], 'modified': rec[3], 'disabled': rec[4], 'count_k': rec[5], 'comment': rec[6], 'area': rec[7] }
		feature = { 'type': 'Feature', 'geometry': json.loads(rec[1]), 'properties': props }
		result.append(feature)
	return jsonify(type='FeatureCollection', features=result)

@app.route('/hasosm')
def check_osm_table():
	res = False
	try:
		cur = g.conn.cursor()
		cur.execute('select osm_id, ST_Area(way), admin_level, name from {} limit 2;'.format(OSM_TABLE))
		if cur.rowcount == 2:
			res = True
	except psycopg2.Error, e:
		pass
	return jsonify(result=res)

@app.route('/split')
def split():
	name = request.args.get('name')
	line = request.args.get('line')
	name2 = '{} p2'.format(name)
	cur = g.conn.cursor()
	#todo: cur.execute
	g.conn.commit()

@app.route('/join')
def join_borders():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	name2 = request.args.get('name2')
	cur = g.conn.cursor()
	cur.execute('update {table} set geom = ST_Union(geom, b2.g), count_k = -1 from (select geom as g from borders where name = %s) as b2 where name = %s;'.format(table=TABLE), (name2, name))
	cur.execute('delete from {} where name = %s;'.format(TABLE), (name2,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/point')
def find_osm_borders():
	lat = request.args.get('lat')
	lon = request.args.get('lon')
	cur = g.conn.cursor()
	cur.execute('select osm_id, name, admin_level, ST_Area(geography(way))/1000000 as area_km from {table} where ST_Contains(way, ST_SetSRID(ST_Point(%s, %s), 4326)) order by admin_level desc, name asc;'.format(table=OSM_TABLE), (lon, lat))
	result = []
	for rec in cur:
		b = { 'id': rec[0], 'name': rec[1], 'admin_level': rec[2], 'area': rec[3] }
		result.append(b)
	return jsonify(borders=result)

@app.route('/from_osm')
def copy_from_osm():
	if READONLY:
		abort(405)
	osm_id = request.args.get('id')
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('insert into {table} (geom, name, modified, count_k) select o.way as way, {name}, now(), -1 from {osm} o where o.osm_id = %s;'.format(table=TABLE, osm=OSM_TABLE, name='%s' if name != '' else '%s || osm_borders.name'), (name, osm_id))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/rename')
def set_name():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	new_name = request.args.get('newname')
	cur = g.conn.cursor()
	cur.execute('update {} set name = %s where name = %s;'.format(TABLE), (new_name, name))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/josm')
def make_osm():
	xmin = request.args.get('xmin')
	xmax = request.args.get('xmax')
	ymin = request.args.get('ymin')
	ymax = request.args.get('ymax')
	cur = g.conn.cursor()
	cur.execute('SELECT name, disabled, ST_AsGeoJSON(geom, 7) as geometry FROM {table} WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s));'.format(table=TABLE), (xmin, ymin, xmax, ymax))

	node_pool = { 'id': 1 } # 'lat_lon': id
	regions = [] # { name: name, rings: [['outer', [ids]], ['inner', [ids]], ...] }
	for rec in cur:
		geometry = json.loads(rec[2])
		rings = []
		if geometry['type'] == 'Polygon':
			parse_polygon(node_pool, rings, geometry['coordinates'])
		elif geometry['type'] == 'MultiPolygon':
			for polygon in geometry['coordinates']:
				parse_polygon(node_pool, rings, polygon)
		if len(rings) > 0:
			regions.append({ 'name': rec[0], 'disabled': rec[1], 'rings': rings })
	
	xml = '<?xml version="1.0" encoding="UTF-8"?><osm version="0.6">'
	for latlon, node_id in node_pool.items():
		if latlon != 'id':
			(lat, lon) = latlon.split()
			xml = xml + '<node id="{id}" visible="true" version="1" lat="{lat}" lon="{lon}" />'.format(id=node_id, lat=lat, lon=lon)

	wrid = 1
	for region in regions:
		if len(region['rings']) == 1:
			# simple case: a way
			xml = xml + '<way id="{id}" visible="true" version="1">'.format(id=wrid)
			xml = xml + '<tag k="name" v={} />'.format(quoteattr(region['name']))
			if region['disabled']:
				xml = xml + '<tag k="disabled" v="yes" />'
			for nd in region['rings'][0][1]:
				xml = xml + '<nd ref="{ref}" />'.format(ref=nd)
			xml = xml + '</way>'
			wrid = wrid + 1
		else:
			# multipolygon
			rxml = '<relation id="{id}" visible="true" version="1">'.format(id=wrid)
			wrid = wrid + 1
			rxml = rxml + '<tag k="name" v={} />'.format(quoteattr(region['name']))
			if region['disabled']:
				rxml = rxml + '<tag k="disabled" v="yes" />'
			for ring in region['rings']:
				xml = xml + '<way id="{id}" visible="true" version="1">'.format(id=wrid)
				rxml = rxml + '<member type="way" ref="{ref}" role="{role}" />'.format(ref=wrid, role=ring[0])
				for nd in ring[1]:
					xml = xml + '<nd ref="{ref}" />'.format(ref=nd)
				xml = xml + '</way>'
				wrid = wrid + 1
			xml = xml + rxml + '</relation>'
	xml = xml + '</osm>'
	return Response(xml, mimetype='application/x-osm+xml')

def parse_polygon(node_pool, rings, polygon):
	role = 'outer'
	for ring in polygon:
		nodes = []
		for lonlat in ring:
			ref = '{} {}'.format(lonlat[1], lonlat[0])
			if ref in node_pool:
				node_id = node_pool[ref]
			else:
				node_id = node_pool['id']
				node_pool[ref] = node_id
				node_pool['id'] = node_id + 1
			nodes.append(node_id)
		rings.append([role, nodes])
		role = 'inner'

@app.route('/import')
def import_osm():
	if READONLY:
		abort(405)
	# todo: read file and reconstruct geometries for modified polygons
	# todo: update borders set geom = ...
	pass

@app.route('/delete')
def delete_border():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('delete from {} where name = %s;'.format(TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/disable')
def disable_border():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('update {} set disabled = true where name = %s;'.format(TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/enable')
def enable_border():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('update {} set disabled = false where name = %s;'.format(TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/comment', methods=['POST'])
def update_comment():
	if READONLY:
		abort(405)
	name = request.form['name']
	comment = request.form['comment']
	cur = g.conn.cursor()
	cur.execute('update {} set cmnt = %s where name = %s;'.format(TABLE), (comment, name))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/divpreview')
def divide_preview():
	like = request.args.get('like')
	query = request.args.get('query')
	cur = g.conn.cursor()
	cur.execute('select name, ST_AsGeoJSON(ST_Simplify(way, 0.01)) as way from {table}, (select way as pway from {table} where name like %s) r where ST_Contains(r.pway, way) and {query};'.format(table=OSM_TABLE, query=query), (like,))
	result = []
	for rec in cur:
		feature = { 'type': 'Feature', 'geometry': json.loads(rec[1]), 'properties': { 'name': rec[0] } }
		result.append(feature)
	return jsonify(type='FeatureCollection', features=result)

@app.route('/divide')
def divide():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	like = request.args.get('like')
	query = request.args.get('query')
	prefix = request.args.get('prefix')
	if prefix != '':
		prefix = '{}_'.format(prefix);
	cur = g.conn.cursor()
	cur.execute('insert into {table} (geom, name, modified, count_k) select o.way as way, %s || name, now(), -1 from {osm}, (select way from {osm} where name like %s) r where ST_Contains(r.way, o.way) and {query};'.format(table=TABLE, osm=OSM_TABLE, query=query), (prefix, like,))
	cur.execute('delete from {} where name = %s;'.format(TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

if __name__ == '__main__':
	app.run(threaded=True)
