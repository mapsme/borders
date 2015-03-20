#!/usr/bin/python
from flask import Flask, g, request, json, jsonify, abort, Response
from flask.ext.cors import CORS
from flask.ext.compress import Compress
import psycopg2
from lxml import etree
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
	return 'Hello <b>World</b>!'

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

@app.route('/small')
def query_small_in_bbox():
	xmin = request.args.get('xmin')
	xmax = request.args.get('xmax')
	ymin = request.args.get('ymin')
	ymax = request.args.get('ymax')
	cur = g.conn.cursor()
	cur.execute('''SELECT name, round(ST_Area(geography(ring))) as area, ST_X(ST_Centroid(ring)), ST_Y(ST_Centroid(ring))
		FROM (
			SELECT name, (ST_Dump(geom)).geom as ring
			FROM {table}
			WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s))
		) g
		WHERE ST_Area(geography(ring)) < 1000000;'''.format(table=TABLE), (xmin, ymin, xmax, ymax))
	result = []
	for rec in cur:
		result.append({ 'name': rec[0], 'area': rec[1], 'lon': float(rec[2]), 'lat': float(rec[3]) })
	return jsonify(features=result)

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
	if READONLY:
		abort(405)
	name = request.args.get('name')
	line = request.args.get('line')
	cur = g.conn.cursor()
	# check that we're splitting a single polygon
	cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(TABLE), (name,))
	res = cur.fetchone()
	if not res or res[0] != 1:
		return jsonify(status='border should have one outer ring')
	cur.execute('select ST_AsText((ST_Dump(ST_Split(geom, ST_GeomFromText(%s, 4326)))).geom) from {} where name = %s;'.format(TABLE), (line, name))
	if cur.rowcount > 1:
		# no use of doing anything if the polygon wasn't modified
		geometries = []
		for res in cur:
			geometries.append(res[0])
		# get disabled flag and delete old border
		cur.execute('select disabled from {} where name = %s;'.format(TABLE), (name,))
		disabled = cur.fetchone()[0]
		cur.execute('delete from {} where name = %s;'.format(TABLE), (name,))
		# find untaken name series
		base_name = name
		found = False
		while not found:
			base_name = base_name + '_'
			cur.execute('select count(1) from {} where name like %s;'.format(TABLE), (name.replace('_', '\_').replace('%', '\%') + '%',))
			found = cur.fetchone()[0] == 0
		# insert new geometries
		counter = 1
		for geom in geometries:
			cur.execute('insert into {table} (name, geom, disabled, count_k, modified) values (%s, ST_GeomFromText(%s, 4326), %s, -1, now());'.format(table=TABLE), ('{}{}'.format(base_name, counter), geom, disabled))
			counter = counter + 1
		g.conn.commit()

	return jsonify(status='ok')

@app.route('/join')
def join_borders():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	name2 = request.args.get('name2')
	cur = g.conn.cursor()
	cur.execute('update {table} set geom = ST_Union(geom, b2.g), count_k = -1 from (select geom as g from {table} where name = %s) as b2 where name = %s;'.format(table=TABLE), (name2, name))
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
	cur.execute('insert into {table} (geom, name, modified, count_k) select o.way as way, {name}, now(), -1 from {osm} o where o.osm_id = %s limit 1;'.format(table=TABLE, osm=OSM_TABLE, name='%s' if name != '' else '%s || o.name'), (name, osm_id))
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

@app.route('/chop1')
def chop_largest_or_farthest():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(TABLE), (name,))
	res = cur.fetchone()
	if not res or res[0] < 2:
		return jsonify(status='border should have more than one outer ring')
	cur.execute("""INSERT INTO {table} (name, disabled, modified, geom)
			SELECT name, disabled, modified, geom from
			(
			(WITH w AS (SELECT name, disabled, (ST_Dump(geom)).geom AS g FROM {table} WHERE name = %s)
			(SELECT name||'_main' as name, disabled, now() as modified, g as geom, ST_Area(g) as a FROM w ORDER BY a DESC LIMIT 1)
			UNION ALL
			SELECT name||'_small' as name, disabled, now() as modified, ST_Collect(g) AS geom, ST_Area(ST_Collect(g)) as a
			FROM (SELECT name, disabled, g, ST_Area(g) AS a FROM w ORDER BY a DESC OFFSET 1) ww
			GROUP BY name, disabled)
			) x;""".format(table=TABLE), (name,))
	cur.execute('delete from {} where name = %s;'.format(TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/hull')
def draw_hull():
	if READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(TABLE), (name,))
	res = cur.fetchone()
	if not res or res[0] < 2:
		return jsonify(status='border should have more than one outer ring')
	cur.execute('update {} set geom = ST_ConvexHull(geom) where name = %s;'.format(TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/josm')
def make_osm():
	xmin = request.args.get('xmin')
	xmax = request.args.get('xmax')
	ymin = request.args.get('ymin')
	ymax = request.args.get('ymax')
	cur = g.conn.cursor()
	cur.execute('SELECT name, disabled, ST_AsGeoJSON(geom, 7) as geometry FROM {table} WHERE ST_Intersects(ST_SetSRID(ST_Buffer(ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s)), 0.3), 4326), geom);'.format(table=TABLE), (xmin, ymin, xmax, ymax))

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
	
	xml = '<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" upload="false">'
	for latlon, node_id in node_pool.items():
		if latlon != 'id':
			(lat, lon) = latlon.split()
			xml = xml + '<node id="{id}" visible="true" version="1" lat="{lat}" lon="{lon}" />'.format(id=node_id, lat=lat, lon=lon)

	wrid = 1
	ways = {} # json: id
	for region in regions:
		w1key = ring_hash(region['rings'][0][1])
		if len(region['rings']) == 1 and w1key not in ways:
			# simple case: a way
			ways[w1key] = wrid
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
			rxml = rxml + '<tag k="type" v="multipolygon" />'
			rxml = rxml + '<tag k="name" v={} />'.format(quoteattr(region['name']))
			if region['disabled']:
				rxml = rxml + '<tag k="disabled" v="yes" />'
			for ring in region['rings']:
				wkey = ring_hash(ring[1])
				if wkey in ways:
					# already have that way
					rxml = rxml + '<member type="way" ref="{ref}" role="{role}" />'.format(ref=ways[wkey], role=ring[0])
				else:
					ways[wkey] = wrid
					xml = xml + '<way id="{id}" visible="true" version="1">'.format(id=wrid)
					rxml = rxml + '<member type="way" ref="{ref}" role="{role}" />'.format(ref=wrid, role=ring[0])
					for nd in ring[1]:
						xml = xml + '<nd ref="{ref}" />'.format(ref=nd)
					xml = xml + '</way>'
					wrid = wrid + 1
			xml = xml + rxml + '</relation>'
	xml = xml + '</osm>'
	return Response(xml, mimetype='application/x-osm+xml')

def ring_hash(refs):
	#return json.dumps(refs)
	return hash(tuple(sorted(refs)))

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

def append_way(way, way2):
	another = list(way2) # make copy to not modify original list
	if way[0] == way[-1] or another[0] == another[-1]:
		return None
	if way[0] == another[0] or way[-1] == another[-1]:
		another.reverse()
	if way[-1] == another[0]:
		result = list(way)
		result.extend(another[1:])
		return result
	elif way[0] == another[-1]:
		result = another
		result.extend(way)
		return result
	return None

def way_to_wkt(node_pool, refs):
	coords = []
	for nd in refs:
		coords.append('{} {}'.format(node_pool[nd]['lon'], node_pool[nd]['lat']))
	return '({})'.format(','.join(coords))

def import_error(msg):
	return '<script>alert("{}");</script>'.format(msg)
	#return jsonify(status=msg)

def extend_bbox(bbox, x, y=None):
	if y is not None:
		x = [x, y, x, y]
	bbox[0] = min(bbox[0], x[0])
	bbox[1] = min(bbox[1], x[1])
	bbox[2] = max(bbox[2], x[2])
	bbox[3] = max(bbox[3], x[3])

def bbox_contains(outer, inner):
	return outer[0] <= inner[0] and outer[1] <= inner[1] and outer[2] >= inner[2] and outer[3] >= inner[3]

@app.route('/import', methods=['POST'])
def import_osm():
	if READONLY:
		abort(405)
	f = request.files['file']
	if not f:
		return import_error('failed upload')
	try:
		tree = etree.parse(f)
	except:
		return import_error('malformed xml document')
	if not tree:
		return import_error('bad document')
	root = tree.getroot()

	# read nodes and ways
	nodes = {} # id: { lat, lon, modified }
	for node in root.iter('node'):
		if node.get('action') == 'delete':
			continue
		modified = int(node.get('id')) < 0 or node.get('action') == 'modify'
		nodes[node.get('id')] = { 'lat': float(node.get('lat')), 'lon': float(node.get('lon')), 'modified': modified }
	ways = {} # id: { name, disabled, modified, bbox, nodes, used }
	for way in root.iter('way'):
		if way.get('action') == 'delete':
			continue
		way_nodes = []
		bbox = [1e4, 1e4, -1e4, -1e4]
		modified = int(way.get('id')) < 0 or way.get('action') == 'modify'
		for node in way.iter('nd'):
			ref = node.get('ref')
			if not ref in nodes:
				return import_error('missing node {} in way {}'.format(ref, way.get('id')))
			way_nodes.append(ref)
			if nodes[ref]['modified']:
				modified = True
			extend_bbox(bbox, float(nodes[ref]['lon']), float(nodes[ref]['lat']))
		name = None
		disabled = False
		for tag in way.iter('tag'):
			if tag.get('k') == 'name':
				name = tag.get('v')
			if tag.get('k') == 'disabled' and tag.get('v') == 'yes':
				disabled = True
		if len(way_nodes) < 2:
			return import_error('way with less than 2 nodes: {}'.format(way.get('id')))
		ways[way.get('id')] = { 'name': name, 'disabled': disabled, 'modified': modified, 'bbox': bbox, 'nodes': way_nodes, 'used': False }

	# finally we are constructing regions: first, from multipolygons
	regions = {} # name: { modified, disabled, wkt }
	for rel in root.iter('relation'):
		modified = int(rel.get('id')) < 0 or rel.get('action') == 'modify'
		name = None
		disabled = False
		multi = False
		inner = []
		outer = []
		for tag in rel.iter('tag'):
			if tag.get('k') == 'name':
				name = tag.get('v')
			if tag.get('k') == 'disabled' and tag.get('v') == 'yes':
				disabled = True
			if tag.get('k') == 'type' and tag.get('v') == 'multipolygon':
				multi = True
		if not multi:
			return import_error('found non-multipolygon relation: {}'.format(rel.get('id')))
		if not name:
			return import_error('relation {} has no name'.format(rel.get('id')))
		if name in regions:
			return import_error('multiple relations with the same name {}'.format(name))
		for member in rel.iter('member'):
			ref = member.get('ref')
			if not ref in ways:
				return import_error('missing way {} in relation {}'.format(ref, rel.get('id')))
			if ways[ref]['modified']:
				modified = True
			role = member.get('role')
			if role == 'outer':
				outer.append(ways[ref])
			elif role == 'inner':
				inner.append(ways[ref])
			else:
				return import_error('unknown role {} in relation {}'.format(role, rel.get('id')))
			ways[ref]['used'] = True
		# after parsing ways, so 'used' flag is set
		if rel.get('action') == 'delete':
			continue
		# reconstruct rings in multipolygon
		for multi in (inner, outer):
			i = 0
			while i < len(multi):
				way = multi[i]['nodes']
				while way[0] != way[-1]:
					print 'Extending way of {} nodes; start={}, end={}'.format(len(way), way[0], way[-1])
					productive = False
					j = i + 1
					while way[0] != way[-1] and j < len(multi):
						print 'maybe way with start={}, end={}?'.format(multi[j]['nodes'][0], multi[j]['nodes'][-1])
						# todo: do not modify source way!!!
						new_way = append_way(way, multi[j]['nodes'])
						if new_way:
							multi[i] = dict(multi[i])
							multi[i]['nodes'] = new_way
							way = new_way
							print 'now {} nodes; start={}, end={}'.format(len(way), way[0], way[-1])
							if multi[j]['modified']:
								multi[i]['modified'] = True
							extend_bbox(multi[i]['bbox'], multi[j]['bbox'])
							del multi[j]
							productive = True
						else:
							j = j + 1
					if not productive:
						return import_error('unconnected way in relation {}'.format(rel.get('id')))
				i = i + 1
		for way in outer:
			print 'Relation {}: outer way of {} nodes: {}-{}'.format(rel.get('id'), len(way['nodes']), way['nodes'][0], way['nodes'][-1])
		# sort inner and outer rings
		polygons = []
		for way in outer:
			rings = [way_to_wkt(nodes, way['nodes'])]
			for i in range(len(inner)-1, 0, -1):
				if bbox_contains(way['bbox'], inner[i]['bbox']):
					rings.append(way_to_wkt(nodes, inner[i]['nodes']))
					del inner[i]
			polygons.append('({})'.format(','.join(rings)))
		regions[name] = { 'modified': modified, 'disabled': disabled, 'wkt': 'MULTIPOLYGON({})'.format(','.join(polygons)) }

	# make regions from unused named ways
	for wid, w in ways.iteritems():
		if w['used']:
			continue
		if not w['name']:
			return import_error('unused in multipolygon way with no name: {}'.format(wid))
		if w['nodes'][0] != w['nodes'][-1]:
			return import_error('non-closed unused in multipolygon way: {}'.format(way.get('id')))
		if w['name'] in regions:
			return import_error('way {} has the same name as other way/multipolygon'.format(wid))
		regions[w['name']] = { 'modified': w['modified'], 'disabled': w['disabled'], 'wkt': 'POLYGON({})'.format(way_to_wkt(nodes, w['nodes'])) }

	# submit modifications to the database
	cur = g.conn.cursor()
	added = 0
	updated = 0
	for name, region in regions.iteritems():
		if not region['modified']:
			continue
		cur.execute('select count(1) from {} where name = %s'.format(TABLE), (name,))
		res = cur.fetchone()
		if res and res[0] > 0:
			# update
			cur.execute('update {table} set disabled = %s, geom = ST_GeomFromText(%s, 4326), modified = now(), count_k = -1 where name = %s'.format(table=TABLE), (region['disabled'], region['wkt'], name))
			updated = updated + 1
		else:
			# create
			cur.execute('insert into {table} (name, disabled, geom, modified, count_k) values (%s, %s, %s, now(), -1);'.format(table=TABLE), (name, region['disabled'], region['wkt']))
			added = added + 1
	g.conn.commit()
	return jsonify(regions=len(regions), added=added, updated=updated)

if __name__ == '__main__':
	app.run(threaded=True)
