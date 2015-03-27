#!/usr/bin/python
from flask import Flask, g, request, json, jsonify, abort, Response, send_file
from flask.ext.cors import CORS
from flask.ext.compress import Compress
import psycopg2
import io, re, zipfile, unicodedata
import config

try:
	from lxml import etree
	LXML = True
except:
	LXML = False

app = Flask(__name__)
app.debug=config.DEBUG
Compress(app)
CORS(app)

@app.route('/')
def hello_world():
	return 'Hello <b>World</b>!'

@app.before_request
def before_request():
	g.conn = psycopg2.connect(config.CONNECTION)

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
	table = request.args.get('table')
	if table in config.OTHER_TABLES:
		table = config.OTHER_TABLES[table]
	else:
		table = config.TABLE

	cur = g.conn.cursor()
	cur.execute("""SELECT name, ST_AsGeoJSON({geom}, 7) as geometry, ST_NPoints(geom),
		modified, disabled, count_k, cmnt,
		round(CASE WHEN ST_Area(geography(geom)) = 'NaN' THEN 0 ELSE ST_Area(geography(geom)) END) as area
		FROM {table}
		WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s))
		order by area desc;
		""".format(table=table, geom='ST_SimplifyPreserveTopology(geom, {})'.format(simplify) if simplify > 0 else 'geom'),
		(xmin, ymin, xmax, ymax))
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
	table = request.args.get('table')
	if table in config.OTHER_TABLES:
		table = config.OTHER_TABLES[table]
	else:
		table = config.TABLE
	cur = g.conn.cursor()
	cur.execute('''SELECT name, round(ST_Area(geography(ring))) as area, ST_X(ST_Centroid(ring)), ST_Y(ST_Centroid(ring))
		FROM (
			SELECT name, (ST_Dump(geom)).geom as ring
			FROM {table}
			WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s))
		) g
		WHERE ST_Area(geography(ring)) < %s;'''.format(table=table), (xmin, ymin, xmax, ymax, config.SMALL_KM2 * 1000000))
	result = []
	for rec in cur:
		result.append({ 'name': rec[0], 'area': rec[1], 'lon': float(rec[2]), 'lat': float(rec[3]) })
	return jsonify(features=result)

@app.route('/tables')
def check_osm_table():
	osm = False
	backup = False
	old = []
	try:
		cur = g.conn.cursor()
		cur.execute('select osm_id, ST_Area(way), admin_level, name from {} limit 2;'.format(config.OSM_TABLE))
		if cur.rowcount == 2:
			osm = True
	except psycopg2.Error, e:
		pass
	try:
		cur.execute('select backup, name, ST_Area(geom), modified, disabled, count_k, cmnt from {} limit 2;'.format(config.BACKUP))
		backup = True
	except psycopg2.Error, e:
		pass
	for t, tname in config.OTHER_TABLES.iteritems():
		try:
			cur.execute('select name, ST_Area(geom), modified, disabled, count_k, cmnt from {} limit 2;'.format(tname))
			if cur.rowcount == 2:
				old.append(t)
		except psycopg2.Error, e:
			pass
	return jsonify(osm=osm, tables=old, readonly=config.READONLY, backup=backup)

@app.route('/split')
def split():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	line = request.args.get('line')
	cur = g.conn.cursor()
	# check that we're splitting a single polygon
	cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(config.TABLE), (name,))
	res = cur.fetchone()
	if not res or res[0] != 1:
		return jsonify(status='border should have one outer ring')
	cur.execute('select ST_AsText((ST_Dump(ST_Split(geom, ST_GeomFromText(%s, 4326)))).geom) from {} where name = %s;'.format(config.TABLE), (line, name))
	if cur.rowcount > 1:
		# no use of doing anything if the polygon wasn't modified
		geometries = []
		for res in cur:
			geometries.append(res[0])
		# get disabled flag and delete old border
		cur.execute('select disabled from {} where name = %s;'.format(config.TABLE), (name,))
		disabled = cur.fetchone()[0]
		cur.execute('delete from {} where name = %s;'.format(config.TABLE), (name,))
		# find untaken name series
		base_name = name
		found = False
		while not found:
			base_name = base_name + '_'
			cur.execute('select count(1) from {} where name like %s;'.format(config.TABLE), (name.replace('_', '\_').replace('%', '\%') + '%',))
			found = cur.fetchone()[0] == 0
		# insert new geometries
		counter = 1
		for geom in geometries:
			cur.execute('insert into {table} (name, geom, disabled, count_k, modified) values (%s, ST_GeomFromText(%s, 4326), %s, -1, now());'.format(table=config.TABLE), ('{}{}'.format(base_name, counter), geom, disabled))
			counter = counter + 1
		g.conn.commit()

	return jsonify(status='ok')

@app.route('/join')
def join_borders():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	name2 = request.args.get('name2')
	cur = g.conn.cursor()
	cur.execute('update {table} set geom = ST_Union(geom, b2.g), count_k = -1 from (select geom as g from {table} where name = %s) as b2 where name = %s;'.format(table=config.TABLE), (name2, name))
	cur.execute('delete from {} where name = %s;'.format(config.TABLE), (name2,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/point')
def find_osm_borders():
	lat = request.args.get('lat')
	lon = request.args.get('lon')
	cur = g.conn.cursor()
	cur.execute("select osm_id, name, admin_level, (case when ST_Area(geography(way)) = 'NaN' then 0 else ST_Area(geography(way))/1000000 end) as area_km from {table} where ST_Contains(way, ST_SetSRID(ST_Point(%s, %s), 4326)) order by admin_level desc, name asc;".format(table=config.OSM_TABLE), (lon, lat))
	result = []
	for rec in cur:
		b = { 'id': rec[0], 'name': rec[1], 'admin_level': rec[2], 'area': rec[3] }
		result.append(b)
	return jsonify(borders=result)

@app.route('/from_osm')
def copy_from_osm():
	if config.READONLY:
		abort(405)
	osm_id = request.args.get('id')
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('insert into {table} (geom, name, modified, count_k) select o.way as way, {name}, now(), -1 from {osm} o where o.osm_id = %s limit 1;'.format(table=config.TABLE, osm=config.OSM_TABLE, name='%s' if name != '' else '%s || o.name'), (name, osm_id))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/rename')
def set_name():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	new_name = request.args.get('newname')
	cur = g.conn.cursor()
	cur.execute('update {} set name = %s where name = %s;'.format(config.TABLE), (new_name, name))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/delete')
def delete_border():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('delete from {} where name = %s;'.format(config.TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/disable')
def disable_border():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('update {} set disabled = true where name = %s;'.format(config.TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/enable')
def enable_border():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('update {} set disabled = false where name = %s;'.format(config.TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/comment', methods=['POST'])
def update_comment():
	name = request.form['name']
	comment = request.form['comment']
	cur = g.conn.cursor()
	cur.execute('update {} set cmnt = %s where name = %s;'.format(config.TABLE), (comment, name))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/divpreview')
def divide_preview():
	like = request.args.get('like')
	query = request.args.get('query')
	cur = g.conn.cursor()
	cur.execute('select name, ST_AsGeoJSON(ST_Simplify(way, 0.01)) as way from {table}, (select way as pway from {table} where name like %s) r where ST_Contains(r.pway, way) and {query};'.format(table=config.OSM_TABLE, query=query), (like,))
	result = []
	for rec in cur:
		feature = { 'type': 'Feature', 'geometry': json.loads(rec[1]), 'properties': { 'name': rec[0] } }
		result.append(feature)
	return jsonify(type='FeatureCollection', features=result)

@app.route('/divide')
def divide():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	like = request.args.get('like')
	query = request.args.get('query')
	prefix = request.args.get('prefix')
	if prefix != '':
		prefix = '{}_'.format(prefix);
	cur = g.conn.cursor()
	cur.execute('''insert into {table} (geom, name, modified, count_k)
		select o.way as way, %s || name, now(), -1
		from {osm} o, (
			select way from {osm} where name like %s
		) r
		where ST_Contains(r.way, o.way) and {query};
		'''.format(table=config.TABLE, osm=config.OSM_TABLE, query=query), (prefix, like,))
	cur.execute('delete from {} where name = %s;'.format(config.TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/chop1')
def chop_largest_or_farthest():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(config.TABLE), (name,))
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
			) x;""".format(table=config.TABLE), (name,))
	cur.execute('delete from {} where name = %s;'.format(config.TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/hull')
def draw_hull():
	if config.READONLY:
		abort(405)
	name = request.args.get('name')
	cur = g.conn.cursor()
	cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(config.TABLE), (name,))
	res = cur.fetchone()
	if not res or res[0] < 2:
		return jsonify(status='border should have more than one outer ring')
	cur.execute('update {} set geom = ST_ConvexHull(geom) where name = %s;'.format(config.TABLE), (name,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/backup')
def backup_do():
	if config.READONLY:
		abort(405)
	cur = g.conn.cursor()
	cur.execute("SELECT to_char(now(), 'IYYY-MM-DD HH24:MI'), max(backup) from {};".format(config.BACKUP))
	(timestamp, tsmax) = cur.fetchone()
	if timestamp == tsmax:
		return jsonify(status='please try again later')
	cur.execute('INSERT INTO {backup} (backup, name, geom, disabled, count_k, modified, cmnt) SELECT %s, name, geom, disabled, count_k, modified, cmnt from {table};'.format(backup=config.BACKUP, table=config.TABLE), (timestamp,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/restore')
def backup_restore():
	if config.READONLY:
		abort(405)
	ts = request.args.get('timestamp')
	cur = g.conn.cursor()
	cur.execute('SELECT count(1) from {} where backup = %s;'.format(config.BACKUP), (ts,))
	(count,) = cur.fetchone()
	if count <= 0:
		return jsonify(status='no such timestamp')
	cur.execute('DELETE FROM {};'.format(config.TABLE))
	cur.execute('INSERT INTO {table} (name, geom, disabled, count_k, modified, cmnt) SELECT name, geom, disabled, count_k, modified, cmnt from {backup} where backup = %s;'.format(backup=config.BACKUP, table=config.TABLE), (ts,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/backlist')
def backup_list():
	cur = g.conn.cursor()
	cur.execute("SELECT backup, count(1) from {} group by backup order by backup desc;".format(config.BACKUP))
	result = []
	for res in cur:
		result.append({ 'timestamp': res[0], 'text': res[0], 'count': res[1] })
	# todo: count number of different objects for the last one
	return jsonify(backups=result)

@app.route('/backdelete')
def backup_delete():
	if config.READONLY:
		abort(405)
	ts = request.args.get('timestamp')
	cur = g.conn.cursor()
	cur.execute('SELECT count(1) from {} where backup = %s;'.format(config.BACKUP), (ts,))
	(count,) = cur.fetchone()
	if count <= 0:
		return jsonify(status='no such timestamp')
	cur.execute('DELETE FROM {} WHERE backup = %s;'.format(config.BACKUP), (ts,))
	g.conn.commit()
	return jsonify(status='ok')

@app.route('/josm')
def make_osm():
	xmin = request.args.get('xmin')
	xmax = request.args.get('xmax')
	ymin = request.args.get('ymin')
	ymax = request.args.get('ymax')
	table = request.args.get('table')
	if table in config.OTHER_TABLES:
		table = config.OTHER_TABLES[table]
	else:
		table = config.TABLE

	cur = g.conn.cursor()
	cur.execute('SELECT name, disabled, ST_AsGeoJSON(geom, 7) as geometry FROM {table} WHERE ST_Intersects(ST_SetSRID(ST_Buffer(ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s)), 0.3), 4326), geom);'.format(table=table), (xmin, ymin, xmax, ymax))

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
		if not config.JOSM_FORCE_MULTI and len(region['rings']) == 1 and w1key not in ways:
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

@app.route('/josmbord')
def josm_borders_along():
	name = request.args.get('name')
	line = request.args.get('line')
	cur = g.conn.cursor()
	# select all outer osm borders inside a buffer of the given line
	cur.execute("""
		with linestr as (
			select ST_Intersection(geom, ST_Buffer(ST_GeomFromText(%s, 4326), 0.2)) as line
			from {table} where name = %s
		), osmborders as (
			select (ST_Dump(way)).geom as g from {osm}, linestr where ST_Intersects(line, way)
		)
		select ST_AsGeoJSON((ST_Dump(ST_LineMerge(ST_Intersection(ST_Collect(ST_ExteriorRing(g)), line)))).geom) from osmborders, linestr group by line
		""".format(table=config.TABLE, osm=config.OSM_TABLE), (line, name))

	node_pool = { 'id': 1 } # 'lat_lon': id
	lines = []
	for rec in cur:
		geometry = json.loads(rec[0])
		if geometry['type'] == 'LineString':
			nodes = parse_linestring(node_pool, geometry['coordinates'])
		elif geometry['type'] == 'MultiLineString':
			nodes = []
			for line in geometry['coordinates']:
				nodes.extend(parse_linestring(node_pool, line))
		if len(nodes) > 0:
			lines.append(nodes)
	
	xml = '<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" upload="false">'
	for latlon, node_id in node_pool.items():
		if latlon != 'id':
			(lat, lon) = latlon.split()
			xml = xml + '<node id="{id}" visible="true" version="1" lat="{lat}" lon="{lon}" />'.format(id=node_id, lat=lat, lon=lon)

	wrid = 1
	for line in lines:
		xml = xml + '<way id="{id}" visible="true" version="1">'.format(id=wrid)
		for nd in line:
			xml = xml + '<nd ref="{ref}" />'.format(ref=nd)
		xml = xml + '</way>'
		wrid = wrid + 1
	xml = xml + '</osm>'
	return Response(xml, mimetype='application/x-osm+xml')

def quoteattr(value):
	value = value.replace('&', '&amp;').replace('>', '&gt;').replace('<', '&lt;')
	value = value.replace('\n', '&#10;').replace('\r', '&#13;').replace('\t', '&#9;')
	value = value.replace('"', '&quot;')
	return '"{}"'.format(value)

def ring_hash(refs):
	#return json.dumps(refs)
	return hash(tuple(sorted(refs)))

def parse_polygon(node_pool, rings, polygon):
	role = 'outer'
	for ring in polygon:
		rings.append([role, parse_linestring(node_pool, ring)])
		role = 'inner'

def parse_linestring(node_pool, linestring):
	nodes = []
	for lonlat in linestring:
		ref = '{} {}'.format(lonlat[1], lonlat[0])
		if ref in node_pool:
			node_id = node_pool[ref]
		else:
			node_id = node_pool['id']
			node_pool[ref] = node_id
			node_pool['id'] = node_id + 1
		nodes.append(node_id)
	return nodes

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
	if config.IMPORT_ERROR_ALERT:
		return '<script>alert("{}");</script>'.format(msg)
	else:
		return jsonify(status=msg)

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
	if config.READONLY:
		abort(405)
	if not LXML:
		return import_error('importing is disabled due to absent lxml library')
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
		if len(outer) == 0:
			continue
			#return import_error('relation {} has no outer ways'.format(rel.get('id')))
		# reconstruct rings in multipolygon
		for multi in (inner, outer):
			i = 0
			while i < len(multi):
				way = multi[i]['nodes']
				while way[0] != way[-1]:
					productive = False
					j = i + 1
					while way[0] != way[-1] and j < len(multi):
						new_way = append_way(way, multi[j]['nodes'])
						if new_way:
							multi[i] = dict(multi[i])
							multi[i]['nodes'] = new_way
							way = new_way
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
		# check for 2-node rings
		for multi in (outer, inner):
			for way in multi:
				if len(way['nodes']) < 3:
					return import_error('Way in relation {} has only {} nodes'.format(rel.get('id'), len(way['nodes'])))
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
			continue
			#return import_error('unused in multipolygon way with no name: {}'.format(wid))
		if w['nodes'][0] != w['nodes'][-1]:
			return import_error('non-closed unused in multipolygon way: {}'.format(wid))
		if len(w['nodes']) < 3:
			return import_error('way {} has {} nodes'.format(wid, len(w['nodes'])))
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
		cur.execute('select count(1) from {} where name = %s'.format(config.TABLE), (name,))
		res = cur.fetchone()
		try:
			if res and res[0] > 0:
				# update
				cur.execute('update {table} set disabled = %s, geom = ST_GeomFromText(%s, 4326), modified = now(), count_k = -1 where name = %s'.format(table=config.TABLE), (region['disabled'], region['wkt'], name))
				updated = updated + 1
			else:
				# create
				cur.execute('insert into {table} (name, disabled, geom, modified, count_k) values (%s, %s, ST_GeomFromText(%s, 4326), now(), -1);'.format(table=config.TABLE), (name, region['disabled'], region['wkt']))
				added = added + 1
		except psycopg2.Error, e:
			print 'WKT: {}'.format(region['wkt'])
			raise
	g.conn.commit()
	return jsonify(regions=len(regions), added=added, updated=updated)

@app.route('/poly')
def export_poly():
	xmin = request.args.get('xmin')
	xmax = request.args.get('xmax')
	ymin = request.args.get('ymin')
	ymax = request.args.get('ymax')
	table = request.args.get('table')
	if table in config.OTHER_TABLES:
		table = config.OTHER_TABLES[table]
	else:
		table = config.TABLE

	cur = g.conn.cursor()
	if xmin and xmax and ymin and ymax:
		cur.execute("""SELECT name, ST_AsGeoJSON(geom, 7) as geometry FROM {table} WHERE disabled = false
			and ST_Intersects(ST_SetSRID(ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s)), 4326), geom);
			""".format(table=table), (xmin, ymin, xmax, ymax))
	else:
		cur.execute("""SELECT name, ST_AsGeoJSON(geom, 7) as geometry FROM {table} WHERE disabled = false;""".format(table=table))

	memory_file = io.BytesIO();
	with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
		for res in cur:
			geometry = json.loads(res[1])
			polygons = [geometry['coordinates']] if geometry['type'] == 'Polygon' else geometry['coordinates']
			# sanitize name, src: http://stackoverflow.com/a/295466/1297601
			name = res[0].decode('utf-8')
			name = unicodedata.normalize('NFKD', name)
			name = name.encode('ascii', 'ignore')
			name = re.sub('[^\w _-]', '', name).strip()
			name = name + '.poly'

			poly = io.BytesIO()
			poly.write(res[0] + '\n')
			pcounter = 1
			for polygon in polygons:
				outer = True
				for ring in polygon:
					poly.write('{}\n'.format(pcounter if outer else -pcounter))
					pcounter = pcounter + 1
					for coord in ring:
						poly.write('\t{:E}\t{:E}\n'.format(coord[0], coord[1]))
					poly.write('END\n')
					outer = False
			poly.write('END\n')
			zf.writestr(name, poly.getvalue())
			poly.close()
	memory_file.seek(0)
	return send_file(memory_file, attachment_filename='borders.zip', as_attachment=True)

@app.route('/stat')
def statistics():
	group = request.args.get('group')
	table = request.args.get('table')
	if table in config.OTHER_TABLES:
		table = config.OTHER_TABLES[table]
	else:
		table = config.TABLE
	cur = g.conn.cursor()
	if group == 'total':
		cur.execute('select count(1) from borders;')
		return jsonify(total=cur.fetchone()[0])
	elif group == 'sizes':
		cur.execute("select name, count_k, ST_NPoints(geom), ST_AsGeoJSON(ST_Centroid(geom)), (case when ST_Area(geography(geom)) = 'NaN' then 0 else ST_Area(geography(geom)) / 1000000 end) as area, disabled, (case when cmnt is null or cmnt = '' then false else true end) as cmnt from {};".format(table))
		result = []
		for res in cur:
			coord = json.loads(res[3])['coordinates']
			result.append({ 'name': res[0], 'lat': coord[1], 'lon': coord[0], 'size': res[1], 'nodes': res[2], 'area': res[4], 'disabled': res[5], 'commented': res[6] })
		return jsonify(regions=result)
	elif group == 'topo':
		cur.execute("select name, count(1), min(case when ST_Area(geography(g)) = 'NaN' then 0 else ST_Area(geography(g)) end) / 1000000, sum(ST_NumInteriorRings(g)), ST_AsGeoJSON(ST_Centroid(ST_Collect(g))) from (select name, (ST_Dump(geom)).geom as g from {}) a group by name;".format(table))
		result = []
		for res in cur:
			coord = json.loads(res[4])['coordinates']
			result.append({ 'name': res[0], 'outer': res[1], 'min_area': res[2], 'inner': res[3], 'lon': coord[0], 'lat': coord[1] })
		return jsonify(regions=result)
	return jsonify(status='wrong group id')

if __name__ == '__main__':
	app.run(threaded=True)
