#!/usr/bin/python3.6
import io
import itertools
import re
import sys, traceback
import zipfile
from unidecode import unidecode
from queue import Queue

from flask import (
        Flask, g,
        request, Response, abort,
        json, jsonify,
        render_template,
        send_file, send_from_directory
)
from flask_cors import CORS
from flask_compress import Compress
import psycopg2

import config
from auto_split import prepare_bulk_split, split_region
from countries_structure import (
    CountryStructureException,
    create_countries_initial_structure,
)

try:
    from lxml import etree
    LXML = True
except:
    LXML = False

app = Flask(__name__)
app.debug=config.DEBUG
Compress(app)
CORS(app)
app.config['JSON_AS_ASCII'] = False


@app.route('/static/<path:path>')
def send_js(path):
    if config.DEBUG:
        return send_from_directory('static/', path)
    abort(404)

@app.before_request
def before_request():
    g.conn = psycopg2.connect(config.CONNECTION)

@app.teardown_request
def teardown(exception):
    conn = getattr(g, 'conn', None)
    if conn is not None:
        conn.close()

@app.route('/')
@app.route('/index.html')
def index():
    return render_template('index.html')

@app.route('/stat.html')
def stat():
    return render_template('stat.html')

def fetch_borders(**kwargs):
    table = kwargs.get('table', config.TABLE)
    simplify = kwargs.get('simplify', 0)
    where_clause = kwargs.get('where_clause', '1=1')
    only_leaves = kwargs.get('only_leaves', True)
    osm_table = config.OSM_TABLE
    geom = (f'ST_SimplifyPreserveTopology(geom, {simplify})'
            if simplify > 0 else 'geom')
    leaves_filter = (f""" AND id NOT IN (SELECT parent_id FROM {table}
                                          WHERE parent_id IS NOT NULL)"""
                     if only_leaves else '')
    query = f"""
        SELECT name, geometry, nodes, modified, disabled, count_k, cmnt,
               (CASE WHEN area = 'NaN' THEN 0 ELSE area END) AS area,
               id, admin_level, parent_id, parent_name
        FROM (
            SELECT name,
               ST_AsGeoJSON({geom}, 7) as geometry,
               ST_NPoints(geom) AS nodes,
               modified,
               disabled,
               count_k,
               cmnt,
               round(ST_Area(geography(geom))) AS area,
               id,
               ( SELECT admin_level FROM {osm_table}
                 WHERE osm_id = t.id
               ) AS admin_level,
               parent_id,
               ( SELECT name FROM {table}
                 WHERE id = t.parent_id
               ) AS parent_name
            FROM {table} t
            WHERE ({where_clause}) {leaves_filter}
        ) q
        ORDER BY area DESC
        """
    #print(query)
    cur = g.conn.cursor()
    cur.execute(query)
    borders = []
    for rec in cur:
        region_id = rec[8]
        country_id, country_name = get_region_country(region_id)
        props = { 'name': rec[0] or '', 'nodes': rec[2], 'modified': rec[3],
                  'disabled': rec[4], 'count_k': rec[5],
                  'comment': rec[6],
                  'area': rec[7],
                  'id': region_id, 'country_id': country_id,
                  'admin_level': rec[9],
                  'parent_id': rec[10],
                  'parent_name': rec[11] or '',
                  'country_name': country_name
                }
        feature = {'type': 'Feature',
                   'geometry': json.loads(rec[1]),
                   'properties': props
                  }
        borders.append(feature)
    #print([x['properties'] for x in borders])
    return borders

def simplify_level_to_postgis_value(simplify_level):
    return (
        0.1 if simplify_level == '2'
        else 0.01 if simplify_level == '1'
        else 0
    )

@app.route('/bbox')
def query_bbox():
    xmin = float(request.args.get('xmin'))
    xmax = float(request.args.get('xmax'))
    ymin = float(request.args.get('ymin'))
    ymax = float(request.args.get('ymax'))
    simplify_level = request.args.get('simplify')
    simplify = simplify_level_to_postgis_value(simplify_level)
    table = request.args.get('table')
    if table in config.OTHER_TABLES:
        table = config.OTHER_TABLES[table]
    else:
        table = config.TABLE
    borders = fetch_borders(
        table=table,
        simplify=simplify,
        where_clause=f'geom && ST_MakeBox2D(ST_Point({xmin}, {ymin}),'
                                          f'ST_Point({xmax}, {ymax}))'
    )
    return jsonify(
        status='ok',
        geojson={'type':'FeatureCollection', 'features': borders}
    )

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

@app.route('/routing')
def query_routing_points():
    xmin = request.args.get('xmin')
    xmax = request.args.get('xmax')
    ymin = request.args.get('ymin')
    ymax = request.args.get('ymax')
    cur = g.conn.cursor()
    try:
        cur.execute('''SELECT ST_X(geom), ST_Y(geom), type
                FROM points
                WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s)
            );''', (xmin, ymin, xmax, ymax))
    except psycopg2.Error as e:
        return jsonify(features=[])
    result = []
    for rec in cur:
        result.append({ 'lon': rec[0], 'lat': rec[1], 'type': rec[2] })
    return jsonify(features=result)

@app.route('/crossing')
def query_crossing():
    xmin = request.args.get('xmin')
    xmax = request.args.get('xmax')
    ymin = request.args.get('ymin')
    ymax = request.args.get('ymax')
    region = request.args.get('region', '').encode('utf-8')
    points = request.args.get('points') == '1'
    rank = request.args.get('rank') or '4'
    cur = g.conn.cursor()
    sql = """SELECT id, ST_AsGeoJSON({line}, 7) as geometry, region, processed FROM {table}
        WHERE line && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s)) and processed = 0 {reg} and rank <= %s;
        """.format(table=config.CROSSING_TABLE, reg='and region = %s' if region else '', line='line' if not points else 'ST_Centroid(line)')
    params = [xmin, ymin, xmax, ymax]
    if region:
        params.append(region)
        params.append(rank)
    result = []
    try:
        cur.execute(sql, tuple(params))
        for rec in cur:
            props = { 'id': rec[0], 'region': rec[2], 'processed': rec[3] }
            feature = { 'type': 'Feature', 'geometry': json.loads(rec[1]), 'properties': props }
            result.append(feature)
    except psycopg2.Error as e:
        pass
    return jsonify(type='FeatureCollection', features=result)

@app.route('/tables')
def check_osm_table():
    osm = False
    backup = False
    old = []
    crossing = False
    try:
        cur = g.conn.cursor()
        cur.execute('select osm_id, ST_Area(way), admin_level, name from {} limit 2;'.format(config.OSM_TABLE))
        if cur.rowcount == 2:
            osm = True
    except psycopg2.Error as e:
        pass
    try:
        cur.execute('select backup, id, name, parent_id, ST_Area(geom), modified, disabled, count_k, cmnt from {} limit 2;'.format(config.BACKUP))
        backup = True
    except psycopg2.Error as e:
        pass
    for t, tname in config.OTHER_TABLES.items():
        try:
            cur.execute('select name, ST_Area(geom), modified, disabled, count_k, cmnt from {} limit 2;'.format(tname))
            if cur.rowcount == 2:
                old.append(t)
        except psycopg2.Error as e:
            pass
    try:
        cur = g.conn.cursor()
        cur.execute('select id, ST_Length(line), region, processed from {} limit 2;'.format(config.CROSSING_TABLE))
        if cur.rowcount == 2:
            crossing = True
    except psycopg2.Error as e:
        pass
    return jsonify(osm=osm, tables=old, readonly=config.READONLY, backup=backup, crossing=crossing)

@app.route('/search')
def search():
    query = request.args.get('q')
    cur = g.conn.cursor()
    cur.execute(f"""
        SELECT ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom)
        FROM {config.TABLE}
        WHERE name ILIKE %s
        ORDER BY (ST_Area(geography(geom)))
        LIMIT 1""", (f'%{query}%',)
    )
    if cur.rowcount > 0:
        rec = cur.fetchone()
        return jsonify(bounds=[rec[0], rec[1], rec[2], rec[3]])
    return jsonify(status='not found')

@app.route('/split')
def split():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    line = request.args.get('line')
    save_region = (request.args.get('save_region') == 'true')
    table = config.TABLE
    cur = g.conn.cursor()
    # check that we're splitting a single polygon
    cur.execute(f'SELECT ST_NumGeometries(geom) FROM {table} WHERE id = %s;', (region_id,))
    res = cur.fetchone()
    if not res or res[0] != 1:
        return jsonify(status='border should have one outer ring')
    cur.execute(f"""
            SELECT ST_AsText((ST_Dump(ST_Split(geom, ST_GeomFromText(%s, 4326)))).geom)
            FROM {table}
            WHERE id = %s
            """, (line, region_id)
    )
    if cur.rowcount > 1:
        # no use of doing anything if the polygon wasn't modified
        geometries = []
        for res in cur:
            geometries.append(res[0])
        # get region properties and delete old border
        cur.execute(f'SELECT name, parent_id, disabled FROM {table} WHERE id = %s', (region_id,))
        name, parent_id, disabled = cur.fetchone()
        if save_region:
            parent_id = region_id
        else:
            cur.execute(f'DELETE FROM {table} WHERE id = %s', (region_id,))
        base_name = name
        # insert new geometries
        counter = 1
        free_id = get_free_id()
        for geom in geometries:
            cur.execute(f"""
                INSERT INTO {table} (id, name, geom, disabled, count_k, modified, parent_id)
                    VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, -1, now(), %s)
                """, (free_id, f'{base_name}_{counter}', geom, disabled, parent_id)
            )
            counter += 1
            free_id -= 1
        g.conn.commit()

    return jsonify(status='ok')

@app.route('/join')
def join_borders():
    if config.READONLY:
        abort(405)
    region_id1 = int(request.args.get('id1'))
    region_id2 = int(request.args.get('id2'))
    if region_id1 == region_id2:
        return jsonify(status='failed to join region with itself')
    cur = g.conn.cursor()
    try:
        table = config.TABLE
        free_id = get_free_id()
        cur.execute(f"""
                UPDATE {table}
                SET id = {free_id},
                    geom = ST_Union(geom, b2.g),
                    count_k = -1
                FROM (SELECT geom AS g FROM {table} WHERE id = %s) AS b2
                WHERE id = %s""", (region_id2, region_id1))
        cur.execute(f"DELETE FROM {table} WHERE id = %s", (region_id2,))
    except psycopg2.Error as e:
        g.conn.rollback()
        return jsonify(status=str(e))
    g.conn.commit()
    return jsonify(status='ok')

def get_parent_region_id(region_id):
    cursor = g.conn.cursor()
    cursor.execute(f"""
        SELECT parent_id FROM {config.TABLE} WHERE id = %s
        """, (region_id,)
    )
    rec = cursor.fetchone()
    parent_id = int(rec[0]) if rec[0] is not None else None
    return parent_id

def get_child_region_ids(region_id):
    cursor = g.conn.cursor()
    cursor.execute(f"""
        SELECT id FROM {config.TABLE} WHERE parent_id = %s
        """, (region_id,)
    )
    child_ids = []
    for rec in cursor:
        child_ids.append(int(rec[0]))
    return child_ids

@app.route('/join_to_parent')
def join_to_parent():
    """Find all descendants of a region and remove them starting
    from the lowerst hierarchical level to not violate 'parent_id'
    foreign key constraint (which is probably not in ON DELETE CASCADE mode)
    """
    region_id = int(request.args.get('id'))
    parent_id = get_parent_region_id(region_id)
    if not parent_id:
        return jsonify(status=f'Region {region_id} has no parent')
    cursor = g.conn.cursor()
    descendants = [[parent_id]]  # regions ordered by hierarchical level

    while True:
        parent_ids = descendants[-1]
        child_ids = list(itertools.chain.from_iterable(
            get_child_region_ids(x) for x in parent_ids
        ))
        if child_ids:
            descendants.append(child_ids)
        else:
            break
    while len(descendants) > 1:
        lowerst_ids = descendants.pop()
        ids_str = ','.join(str(x) for x in lowerst_ids)
        cursor.execute(f"""
            DELETE FROM {config.TABLE} WHERE id IN ({ids_str})
            """
        )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/set_parent')
def set_parent():
    region_id = int(request.args.get('id'))
    parent_id = request.args.get('parent_id')
    parent_id = int(parent_id) if parent_id else None
    table = config.TABLE
    cursor = g.conn.cursor()
    cursor.execute(f"""
        UPDATE {table} SET parent_id = %s WHERE id = %s
        """, (parent_id, region_id)
    )
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
    name_sql = f"'{name}'" if name else "'name'"
    table = config.TABLE
    osm_table = config.OSM_TABLE
    cur = g.conn.cursor()
    # Check if this id already in use
    cur.execute(f"SELECT id FROM {table} WHERE id = %s", (osm_id,))
    rec = cur.fetchone()
    if rec and rec[0]:
        return jsonify(status=f"Region with id={osm_id} already exists")
    cur.execute(f"""
        INSERT INTO {table} (id, geom, name, modified, count_k)
          SELECT osm_id, way, {name_sql}, now(), -1
          FROM {osm_table}
          WHERE osm_id = %s
        """, (osm_id,)
    )
    assign_region_to_lowerst_parent(osm_id)
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/rename')
def set_name():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    table = config.TABLE
    new_name = request.args.get('new_name')
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {table} SET name = %s WHERE id = %s",
                (new_name, region_id))
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/delete')
def delete_border():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    cur = g.conn.cursor()
    cur.execute('delete from {} where id = %s;'.format(config.TABLE), (region_id,))
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/disable')
def disable_border():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {config.TABLE} SET disabled = true WHERE id = %s",
                (region_id,)
    )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/enable')
def enable_border():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {config.TABLE} SET disabled = false WHERE id = %s",
                (region_id,)
    )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/comment', methods=['POST'])
def update_comment():
    region_id = int(request.form['id'])
    comment = request.form['comment']
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {config.TABLE} SET cmnt = %s WHERE id = %s",
                (comment, region_id)
    )
    g.conn.commit()
    return jsonify(status='ok')

def is_administrative_region(region_id):
    osm_table = config.OSM_TABLE
    cur = g.conn.cursor()
    cur.execute(f"""
        SELECT osm_id FROM {osm_table} WHERE osm_id = %s
        """, (region_id,)
    )
    return bool(cur.rowcount > 0)

def find_osm_child_regions(region_id):
    cursor = g.conn.cursor()
    table = config.TABLE
    osm_table = config.OSM_TABLE
    cursor.execute(f"""
        SELECT c.id, oc.admin_level
        FROM {table} c, {table} p, {osm_table} oc
        WHERE p.id = c.parent_id AND c.id = oc.osm_id
            AND p.id = %s
        """, (region_id,)
    )
    children = []
    for rec in cursor:
        children.append({'id': int(rec[0]), 'admin_level': int(rec[1])})
    return children

def is_leaf(region_id):
    cursor = g.conn.cursor()
    cursor.execute(f"""
        SELECT 1
        FROM {config.TABLE}
        WHERE parent_id = %s
        LIMIT 1
        """, (region_id,)
    )
    return cursor.rowcount == 0

def get_region_country(region_id):
    """Returns the uppermost predecessor of the region in the hierarchy,
    possibly itself.
    """
    predecessors = get_predecessors(region_id)
    return predecessors[-1]

def get_predecessors(region_id):
    """Returns the list of (id, name)-tuples of all predecessors,
    starting from the very region_id.
    """
    predecessors = []
    table = config.TABLE
    cursor = g.conn.cursor()
    while True:
        cursor.execute(f"""
            SELECT id, name, parent_id
            FROM {table} WHERE id={region_id}
            """)
        rec = cursor.fetchone()
        if not rec:
           raise Exception(f"No record in '{table}' table with id = {region_id}")
        predecessors.append(rec[0:2])
        parent_id = rec[2]
        if not parent_id:
            break
        region_id = parent_id
    return predecessors

def get_region_full_name(region_id):
    predecessors = get_predecessors(region_id)
    return '_'.join(pr[1] for pr in reversed(predecessors))

def get_similar_regions(region_id, only_leaves=False):
    """Returns ids of regions of the same admin_level in the same country.
    Prerequisite: is_administrative_region(region_id) is True.
    """
    cursor = g.conn.cursor()
    cursor.execute(f"""
        SELECT admin_level FROM {config.OSM_TABLE}
        WHERE osm_id = %s""", (region_id,)
    )
    admin_level = int(cursor.fetchone()[0])
    country_id, country_name = get_region_country(region_id)
    q = Queue()
    q.put({'id': country_id, 'admin_level': 2})
    similar_region_ids = []
    while not q.empty():
        item = q.get()
        if item['admin_level'] == admin_level:
            similar_region_ids.append(item['id'])
        elif item['admin_level'] < admin_level:
            children = find_osm_child_regions(item['id'])
            for ch in children:
                q.put(ch)
    if only_leaves:
        similar_region_ids = [r_id for r_id in similar_region_ids
                                  if is_leaf(r_id)]
    return similar_region_ids


NON_ADMINISTRATIVE_REGION_ERROR = ("Not allowed to split non-administrative"
                                   " border into administrative subregions")

@app.route('/divpreview')
def divide_preview():
    region_id = int(request.args.get('id'))
    if not is_administrative_region(region_id):
        return jsonify(status=NON_ADMINISTRATIVE_REGION_ERROR)
    next_level = int(request.args.get('next_level'))
    apply_to_similar = (request.args.get('apply_to_similar') == 'true')
    region_ids = [region_id]
    if apply_to_similar:
        region_ids = get_similar_regions(region_id, only_leaves=True)
    auto_divide = (request.args.get('auto_divide') == 'true')
    if auto_divide:
        try:
            city_population_thr = int(request.args.get('city_population_thr'))
            cluster_population_thr = int(request.args.get('cluster_population_thr'))
        except ValueError:
            return jsonify(status='Not a number in thresholds')
        return divide_into_clusters_preview(
                region_ids, next_level,
                (city_population_thr, cluster_population_thr))
    else:
        return divide_into_subregions_preview(region_ids, next_level)

def get_subregions(region_ids, next_level):
    subregions = list(itertools.chain.from_iterable(
        get_subregions_one(region_id, next_level)
            for region_id in region_ids
    ))
    return subregions

def get_subregions_one(region_id, next_level):
    osm_table = config.OSM_TABLE
    cur = g.conn.cursor()
    # We use ST_SimplifyPreserveTopology, since ST_Simplify would give NULL
    # for very little regions.
    cur.execute(f"""
        SELECT name,
               ST_AsGeoJSON(ST_SimplifyPreserveTopology(way, 0.01)) as way,
               osm_id
        FROM {osm_table}
        WHERE ST_Contains(
                (SELECT way FROM {osm_table} WHERE osm_id = %s), way
              )
            AND admin_level = %s
        """, (region_id, next_level)
    )
    subregions = []
    for rec in cur:
        #if rec[1] is None:
        #    continue
        feature = { 'type': 'Feature', 'geometry': json.loads(rec[1]),
                    'properties': { 'name': rec[0] } }
        subregions.append(feature)
    return subregions

def get_clusters(region_ids, next_level, thresholds):
    clusters = list(itertools.chain.from_iterable(
        get_clusters_one(region_id, next_level, thresholds)
            for region_id in region_ids
    ))
    return clusters

def get_clusters_one(region_id, next_level, thresholds):
    autosplit_table = config.AUTOSPLIT_TABLE
    cursor = g.conn.cursor()
    where_clause = f"""
        osm_border_id = %s
        AND city_population_thr = %s
        AND cluster_population_thr = %s
        """
    splitting_sql_params = (region_id,) + thresholds
    cursor.execute(f"""
        SELECT id FROM {autosplit_table}
        WHERE {where_clause}
        """, splitting_sql_params)
    if cursor.rowcount == 0:
        split_region(g.conn, region_id, next_level, thresholds)
    cursor.execute(f"""
        SELECT id, ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.01)) as way
        FROM {autosplit_table}
        WHERE {where_clause}
        """, splitting_sql_params)
    clusters = []
    for rec in cursor:
        cluster = { 'type': 'Feature',
                    'geometry': json.loads(rec[1]),
                    'properties': {'osm_id': int(rec[0])}
        }
        clusters.append(cluster)
    return clusters

def divide_into_subregions_preview(region_ids, next_level):
    subregions = get_subregions(region_ids, next_level)
    return jsonify(
        status='ok',
        subregions={'type': 'FeatureCollection', 'features': subregions}
    )

def divide_into_clusters_preview(region_ids, next_level, thresholds):
    subregions = get_subregions(region_ids, next_level)
    clusters = get_clusters(region_ids, next_level, thresholds)
    return jsonify(
        status='ok',
        subregions={'type': 'FeatureCollection', 'features': subregions},
        clusters={'type': 'FeatureCollection', 'features': clusters}
    )

@app.route('/divide')
def divide():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    if not is_administrative_region(region_id):
        return jsonify(status=NON_ADMINISTRATIVE_REGION_ERROR)
    next_level = int(request.args.get('next_level'))
    apply_to_similar = (request.args.get('apply_to_similar') == 'true')
    region_ids = [region_id]
    if apply_to_similar:
        region_ids = get_similar_regions(region_id, only_leaves=True)
    auto_divide = (request.args.get('auto_divide') == 'true')
    if auto_divide:
        try:
            city_population_thr = int(request.args.get('city_population_thr'))
            cluster_population_thr = int(request.args.get('cluster_population_thr'))
        except ValueError:
            return jsonify(status='Not a number in thresholds')
        return divide_into_clusters(
                region_ids, next_level,
                (city_population_thr, cluster_population_thr))
    else:
        return divide_into_subregions(region_ids, next_level)

def divide_into_subregions(region_ids, next_level):
    table = config.TABLE
    osm_table = config.OSM_TABLE
    cur = g.conn.cursor()
    for region_id in region_ids:
        # TODO: rewrite SELECT into join rather than subquery to enable gist index
        cur.execute(f"""
            INSERT INTO {table} (id, geom, name, parent_id, modified, count_k)
            SELECT osm_id, way, name, %s, now(), -1
            FROM {osm_table}
            WHERE ST_Contains(
                    (SELECT way FROM {osm_table} WHERE osm_id = %s), way
                )
                AND admin_level = {next_level}
            """, (region_id, region_id,)
        )
    g.conn.commit()
    return jsonify(status='ok')

def divide_into_clusters(region_ids, next_level, thresholds):
    table = config.TABLE
    autosplit_table = config.AUTOSPLIT_TABLE
    cursor = g.conn.cursor()
    insert_cursor = g.conn.cursor()
    for region_id in region_ids:
        cursor.execute(f"SELECT name FROM {table} WHERE id = %s", (region_id,))
        base_name = cursor.fetchone()[0]

        where_clause = f"""
            osm_border_id = %s
            AND city_population_thr = %s
            AND cluster_population_thr = %s
            """
        splitting_sql_params = (region_id,) + thresholds
        cursor.execute(f"""
            SELECT id FROM {autosplit_table}
            WHERE {where_clause}
            """, splitting_sql_params)
        if cursor.rowcount == 0:
            split_region(g.conn, region_id, next_level, thresholds)

        free_id = get_free_id()
        counter = 0
        cursor.execute(f"""
            SELECT id
            FROM {autosplit_table} WHERE {where_clause}
            """, splitting_sql_params)
        for rec in cursor:
            cluster_id = rec[0]
            counter += 1
            name = f"{base_name}_{counter}"
            insert_cursor.execute(f"""
                INSERT INTO {table} (id, name, parent_id, geom, modified, count_k)
                SELECT {free_id}, '{name}', osm_border_id, geom, now(), -1
                FROM {autosplit_table} WHERE id = %s AND {where_clause}
                """, (cluster_id,) + splitting_sql_params)
            free_id -= 1
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/chop1')
def chop_largest_or_farthest():
    if config.READONLY:
        abort(405)
    name = request.args.get('name').encode('utf-8')
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
    name = request.args.get('name').encode('utf-8')
    cur = g.conn.cursor()
    cur.execute('select ST_NumGeometries(geom) from {} where name = %s;'.format(config.TABLE), (name,))
    res = cur.fetchone()
    if not res or res[0] < 2:
        return jsonify(status='border should have more than one outer ring')
    cur.execute('update {} set geom = ST_ConvexHull(geom) where name = %s;'.format(config.TABLE), (name,))
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/fixcrossing')
def fix_crossing():
    if config.READONLY:
        abort(405)
    preview = request.args.get('preview') == '1'
    region = request.args.get('region').encode('utf-8')
    if region is None:
        return jsonify(status='Please specify a region')
    ids = request.args.get('ids')
    if ids is None or len(ids) == 0:
        return jsonify(status='Please specify a list of line ids')
    ids = tuple(ids.split(','))
    cur = g.conn.cursor()
    if preview:
        cur.execute("""
        WITH lines as (SELECT ST_Buffer(ST_Collect(line), 0.002, 1) as g FROM {cross} WHERE id IN %s)
        SELECT ST_AsGeoJSON(ST_Collect(ST_MakePolygon(er.ring))) FROM
        (
        SELECT ST_ExteriorRing((ST_Dump(ST_Union(ST_Buffer(geom, 0.0), lines.g))).geom) as ring FROM {table}, lines WHERE name = %s
        ) as er
        """.format(table=config.TABLE, cross=config.CROSSING_TABLE), (ids, region))
        res = cur.fetchone()
        if not res:
            return jsonify(status='Failed to extend geometry')
        f = { "type": "Feature", "properties": {}, "geometry": json.loads(res[0]) }
        #return jsonify(type="FeatureCollection", features=[f])
        return jsonify(type="Feature", properties={}, geometry=json.loads(res[0]))
    else:
        cur.execute("""
        WITH lines as (SELECT ST_Buffer(ST_Collect(line), 0.002, 1) as g FROM {cross} WHERE id IN %s)
        UPDATE {table} SET geom = res.g FROM
        (
        SELECT ST_Collect(ST_MakePolygon(er.ring)) as g FROM
        (
        SELECT ST_ExteriorRing((ST_Dump(ST_Union(ST_Buffer(geom, 0.0), lines.g))).geom) as ring FROM {table}, lines WHERE name = %s
        ) as er
        ) as res
        WHERE name = %s
        """.format(table=config.TABLE, cross=config.CROSSING_TABLE), (ids, region, region))
        cur.execute("""
        UPDATE {table} b SET geom = ST_Difference(b.geom, o.geom)
        FROM {table} o
        WHERE ST_Overlaps(b.geom, o.geom)
        AND o.name = %s
        """.format(table=config.TABLE), (region,))
        cur.execute("UPDATE {cross} SET processed = 1 WHERE id IN %s".format(cross=config.CROSSING_TABLE), (ids,))
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
    backup_table = config.BACKUP
    table = config.TABLE
    cur.execute(f"""
        INSERT INTO {backup_table}
             (backup, id, name, parent_id, geom, disabled, count_k, modified, cmnt)
          SELECT %s, id, name, parent_id, geom, disabled, count_k, modified, cmnt
          FROM {table}
        """, (timestamp,)
    )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/restore')
def backup_restore():
    if config.READONLY:
        abort(405)
    ts = request.args.get('timestamp')
    cur = g.conn.cursor()
    table = config.TABLE
    backup_table = config.BACKUP
    cur.execute(f"SELECT count(1) from {backup_table} WHERE backup = %s",(ts,))
    (count,) = cur.fetchone()
    if count <= 0:
        return jsonify(status='no such timestamp')
    cur.execute(f'DELETE FROM {table}')
    cur.execute(f"""
        INSERT INTO {table}
            (id, name, parent_id, geom, disabled, count_k, modified, cmnt)
          SELECT id, name, parent_id, geom, disabled, count_k, modified, cmnt
          FROM {backup_table}
          WHERE backup = %s
        """, (ts,)
    )
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
    borders = fetch_borders(
        table=table,
        where_clause=f'geom && ST_MakeBox2D(ST_Point({xmin}, {ymin}),'
                                          f'ST_Point({xmax}, {ymax}))'
    )
    node_pool = { 'id': 1 } # 'lat_lon': id
    regions = [] # { id: id, name: name, rings: [['outer', [ids]], ['inner', [ids]], ...] }
    for border in borders:
        geometry = border['geometry'] #json.loads(rec[2])
        rings = []
        if geometry['type'] == 'Polygon':
            parse_polygon(node_pool, rings, geometry['coordinates'])
        elif geometry['type'] == 'MultiPolygon':
            for polygon in geometry['coordinates']:
                parse_polygon(node_pool, rings, polygon)
        if len(rings) > 0:
            regions.append({
                'id': abs(border['properties']['id']),
                'name': border['properties']['name'],
                'disabled': border['properties']['disabled'],
                'rings': rings
            })

    xml = '<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" upload="false">'
    for latlon, node_id in node_pool.items():
        if latlon != 'id':
            (lat, lon) = latlon.split()
            xml = xml + '<node id="{id}" visible="true" version="1" lat="{lat}" lon="{lon}" />'.format(id=node_id, lat=lat, lon=lon)

    ways = {} # json: id
    wrid = 1
    for region in regions:
        w1key = ring_hash(region['rings'][0][1])
        if not config.JOSM_FORCE_MULTI and len(region['rings']) == 1 and w1key not in ways:
            # simple case: a way
            ways[w1key] = region['id']
            xml = xml + '<way id="{id}" visible="true" version="1">'.format(id=region['id'])
            xml = xml + '<tag k="name" v={} />'.format(quoteattr(region['name']))
            if region['disabled']:
                xml = xml + '<tag k="disabled" v="yes" />'
            for nd in region['rings'][0][1]:
                xml = xml + '<nd ref="{ref}" />'.format(ref=nd)
            xml = xml + '</way>'
        else:
            # multipolygon
            rxml = '<relation id="{id}" visible="true" version="1">'.format(id=region['id'])
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
    regions = {} # /*name*/ id: { modified, disabled, wkt, type: 'r'|'w' }
    for rel in root.iter('relation'):
        if rel.get('action') == 'delete':
            continue
        osm_id = int(rel.get('id'))
        modified = osm_id < 0 or rel.get('action') == 'modify'
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
        #if not name:
        #    return import_error('relation {} has no name'.format(rel.get('id')))
        #if name in regions:
        #    return import_error('multiple relations with the same name {}'.format(name))
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
            return import_error('relation {} has no outer ways'.format(rel.get('id')))
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
            for i in range(len(inner)-1, -1, -1):
                if bbox_contains(way['bbox'], inner[i]['bbox']):
                    rings.append(way_to_wkt(nodes, inner[i]['nodes']))
                    del inner[i]
            polygons.append('({})'.format(','.join(rings)))
        regions[osm_id] = {
                'id': osm_id,
                'type': 'r',
                'name': name,
                'modified': modified,
                'disabled': disabled,
                'wkt': 'MULTIPOLYGON({})'.format(','.join(polygons))
        }

    # make regions from unused named ways
    for wid, w in ways.items():
        if w['used']:
            continue
        if not w['name']:
            #continue
            return import_error('unused in multipolygon way with no name: {}'.format(wid))
        if w['nodes'][0] != w['nodes'][-1]:
            return import_error('non-closed unused in multipolygon way: {}'.format(wid))
        if len(w['nodes']) < 3:
            return import_error('way {} has {} nodes'.format(wid, len(w['nodes'])))
        #if w['name'] in regions:
        #    return import_error('way {} has the same name as other way/multipolygon'.format(wid))
        regions[wid] = {
                'id': int(wid),
                'type': 'w',
                'name': w['name'],
                'modified': w['modified'],
                'disabled': w['disabled'],
                'wkt': 'POLYGON({})'.format(way_to_wkt(nodes, w['nodes']))
        }

    # submit modifications to the database
    cur = g.conn.cursor()
    added = 0
    updated = 0
    free_id = None
    for r_id, region in regions.items():
        if not region['modified']:
            continue
        try:
            region_id = create_or_update_region(region, free_id)
        except psycopg2.Error as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            return import_error('Database error. See server log for details')
        if region_id < 0:
            added += 1
            if free_id is None:
                free_id = region_id - 1
            else:
                free_id -= 1
        else:
            updated += 1
    g.conn.commit()
    return jsonify(regions=len(regions), added=added, updated=updated)

def get_free_id():
    cursor = g.conn.cursor()
    table = config.TABLE
    cursor.execute(f"SELECT min(id) FROM {table} WHERE id < -1000000000")
    min_id = cursor.fetchone()[0]
    free_id = min_id - 1 if min_id else -1_000_000_001
    return free_id

def assign_region_to_lowerst_parent(region_id):
    pot_parents = find_potential_parents(region_id)
    if pot_parents:
        # potential_parents are sorted by area ascending
        parent_id = pot_parents[0]['properties']['id']
        cursor = g.conn.cursor()
        table = config.TABLE
        cursor.execute(f"""
            UPDATE {table}
            SET parent_id = %s
            WHERE id = %s
            """, (parent_id, region_id)
        )
        return True
    return False

def create_or_update_region(region, free_id):
    cursor = g.conn.cursor()
    table = config.TABLE
    osm_table = config.OSM_TABLE
    if region['id'] < 0:
        if not free_id:
            free_id = get_free_id()
        region_id = free_id

        cursor.execute(f"""
            INSERT INTO {table}
                (id, name, disabled, geom, modified, count_k)
            VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), now(), -1)
            """, (region_id, region['name'], region['disabled'], region['wkt'])
        )
        assign_region_to_lowerst_parent(region_id)
        return region_id
    else:
        cursor.execute(f"SELECT count(1) FROM {table} WHERE id = %s",
                       (-region['id'],)
        )
        rec = cursor.fetchone()
        if rec[0] == 0:
            raise Exception("Can't find border ({region['id']}) for update")
        cursor.execute(f"""
            UPDATE {table}
            SET disabled = %s,
                name = %s,
                modified = now(),
                count_k = -1,
                geom = ST_GeomFromText(%s, 4326)
            WHERE id = %s
            """, (region['disabled'], region['name'],
                  region['wkt'], -region['id'])
        )
        return region['id']

def find_potential_parents(region_id):
    table = config.TABLE
    osm_table = config.OSM_TABLE
    p_geogr = "geography(p.geom)"
    c_geogr = "geography(c.geom)"
    cursor = g.conn.cursor()
    query = f"""
        SELECT
          p.id,
          p.name,
          (SELECT admin_level FROM {osm_table} WHERE osm_id = p.id) admin_level,
          ST_AsGeoJSON(ST_SimplifyPreserveTopology(p.geom, 0.01)) geometry
        FROM {table} p, {table} c
        WHERE c.id = %s
            AND ST_Intersects(p.geom, c.geom)
            AND ST_Area({p_geogr}) > ST_Area({c_geogr})
            AND ST_Area(ST_Intersection({p_geogr}, {c_geogr})) >
                    0.5 * ST_Area({c_geogr})
        ORDER BY ST_Area({p_geogr})
    """
    cursor.execute(query, (region_id,))
    parents = []
    for rec in cursor:
        props = {
                'id': rec[0],
                'name': rec[1],
                'admin_level': rec[2],
        }
        feature = {'type': 'Feature',
                   'geometry': json.loads(rec[3]),
                   'properties': props
                  }
        parents.append(feature)
    return parents

@app.route('/potential_parents')
def potential_parents():
    region_id = int(request.args.get('id'))
    parents = find_potential_parents(region_id)
    return jsonify(
            status='ok',
            parents=parents
            #geojson={'type':'FeatureCollection', 'features': borders}
    )


@app.route('/poly')
def export_poly():
    table = request.args.get('table')
    if table in config.OTHER_TABLES:
        table = config.OTHER_TABLES[table]
    else:
        table = config.TABLE

    fetch_borders_args = {'table': table,  'only_leaves': True}

    if 'xmin' in request.args:
        xmin = request.args.get('xmin')
        xmax = request.args.get('xmax')
        ymin = request.args.get('ymin')
        ymax = request.args.get('ymax')
        fetch_borders_args['where_clause'] = (
                f'geom && ST_MakeBox2D(ST_Point({xmin}, {ymin}),'
                                     f'ST_Point({xmax}, {ymax}))'
        )
    borders = fetch_borders(**fetch_borders_args)

    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for border in borders:
            geometry = border['geometry']
            polygons = ([geometry['coordinates']]
                        if geometry['type'] == 'Polygon'
                        else geometry['coordinates'])
            # sanitize name, src: http://stackoverflow.com/a/295466/1297601
            name = border['properties']['name'] or str(-border['properties']['id'])
            fullname = get_region_full_name(border['properties']['id'])
            filename = unidecode(fullname)
            filename = re.sub('[^\w _-]', '', filename).strip()
            filename = filename + '.poly'

            poly = io.BytesIO()
            poly.write(name.encode() + b'\n')
            pcounter = 1
            for polygon in polygons:
                outer = True
                for ring in polygon:
                    poly.write('{inner_mark}{name}\n'.format(
                        inner_mark=('' if outer else '!'),
                        name=(pcounter if outer else -pcounter)
                    ).encode())
                    pcounter = pcounter + 1
                    for coord in ring:
                        poly.write('\t{:E}\t{:E}\n'.format(coord[0], coord[1]).encode())
                    poly.write(b'END\n')
                    outer = False
            poly.write(b'END\n')
            zf.writestr(filename, poly.getvalue())
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


@app.route('/border')
def border():
    region_id = int(request.args.get('id'))
    table = config.TABLE
    simplify_level = request.args.get('simplify')
    simplify = simplify_level_to_postgis_value(simplify_level)
    borders = fetch_borders(
        table=table,
        simplify=simplify,
        only_leaves=False,
        where_clause=f'id = {region_id}'
    )
    if not borders:
        return jsonify(status=f'No border with id={region_id} found')
    return jsonify(status='ok', geojson=borders[0])

@app.route('/start_over')
def start_over():
    try:
        create_countries_initial_structure(g.conn)
    except CountryStructureException as e:
        return jsonify(status=str(e))

    autosplit_table = config.AUTOSPLIT_TABLE
    cursor = g.conn.cursor()
    cursor.execute(f"DELETE FROM {autosplit_table}")
    g.conn.commit()
    return jsonify(status='ok')


if __name__ == '__main__':
    app.run(threaded=True)
