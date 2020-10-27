#!/usr/bin/python3.6
import io
import itertools
import re
import sys, traceback
import zipfile
from unidecode import unidecode

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
from auto_split import split_region
from countries_structure import (
    CountryStructureException,
    create_countries_initial_structure,
    get_osm_border_name_by_osm_id,
    get_region_country,
    get_region_full_name,
    get_similar_regions,
    is_administrative_region,
)
from osm_xml import (
    borders_from_xml,
    borders_to_xml,
    lines_to_xml,
)
from subregions import (
    get_subregions_info,
    update_border_mwm_size_estimation,
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
               (CASE WHEN area = 'NaN'::DOUBLE PRECISION THEN 0 ELSE area END) AS area,
               id, admin_level, parent_id, parent_name, parent_admin_level,
               mwm_size_est
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
               ) AS parent_name,
               ( SELECT admin_level FROM {osm_table}
                 WHERE osm_id = (SELECT parent_id FROM {table} WHERE id = t.id)
               ) AS parent_admin_level,
               mwm_size_est
            FROM {table} t
            WHERE ({where_clause}) {leaves_filter}
        ) q
        ORDER BY area DESC
        """
    cur = g.conn.cursor()
    cur.execute(query)
    borders = []
    for rec in cur:
        region_id = rec[8]
        country_id, country_name = get_region_country(g.conn, region_id)
        props = { 'name': rec[0] or '', 'nodes': rec[2], 'modified': rec[3],
                  'disabled': rec[4], 'count_k': rec[5],
                  'comment': rec[6],
                  'area': rec[7],
                  'id': region_id,
                  'admin_level': rec[9],
                  'parent_id': rec[10],
                  'parent_name': rec[11],
                  'parent_admin_level': rec[12],
                  'country_id': country_id,
                  'country_name': country_name,
                  'mwm_size_est': rec[13]
                }
        feature = {'type': 'Feature',
                   'geometry': json.loads(rec[1]),
                   'properties': props
                  }
        borders.append(feature)
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
        geojson={'type': 'FeatureCollection', 'features': borders}
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
    cur.execute(f"""
        SELECT name, round(ST_Area(geography(ring))) as area,
               ST_X(ST_Centroid(ring)), ST_Y(ST_Centroid(ring))
        FROM (
            SELECT name, (ST_Dump(geom)).geom as ring
            FROM {table}
            WHERE geom && ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s))
        ) g
        WHERE ST_Area(geography(ring)) < %s
        """, (xmin, ymin, xmax, ymax, config.SMALL_KM2 * 1000000)
    )
    result = []
    for rec in cur:
        result.append({ 'name': rec[0], 'area': rec[1],
                        'lon': float(rec[2]), 'lat': float(rec[3]) })
    return jsonify(features=result)

@app.route('/config')
def get_server_configuration():
    osm = False
    backup = False
    old = []
    try:
        cur = g.conn.cursor()
        cur.execute(f"""SELECT osm_id, ST_Area(way), admin_level, name
                        FROM {config.OSM_TABLE} LIMIT 2""")
        if cur.rowcount == 2:
            osm = True
    except psycopg2.Error as e:
        pass
    try:
        cur.execute(f"""SELECT backup, id, name, parent_id, ST_Area(geom),
                              modified, disabled, count_k, cmnt
                       FROM {config.BACKUP} LIMIT 2""")
        backup = True
    except psycopg2.Error as e:
        pass
    for t, tname in config.OTHER_TABLES.items():
        try:
            cur.execute(f"""SELECT name, ST_Area(geom), modified, disabled,
                                   count_k, cmnt
                            FROM {tname} LIMIT 2""")
            if cur.rowcount == 2:
                old.append(t)
        except psycopg2.Error as e:
            pass
    return jsonify(osm=osm, tables=old,
                   readonly=config.READONLY,
                   backup=backup,
                   mwm_size_thr=config.MWM_SIZE_THRESHOLD)

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
    cur.execute(f"""
        SELECT ST_NumGeometries(geom) FROM {table} WHERE id = %s
        """, (region_id,)
    )
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
        cur.execute(f"""
            SELECT name, parent_id, disabled FROM {table} WHERE id = %s
            """, (region_id,))
        name, parent_id, disabled = cur.fetchone()
        if save_region:
            parent_id = region_id
        else:
            cur.execute(f"DELETE FROM {table} WHERE id = %s", (region_id,))
        base_name = name
        # insert new geometries
        counter = 1
        new_ids = []
        free_id = get_free_id()
        for geom in geometries:
            cur.execute(f"""
                INSERT INTO {table} (id, name, geom, disabled, count_k, modified, parent_id)
                    VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, -1, now(), %s)
                """, (free_id, f'{base_name}_{counter}', geom, disabled, parent_id)
            )
            new_ids.append(free_id)
            counter += 1
            free_id -= 1
        warnings = []
        for border_id in new_ids:
            try:
                update_border_mwm_size_estimation(g.conn, border_id)
            except Exception as e:
                warnings.append(str(e))
        g.conn.commit()
    return jsonify(status='ok', warnings=warnings)

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
                    geom = ST_Union({table}.geom, b2.geom),
                    mwm_size_est = {table}.mwm_size_est + b2.mwm_size_est,
                    count_k = -1
                FROM (SELECT geom, mwm_size_est FROM {table} WHERE id = %s) AS b2
                WHERE id = %s""", (region_id2, region_id1)
        )
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
        """, (region_id,))
    rec = cursor.fetchone()
    parent_id = int(rec[0]) if rec and rec[0] is not None else None
    return parent_id

def get_child_region_ids(region_id):
    cursor = g.conn.cursor()
    cursor.execute(f"""
        SELECT id FROM {config.TABLE} WHERE parent_id = %s
        """, (region_id,))
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
        return jsonify(status=f"Region {region_id} does not exist or has no parent")
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
            DELETE FROM {config.TABLE} WHERE id IN ({ids_str})"""
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
    cur.execute(f"""
        SELECT osm_id, name, admin_level,
                (CASE
                    WHEN ST_Area(geography(way)) = 'NaN'::DOUBLE PRECISION THEN 0
                    ELSE ST_Area(geography(way))/1000000
                END) AS area_km
        FROM {config.OSM_TABLE} 
        WHERE ST_Contains(way, ST_SetSRID(ST_Point(%s, %s), 4326))
        ORDER BY admin_level DESC, name ASC
        """, (lon, lat)
    )
    result = []
    for rec in cur:
        border = {'id': rec[0], 'name': rec[1],
                  'admin_level': rec[2], 'area': rec[3]}
        result.append(border)
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
    warnings = []
    try:
        update_border_mwm_size_estimation(g.conn, osm_id)
    except Exception as e:
        warnings.append(str(e))
    g.conn.commit()
    return jsonify(status='ok', warnings=warnings)

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
    cur.execute(f"DELETE FROM {config.TABLE} WHERE id = %s", (region_id,))
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/disable')
def disable_border():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {config.TABLE} SET disabled = true WHERE id = %s",
                (region_id,))
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/enable')
def enable_border():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {config.TABLE} SET disabled = false WHERE id = %s",
                (region_id,))
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/comment', methods=['POST'])
def update_comment():
    region_id = int(request.form['id'])
    comment = request.form['comment']
    cur = g.conn.cursor()
    cur.execute(f"UPDATE {config.TABLE} SET cmnt = %s WHERE id = %s",
                (comment, region_id))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/divpreview')
def divide_preview():
    region_id = int(request.args.get('id'))
    try:
        next_level = int(request.args.get('next_level'))
    except ValueError:
        return jsonify(status="Not a number in next level")
    is_admin_region = is_administrative_region(g.conn, region_id)
    region_ids = [region_id]
    apply_to_similar = (request.args.get('apply_to_similar') == 'true')
    if apply_to_similar:
        if not is_admin_region:
            return jsonify(status="Could not use 'apply to similar' for non-administrative regions")
        region_ids = get_similar_regions(g.conn, region_id, only_leaves=True)
    auto_divide = (request.args.get('auto_divide') == 'true')
    if auto_divide:
        if not is_admin_region:
            return jsonify(status="Could not apply auto-division to non-administrative regions")
        try:
            mwm_size_thr = int(request.args.get('mwm_size_thr'))
        except ValueError:
            return jsonify(status="Not a number in thresholds")
        return divide_into_clusters_preview(
                region_ids, next_level,
                mwm_size_thr)
    else:
        return divide_into_subregions_preview(region_ids, next_level)

def get_subregions_for_preview(region_ids, next_level):
    subregions = list(itertools.chain.from_iterable(
        get_subregions_one_for_preview(region_id, next_level)
            for region_id in region_ids
    ))
    return subregions

def get_subregions_one_for_preview(region_id, next_level):
    osm_table = config.OSM_TABLE
    table = config.TABLE
    cur = g.conn.cursor()
    # We use ST_SimplifyPreserveTopology, since ST_Simplify would give NULL
    # for very little regions.
    cur.execute(f"""
        SELECT name,
               ST_AsGeoJSON(ST_SimplifyPreserveTopology(way, 0.01)) as way,
               osm_id
        FROM {osm_table}
        WHERE ST_Contains(
                (SELECT geom FROM {table} WHERE id = %s), way
              )
            AND admin_level = %s
        """, (region_id, next_level)
    )
    subregions = []
    for rec in cur:
        feature = {'type': 'Feature', 'geometry': json.loads(rec[1]),
                   'properties': {'name': rec[0]}}
        subregions.append(feature)
    return subregions

def get_clusters_for_preview(region_ids, next_level, thresholds):
    clusters = list(itertools.chain.from_iterable(
        get_clusters_for_preview_one(region_id, next_level, thresholds)
            for region_id in region_ids
    ))
    return clusters

def get_clusters_for_preview_one(region_id, next_level, mwm_size_thr):
    autosplit_table = config.AUTOSPLIT_TABLE
    cursor = g.conn.cursor()
    where_clause = f"""
        osm_border_id = %s
        AND mwm_size_thr = %s
        """
    splitting_sql_params = (region_id, mwm_size_thr)
    cursor.execute(f"""
        SELECT 1 FROM {autosplit_table}
        WHERE {where_clause}
        """, splitting_sql_params
    )
    if cursor.rowcount == 0:
        split_region(g.conn, region_id, next_level, mwm_size_thr)

    cursor.execute(f"""
        SELECT subregion_ids[1],
               ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.01)) as way
        FROM {autosplit_table}
        WHERE {where_clause}
        """, splitting_sql_params
    )
    clusters = []
    for rec in cursor:
        cluster = {
            'type': 'Feature',
            'geometry': json.loads(rec[1]),
            'properties': {'osm_id': int(rec[0])}
        }
        clusters.append(cluster)
    return clusters

def divide_into_subregions_preview(region_ids, next_level):
    subregions = get_subregions_for_preview(region_ids, next_level)
    return jsonify(
        status='ok',
        subregions={'type': 'FeatureCollection', 'features': subregions}
    )

def divide_into_clusters_preview(region_ids, next_level, mwm_size_thr):
    subregions = get_subregions_for_preview(region_ids, next_level)
    clusters = get_clusters_for_preview(region_ids, next_level, mwm_size_thr)
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
    try:
        next_level = int(request.args.get('next_level'))
    except ValueError:
        return jsonify(status="Not a number in next level")
    is_admin_region = is_administrative_region(g.conn, region_id)
    apply_to_similar = (request.args.get('apply_to_similar') == 'true')
    region_ids = [region_id]
    if apply_to_similar:
        if not is_admin_region:
            return jsonify(status="Could not use 'apply to similar' for non-administrative regions")
        region_ids = get_similar_regions(g.conn, region_id, only_leaves=True)
    auto_divide = (request.args.get('auto_divide') == 'true')
    if auto_divide:
        if not is_admin_region:
            return jsonify(status="Could not apply auto-division to non-administrative regions")
        try:
            mwm_size_thr = int(request.args.get('mwm_size_thr'))
        except ValueError:
            return jsonify(status="Not a number in thresholds")
        return divide_into_clusters(
                region_ids, next_level,
                mwm_size_thr)
    else:
        return divide_into_subregions(region_ids, next_level)

def divide_into_subregions(region_ids, next_level):
    for region_id in region_ids:
        divide_into_subregions_one(region_id, next_level)
    g.conn.commit()
    return jsonify(status='ok')

def divide_into_subregions_one(region_id, next_level):
    table = config.TABLE
    osm_table = config.OSM_TABLE
    subregions = get_subregions_info(g.conn, region_id, table,
                                     next_level, need_cities=False)
    cursor = g.conn.cursor()
    is_admin_region = is_administrative_region(g.conn, region_id)
    if is_admin_region:
        for subregion_id, data in subregions.items():
            cursor.execute(f"""
                INSERT INTO {table}
                    (id, geom, name, parent_id, modified, count_k, mwm_size_est)
                SELECT osm_id, way, name, %s, now(), -1, {data['mwm_size_est']}
                FROM {osm_table}
                WHERE osm_id = %s
                """, (region_id, subregion_id)
            )
    else:
        for subregion_id, data in subregions.items():
            cursor.execute(f"""
                INSERT INTO {table}
                    (id, geom, name, parent_id, modified, count_k, mwm_size_est)
                SELECT osm_id, way, name,
                       (SELECT parent_id FROM {table} WHERE id = %s),
                       now(), -1, {data['mwm_size_est']}
                FROM {osm_table}
                WHERE osm_id = %s
                """, (region_id, subregion_id)
            )
        cursor.execute(f"DELETE FROM {table} WHERE id = %s", (region_id,))

def divide_into_clusters(region_ids, next_level, mwm_size_thr):
    table = config.TABLE
    autosplit_table = config.AUTOSPLIT_TABLE
    cursor = g.conn.cursor()
    insert_cursor = g.conn.cursor()
    for region_id in region_ids:
        cursor.execute(f"SELECT name FROM {table} WHERE id = %s", (region_id,))
        base_name = cursor.fetchone()[0]

        where_clause = f"""
            osm_border_id = %s
            AND mwm_size_thr = %s
            """
        splitting_sql_params = (region_id, mwm_size_thr)
        cursor.execute(f"""
            SELECT 1 FROM {autosplit_table}
            WHERE {where_clause}
            """, splitting_sql_params
        )
        if cursor.rowcount == 0:
            split_region(g.conn, region_id, next_level, mwm_size_thr)

        free_id = get_free_id()
        counter = 0
        cursor.execute(f"""
            SELECT subregion_ids
            FROM {autosplit_table} WHERE {where_clause}
            """, splitting_sql_params
        )
        if cursor.rowcount == 1:
            continue
        for rec in cursor:
            subregion_ids = rec[0]
            cluster_id = subregion_ids[0]
            if len(subregion_ids) == 1:
                subregion_id = cluster_id
                name = get_osm_border_name_by_osm_id(g.conn, subregion_id)
            else:
                counter += 1
                free_id -= 1
                subregion_id = free_id
                name = f"{base_name}_{counter}"
            insert_cursor.execute(f"""
                INSERT INTO {table} (id, name, parent_id, geom, modified, count_k, mwm_size_est)
                SELECT {subregion_id}, %s, osm_border_id, geom, now(), -1, mwm_size_est
                FROM {autosplit_table} WHERE subregion_ids[1] = %s AND {where_clause}
                """, (name, cluster_id,) + splitting_sql_params
            )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/chop1')
def chop_largest_or_farthest():
    if config.READONLY:
        abort(405)
    region_id = int(request.args.get('id'))
    table = config.TABLE
    cur = g.conn.cursor()
    cur.execute(f"""SELECT ST_NumGeometries(geom)
                    FROM {table}
                    WHERE id = {region_id}""")
    res = cur.fetchone()
    if not res or res[0] < 2:
        return jsonify(status='border should have more than one outer ring')
    free_id1 = get_free_id()
    free_id2 = free_id1 - 1
    cur.execute(f"""
        INSERT INTO {table} (id, parent_id, name, disabled, modified, geom)
            SELECT id, region_id, name, disabled, modified, geom FROM
            (
                (WITH w AS (SELECT name, disabled, (ST_Dump(geom)).geom AS g
                            FROM {table} WHERE id = {region_id})
                (SELECT {free_id1} id, {region_id} region_id, name||'_main' as name, disabled,
                        now() as modified, g as geom, ST_Area(g) as a
                 FROM w ORDER BY a DESC LIMIT 1)
                UNION ALL
                SELECT {free_id2} id, {region_id} region_id, name||'_small' as name, disabled,
                       now() as modified, ST_Collect(g) AS geom,
                       ST_Area(ST_Collect(g)) as a
                FROM (SELECT name, disabled, g, ST_Area(g) AS a FROM w ORDER BY a DESC OFFSET 1) ww
                GROUP BY name, disabled)
            ) x"""
    )
    warnings = []
    for border_id in (free_id1, free_id2):
        try:
            update_border_mwm_size_estimation(g.conn, border_id)
        except Exception as e:
            warnings.append(str(e))
    g.conn.commit()
    return jsonify(status='ok', warnings=warnings)

@app.route('/hull')
def draw_hull():
    if config.READONLY:
        abort(405)
    border_id = int(request.args.get('id'))
    cursor = g.conn.cursor()
    table = config.TABLE
    cursor.execute(f"SELECT ST_NumGeometries(geom) FROM {table} WHERE id = %s",
                   (border_id,))
    res = cursor.fetchone()
    if not res or res[0] < 2:
        return jsonify(status='border should have more than one outer ring')
    cursor.execute(f"""
        UPDATE {table} SET geom = ST_ConvexHull(geom)
        WHERE id = %s""", (border_id,)
    )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/backup')
def backup_do():
    if config.READONLY:
        abort(405)
    cur = g.conn.cursor()
    cur.execute(f"""SELECT to_char(now(), 'IYYY-MM-DD HH24:MI'), max(backup)
                    FROM {config.BACKUP}""")
    (timestamp, tsmax) = cur.fetchone()
    if timestamp == tsmax:
        return jsonify(status="please try again later")
    backup_table = config.BACKUP
    table = config.TABLE
    cur.execute(f"""
        INSERT INTO {backup_table}
             (backup, id, name, parent_id, geom, disabled, count_k,
                modified, cmnt, mwm_size_est)
          SELECT %s, id, name, parent_id, geom, disabled, count_k,
                modified, cmnt, mwm_size_est
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
        return jsonify(status="no such timestamp")
    cur.execute(f"DELETE FROM {table}")
    cur.execute(f"""
        INSERT INTO {table}
            (id, name, parent_id, geom, disabled, count_k, modified, cmnt, mwm_size_est)
          SELECT id, name, parent_id, geom, disabled, count_k, modified, cmnt, mwm_size_est
          FROM {backup_table}
          WHERE backup = %s
        """, (ts,)
    )
    g.conn.commit()
    return jsonify(status='ok')

@app.route('/backlist')
def backup_list():
    cur = g.conn.cursor()
    cur.execute(f"""SELECT backup, count(1)
                    FROM {config.BACKUP}
                    GROUP BY backup
                    ORDER BY backup DESC""")
    result = []
    for res in cur:
        result.append({'timestamp': res[0], 'text': res[0], 'count': res[1]})
    # todo: count number of different objects for the last one
    return jsonify(backups=result)

@app.route('/backdelete')
def backup_delete():
    if config.READONLY:
        abort(405)
    ts = request.args.get('timestamp')
    cur = g.conn.cursor()
    cur.execute(f"SELECT count(1) FROM {config.BACKUP} WHERE backup = %s", (ts,))
    (count,) = cur.fetchone()
    if count <= 0:
        return jsonify(status='no such timestamp')
    cur.execute(f"DELETE FROM {config.BACKUP} WHERE backup = %s", (ts,))
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
        where_clause=f"geom && ST_MakeBox2D(ST_Point({xmin}, {ymin}),"
                                          f"ST_Point({xmax}, {ymax}))"
    )
    xml = borders_to_xml(borders)
    return Response(xml, mimetype='application/x-osm+xml')

@app.route('/josmbord')
def josm_borders_along():
    region_id = int(request.args.get('id'))
    line = request.args.get('line')
    cursor = g.conn.cursor()
    # select all outer osm borders inside a buffer of the given line
    table = config.TABLE
    osm_table = config.OSM_TABLE
    cursor.execute(f"""
        WITH linestr AS (
            SELECT ST_Intersection(geom, ST_Buffer(ST_GeomFromText(%s, 4326), 0.2)) as line
            FROM {table}
            WHERE id = %s
        ), osmborders AS (
            SELECT (ST_Dump(way)).geom as g
            FROM {osm_table}, linestr
            WHERE ST_Intersects(line, way)
        )
        SELECT ST_AsGeoJSON((ST_Dump(ST_LineMerge(ST_Intersection(ST_Collect(ST_ExteriorRing(g)), line)))).geom)
        FROM osmborders, linestr
        GROUP BY line
        """, (line, region_id)
    )
    xml = lines_to_xml(rec[0] for rec in cursor)
    return Response(xml, mimetype='application/x-osm+xml')


def import_error(msg):
    if config.IMPORT_ERROR_ALERT:
        return f'<script>alert("{msg}");</script>'
    else:
        return jsonify(status=msg)


@app.route('/import', methods=['POST'])
def import_osm():
    # Though this variable is not used it's necessary to consume request.data
    # so that nginx doesn't produce error like "#[error] 13#13: *65 readv()
    # failed (104: Connection reset by peer) while reading upstream"
    data = request.data

    if config.READONLY:
        abort(405)
    if not LXML:
        return import_error("importing is disabled due to absent lxml library")
    f = request.files['file']
    if not f:
        return import_error("failed upload")
    try:
        tree = etree.parse(f)
    except:
        return import_error("malformed xml document")
    if not tree:
        return import_error("bad document")

    result = borders_from_xml(tree)
    if type(result) == 'str':
        return import_error(result)
    regions = result

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
            return import_error("Database error. See server log for details")
        except Exception as e:
            return import_error(f"Import error: {str(e)}")
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
                       (-region['id'],))
        rec = cursor.fetchone()
        if rec[0] == 0:
            raise Exception(f"Can't find border ({region['id']}) for update")
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
    return jsonify(status='ok', parents=parents)

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
                f"geom && ST_MakeBox2D(ST_Point({xmin}, {ymin}),"
                                     f"ST_Point({xmax}, {ymax}))"
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
            fullname = get_region_full_name(g.conn, border['properties']['id'])
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
        cur.execute(f"SELECT count(1) FROM {table}")
        return jsonify(total=cur.fetchone()[0])
    elif group == 'sizes':
        cur.execute(f"""
            SELECT name, count_k, ST_NPoints(geom), ST_AsGeoJSON(ST_Centroid(geom)),
                (CASE
                    WHEN ST_Area(geography(geom)) = 'NaN'::DOUBLE PRECISION THEN 0
                    ELSE ST_Area(geography(geom)) / 1000000
                 END) AS area,
                 disabled,
                 (CASE
                     WHEN coalesce(cmnt, '') = '' THEN false
                     ELSE true
                  END) AS cmnt
            FROM {table}"""
        )
        result = []
        for res in cur:
            coord = json.loads(res[3])['coordinates']
            result.append({'name': res[0], 'lat': coord[1], 'lon': coord[0],
                           'size': res[1], 'nodes': res[2], 'area': res[4],
                           'disabled': res[5], 'commented': res[6]})
        return jsonify(regions=result)
    elif group == 'topo':
        cur.execute(f"""
            SELECT name, count(1),
                min(
                    CASE
                        WHEN ST_Area(geography(g)) = 'NaN'::DOUBLE PRECISION THEN 0
                        ELSE ST_Area(geography(g))
                    END
                ) / 1000000,
                sum(ST_NumInteriorRings(g)), ST_AsGeoJSON(ST_Centroid(ST_Collect(g)))
            FROM (SELECT name, (ST_Dump(geom)).geom AS g FROM {table}) a
            GROUP BY name"""
        )
        result = []
        for (name, outer, min_area, inner, coords) in cur:
            coord = json.loads(coords)['coordinates']
            result.append({'name': name, 'outer': outer, 'min_area': min_area,
                           'inner': inner, 'lon': coord[0], 'lat': coord[1]})
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
        warnings = create_countries_initial_structure(g.conn)
    except CountryStructureException as e:
        return jsonify(status=str(e))

    autosplit_table = config.AUTOSPLIT_TABLE
    cursor = g.conn.cursor()
    cursor.execute(f"DELETE FROM {autosplit_table}")
    g.conn.commit()
    return jsonify(status='ok', warnings=warnings[:10])


if __name__ == '__main__':
    app.run(threaded=True)
