#!/usr/bin/python3
import io
import re
import sys, traceback
import zipfile
from functools import wraps
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
from borders_api_utils import *
from countries_structure import (
    auto_divide_country,
    CountryStructureException,
    create_countries_initial_structure,
)
from osm_xml import (
    borders_from_xml,
    borders_to_xml,
    lines_to_xml,
)
from simple_splitting import simple_split
from subregions import (
    get_child_region_ids,
    get_parent_region_id,
    get_region_full_name,
    get_similar_regions,
    is_administrative_region,
    update_border_mwm_size_estimation,
)


try:
    from lxml import etree
    LXML = True
except:
    LXML = False


app = Flask(__name__)
app.debug = config.DEBUG
Compress(app)
CORS(app)
app.config['JSON_AS_ASCII'] = False


def validate_args_types(**expected_types):
    """This decorator does early server method termination
    if some args from request.args cannot be converted into expected types.

    expected_types example: id=int, size=float, parent_id=(None, int)
    Only one type or tuple of the form (None, some_type) is allowed.
    """
    def f_with_validation(f):
        @wraps(f)
        def inner(*args, **kwargs):
            args_ok = True
            for arg, arg_type in expected_types.items():
                if isinstance(arg_type, tuple):
                    assert len(arg_type) == 2 and arg_type[0] is None
                    arg_type = arg_type[1]
                    allow_None = True
                else:
                    assert arg_type is not None
                    allow_None = False

                arg_value = request.args.get(arg)
                if allow_None and arg_value is None:
                    continue
                try:
                    arg_type(arg_value)
                except (TypeError, ValueError):
                    args_ok = False
                    break
            if not args_ok:
                return abort(400)
            return f(*args, **kwargs)
        return inner

    return f_with_validation


def check_write_access(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if config.READONLY:
            abort(403)
        else:
            return f(*args, **kwargs)
    return inner


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


@app.route('/bbox')
@validate_args_types(xmin=float, xmax=float, ymin=float, ymax=float)
def query_bbox():
    xmin = request.args.get('xmin')
    xmax = request.args.get('xmax')
    ymin = request.args.get('ymin')
    ymax = request.args.get('ymax')
    simplify_level = request.args.get('simplify')
    simplify = simplify_level_to_postgis_value(simplify_level)
    borders_table = request.args.get('table')
    borders_table = config.OTHER_TABLES.get(borders_table, config.BORDERS_TABLE)
    borders = fetch_borders(
        table=borders_table,
        simplify=simplify,
        where_clause=geom_intersects_bbox_sql(xmin, ymin, xmax, ymax)
    )
    return jsonify(
        status='ok',
        geojson={'type': 'FeatureCollection', 'features': borders}
    )


@app.route('/small')
@validate_args_types(xmin=float, xmax=float, ymin=float, ymax=float)
def query_small_in_bbox():
    xmin = request.args.get('xmin')
    xmax = request.args.get('xmax')
    ymin = request.args.get('ymin')
    ymax = request.args.get('ymax')
    borders_table = request.args.get('table')
    borders_table = config.OTHER_TABLES.get(borders_table, config.BORDERS_TABLE)
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT id, name, ST_Area(geography(ring))/1E6 AS area,
                   ST_X(ST_Centroid(ring)), ST_Y(ST_Centroid(ring))
            FROM (
                SELECT id, name, (ST_Dump(geom)).geom AS ring
                FROM {borders_table}
                WHERE {geom_intersects_bbox_sql(xmin, ymin, xmax, ymax)}
            ) g
            WHERE ST_Area(geography(ring))/1E6 < %s
            """, (config.SMALL_KM2,)
        )
        rings = []
        for border_id, name, area, lon, lat in cursor:
            rings.append({
                'id': border_id,
                'name': name,
                'area': area,
                'lon': lon,
                'lat': lat
            })
        return jsonify(rings=rings)


@app.route('/config')
def get_server_configuration():
    osm = False
    backup = False
    old = []
    with g.conn.cursor() as cursor:
        try:
            cursor.execute(f"""SELECT osm_id, ST_Area(way), admin_level, name
                               FROM {config.OSM_TABLE} LIMIT 2""")
            if cursor.rowcount == 2:
                osm = True
        except psycopg2.Error as e:
            pass
        try:
            cursor.execute(f"""SELECT backup, id, name, parent_id, cmnt,
                                  modified, disabled, count_k, ST_Area(geom)
                           FROM {config.BACKUP} LIMIT 2""")
            backup = True
        except psycopg2.Error as e:
            pass
        for t, tname in config.OTHER_TABLES.items():
            try:
                cursor.execute(f"""SELECT name, ST_Area(geom), modified,
                                          disabled, count_k, cmnt
                                FROM {tname} LIMIT 2""")
                if cursor.rowcount == 2:
                    old.append(t)
            except psycopg2.Error as e:
                pass
    return jsonify(osm=osm, tables=old,
                   readonly=config.READONLY,
                   backup=backup,
                   mwm_size_thr=config.MWM_SIZE_THRESHOLD)


def prepare_sql_search_string(string):
    if string.startswith('^'):
        string = string[1:]
    else:
        string = f"%{string}"
    if string.endswith('$'):
        string = string[:-1]
    else:
        string = f"{string}%"
    return string


@app.route('/search')
def search():
    query = request.args.get('q')
    # query may contain region id or a part of its name
    try:
        region_id = int(query)
        search_value = region_id
        is_id = True
    except ValueError:
        search_value = prepare_sql_search_string(query)
        is_id = False

    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom)
            FROM {config.BORDERS_TABLE}
            WHERE {'id =' if is_id else 'name ILIKE'} %s
            ORDER BY (ST_Area(geography(geom)))
            LIMIT 1""", (search_value,)
        )
        if cursor.rowcount > 0:
            rec = cursor.fetchone()
            return jsonify(status='ok', bounds=rec)
    return jsonify(status='not found')


@app.route('/simple_split')
@check_write_access
@validate_args_types(id=int)
def simple_split_endpoint():
    """Split into 2/4 parts with straight lines"""
    region_id = int(request.args.get('id'))
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT name, mwm_size_est
            FROM {config.BORDERS_TABLE}
            WHERE id = %s""", (region_id,))
        if cursor.rowcount == 0:
            return jsonify(status=f"Region {region_id} not found")
        name, mwm_size_est = cursor.fetchone()
        if mwm_size_est is None:
            mwm_size_est = update_border_mwm_size_estimation(g.conn, region_id)
            if mwm_size_est is not None:
                return jsonify(status='MWM size estimation was updated')
            else:
                return jsonify(status="Cannot esitmate region mwm size")
        region = {
            'id': region_id,
            'name': name,
            'mwm_size_est': mwm_size_est,
        }

    if simple_split(g.conn, region):
        g.conn.commit()
        return jsonify(status='ok')
    return jsonify(status="Can't split region into parts")


@app.route('/split')
@check_write_access
@validate_args_types(id=int)
def split():
    region_id = int(request.args.get('id'))
    line = request.args.get('line')
    save_region = (request.args.get('save_region') == 'true')
    borders_table = config.BORDERS_TABLE
    warnings = []
    with g.conn.cursor() as cursor:
        # check that we're splitting a single polygon
        cursor.execute(f"""
            SELECT ST_NumGeometries(geom) FROM {borders_table} WHERE id = %s
            """, (region_id,)
        )
        res = cursor.fetchone()
        if not res or res[0] != 1:
            return jsonify(status='border should have one outer ring')
        cursor.execute(f"""
            SELECT ST_AsText(
                    (ST_Dump(ST_Split(geom, ST_GeomFromText(%s, 4326)))).geom)
            FROM {borders_table}
            WHERE id = %s
            """, (line, region_id)
        )
        if cursor.rowcount > 1:
            # no use of doing anything if the polygon wasn't modified
            geometries = []
            for res in cursor:
                geometries.append(res[0])
            # get region properties and delete old border
            cursor.execute(f"""
                SELECT name, parent_id, disabled FROM {borders_table} WHERE id = %s
                """, (region_id,))
            name, parent_id, disabled = cursor.fetchone()
            if save_region:
                parent_id = region_id
            else:
                cursor.execute(f"DELETE FROM {borders_table} WHERE id = %s",
                               (region_id,))
            base_name = name
            # insert new geometries
            counter = 1
            new_ids = []
            free_id = get_free_id()
            for geom in geometries:
                cursor.execute(f"""
                    INSERT INTO {borders_table} (id, name, geom, disabled,
                                                count_k, modified, parent_id)
                        VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, -1,
                                now(), %s)
                    """, (free_id, f'{base_name}_{counter}', geom,
                          disabled, parent_id)
                )
                new_ids.append(free_id)
                counter += 1
                free_id -= 1
            for border_id in new_ids:
                try:
                    update_border_mwm_size_estimation(g.conn, border_id)
                except Exception as e:
                    warnings.append(str(e))
        g.conn.commit()
    return jsonify(status='ok', warnings=warnings)


@app.route('/join')
@check_write_access
@validate_args_types(id1=int, id2=int)
def join_borders():
    region_id1 = int(request.args.get('id1'))
    region_id2 = int(request.args.get('id2'))
    if region_id1 == region_id2:
        return jsonify(status='failed to join region with itself')
    with g.conn.cursor() as cursor:
        try:
            borders_table = config.BORDERS_TABLE
            joint_id = get_free_id()
            cursor.execute(f"""
                    UPDATE {borders_table}
                    SET id = {joint_id},
                        geom = ST_Union({borders_table}.geom, b2.geom),
                        mwm_size_est = {borders_table}.mwm_size_est + b2.mwm_size_est,
                        count_k = -1
                    FROM (SELECT geom, mwm_size_est FROM {borders_table} WHERE id = %s) AS b2
                    WHERE id = %s""", (region_id2, region_id1)
            )
            cursor.execute(f"DELETE FROM {borders_table} WHERE id = %s", (region_id2,))
        except psycopg2.Error as e:
            g.conn.rollback()
            return jsonify(status=str(e))

        # If joint_id is the only child of its parent, then leave only parent
        parent_id = get_parent_region_id(g.conn, joint_id)
        if parent_id is not None:
            cursor.execute(f"""
                SELECT count(*) FROM {borders_table} WHERE parent_id = %s
                """, (parent_id,)
            )
            children_cnt = cursor.fetchone()[0]
            if children_cnt == 1:
                cursor.execute(f"""
                    UPDATE {borders_table}
                    SET mwm_size_est = (SELECT mwm_size_est
                                        FROM {borders_table}
                                        WHERE id = %s)
                    WHERE id = %s
                    """, (joint_id, parent_id)
                )
                cursor.execute(f"DELETE FROM {borders_table} WHERE id = %s",
                               (joint_id,))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/join_to_parent')
@validate_args_types(id=int)
def join_to_parent():
    """Find all descendants of a region and remove them starting
    from the lowest hierarchical level to not violate 'parent_id'
    foreign key constraint (which is probably not in ON DELETE CASCADE mode)
    """
    region_id = int(request.args.get('id'))
    parent_id = get_parent_region_id(g.conn, region_id)
    if not parent_id:
        return jsonify(status=f"Region {region_id} does not exist or has no parent")

    descendants = [[parent_id]]  # regions ordered by hierarchical level

    while True:
        parent_ids = descendants[-1]
        child_ids = list(itertools.chain.from_iterable(
            get_child_region_ids(g.conn, parent_id) for parent_id in parent_ids
        ))
        if child_ids:
            descendants.append(child_ids)
        else:
            break
    with g.conn.cursor() as cursor:
        borders_table = config.BORDERS_TABLE
        while len(descendants) > 1:
            lowest_ids = descendants.pop()
            ids_str = ','.join(str(x) for x in lowest_ids)
            cursor.execute(f"""
                DELETE FROM {borders_table} WHERE id IN ({ids_str})"""
            )
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/set_parent')
@validate_args_types(id=int, parent_id=(None, int))
def set_parent():
    region_id = int(request.args.get('id'))
    parent_id = request.args.get('parent_id')
    parent_id = int(parent_id) if parent_id else None
    borders_table = config.BORDERS_TABLE
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {borders_table} SET parent_id = %s WHERE id = %s
            """, (parent_id, region_id)
        )
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/point')
@validate_args_types(lat=float, lon=float)
def find_osm_borders():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT osm_id, name, admin_level,
                    (CASE
                        WHEN ST_Area(geography(way)) = 'NaN'::DOUBLE PRECISION
                            THEN 0
                        ELSE ST_Area(geography(way))/1E6
                    END) AS area_km
            FROM {config.OSM_TABLE} 
            WHERE ST_Contains(way, ST_SetSRID(ST_Point(%s, %s), 4326))
            ORDER BY admin_level DESC, name ASC
            """, (lon, lat)
        )
        result = []
        for osm_id, name, admin_level, area in cursor:
            border = {'id': osm_id, 'name': name,
                      'admin_level': admin_level, 'area': area}
            result.append(border)
    return jsonify(borders=result)


@app.route('/from_osm')
@check_write_access
@validate_args_types(id=int)
def copy_from_osm():
    osm_id = int(request.args.get('id'))
    name = request.args.get('name')
    errors, warnings = copy_region_from_osm(g.conn, osm_id, name)
    if errors:
        return jsonify(status='\n'.join(errors))
    g.conn.commit()
    return jsonify(status='ok', warnings=warnings)


@app.route('/rename')
@check_write_access
@validate_args_types(id=int)
def set_name():
    region_id = int(request.args.get('id'))
    borders_table = config.BORDERS_TABLE
    new_name = request.args.get('new_name')
    with g.conn.cursor() as cursor:
        cursor.execute(f"UPDATE {borders_table} SET name = %s WHERE id = %s",
                       (new_name, region_id))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/delete')
@check_write_access
@validate_args_types(id=int)
def delete_border():
    region_id = int(request.args.get('id'))
    with g.conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM {config.BORDERS_TABLE} WHERE id = %s",
                       (region_id,))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/disable')
@check_write_access
@validate_args_types(id=int)
def disable_border():
    region_id = int(request.args.get('id'))
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {config.BORDERS_TABLE}
            SET disabled = true
            WHERE id = %s""", (region_id,))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/enable')
@check_write_access
@validate_args_types(id=int)
def enable_border():
    region_id = int(request.args.get('id'))
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {config.BORDERS_TABLE}
            SET disabled = false
            WHERE id = %s""", (region_id,))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/comment', methods=['POST'])
@validate_args_types(id=int)
def update_comment():
    region_id = int(request.form['id'])
    comment = request.form['comment']
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {config.BORDERS_TABLE}
            SET cmnt = %s WHERE id = %s
            """, (comment, region_id))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/divide_preview')
def divide_preview():
    return divide(preview=True)


@app.route('/divide')
@check_write_access
def divide_do():
    return divide(preview=False)


@validate_args_types(id=int)
def divide(preview=False):
    region_id = int(request.args.get('id'))
    try:
        # TODO: perform next_level field validation on client-side
        ## and move the parameter check to @validate_args_types decorator
        next_level = int(request.args.get('next_level'))
    except ValueError:
        return jsonify(status="Not a number in next level")
    is_admin_region = is_administrative_region(g.conn, region_id)
    region_ids = [region_id]
    apply_to_similar = (request.args.get('apply_to_similar') == 'true')
    if apply_to_similar:
        if not is_admin_region:
            return jsonify(status="Could not use 'apply to similar' "
                                  "for non-administrative regions")
        region_ids = get_similar_regions(g.conn, region_id, only_leaves=True)
    auto_divide = (request.args.get('auto_divide') == 'true')
    if auto_divide:
        if not is_admin_region:
            return jsonify(status="Could not apply auto-division "
                                  "to non-administrative regions")
        try:
            # TODO: perform mwm_size_thr field validation on client-side
            ## and move the parameter check to @validate_args_types decorator
            mwm_size_thr = int(request.args.get('mwm_size_thr'))
        except ValueError:
            return jsonify(status="Not a number in thresholds")
        divide_into_clusters_func = (
            divide_into_clusters_preview
            if preview
            else divide_into_clusters
        )
        return divide_into_clusters_func(
                region_ids, next_level,
                mwm_size_thr)
    else:
        divide_into_subregions_func = (
            divide_into_subregions_preview
            if preview
            else divide_into_subregions
        )
        return divide_into_subregions_func(region_ids, next_level)


@app.route('/chop1')
@check_write_access
@validate_args_types(id=int)
def chop_largest_or_farthest():
    region_id = int(request.args.get('id'))
    borders_table = config.BORDERS_TABLE
    with g.conn.cursor() as cursor:
        cursor.execute(f"""SELECT ST_NumGeometries(geom)
                           FROM {borders_table}
                           WHERE id = {region_id}""")
        res = cursor.fetchone()
        if not res or res[0] < 2:
            return jsonify(status='border should have more than one outer ring')
        free_id1 = get_free_id()
        free_id2 = free_id1 - 1
        cursor.execute(f"""
            INSERT INTO {borders_table} (id, parent_id, name, disabled,
                                         modified, geom)
                SELECT id, region_id, name, disabled, modified, geom FROM
                (
                    (WITH w AS (SELECT name, disabled, (ST_Dump(geom)).geom AS g
                                FROM {borders_table} WHERE id = {region_id})
                    (SELECT {free_id1} id, {region_id} region_id,
                            name||'_main' as name, disabled,
                            now() as modified, g as geom, ST_Area(g) as a
                     FROM w ORDER BY a DESC LIMIT 1)
                    UNION ALL
                    SELECT {free_id2} id, {region_id} region_id,
                           name||'_small' as name, disabled,
                           now() as modified, ST_Collect(g) AS geom,
                           ST_Area(ST_Collect(g)) as a
                    FROM (SELECT name, disabled, g, ST_Area(g) AS a
                          FROM w ORDER BY a DESC OFFSET 1) ww
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
@check_write_access
@validate_args_types(id=int)
def draw_hull():
    border_id = int(request.args.get('id'))
    borders_table = config.BORDERS_TABLE
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT ST_NumGeometries(geom) FROM {borders_table} WHERE id = %s
            """, (border_id,))
        res = cursor.fetchone()
        if not res or res[0] < 2:
            return jsonify(status='border should have more than one outer ring')
        cursor.execute(f"""
            UPDATE {borders_table} SET geom = ST_ConvexHull(geom)
            WHERE id = %s""", (border_id,)
        )
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/backup')
@check_write_access
def backup_do():
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT to_char(now(), 'IYYY-MM-DD HH24:MI'), max(backup)
            FROM {config.BACKUP}
            """)
        (timestamp, tsmax) = cursor.fetchone()
        if timestamp == tsmax:
            return jsonify(status="please try again later")
        backup_table = config.BACKUP
        borders_table = config.BORDERS_TABLE
        cursor.execute(f"""
            INSERT INTO {backup_table}
                 (backup, id, name, parent_id, geom, disabled, count_k,
                    modified, cmnt, mwm_size_est)
              SELECT %s, id, name, parent_id, geom, disabled, count_k,
                    modified, cmnt, mwm_size_est
              FROM {borders_table}
            """, (timestamp,)
        )
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/restore')
@check_write_access
def backup_restore():
    ts = request.args.get('timestamp')
    borders_table = config.BORDERS_TABLE
    backup_table = config.BACKUP
    with g.conn.cursor() as cursor:
        cursor.execute(f"SELECT count(1) FROM {backup_table} WHERE backup = %s",
                       (ts,))
        (count,) = cursor.fetchone()
        if count <= 0:
            return jsonify(status="no such timestamp")
        cursor.execute(f"DELETE FROM {borders_table}")
        cursor.execute(f"""
            INSERT INTO {borders_table}
                (id, name, parent_id, geom, disabled, count_k,
                 modified, cmnt, mwm_size_est)
              SELECT id, name, parent_id, geom, disabled, count_k,
                     modified, cmnt, mwm_size_est
              FROM {backup_table}
              WHERE backup = %s
            """, (ts,)
        )
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/backlist')
def backup_list():
    with g.conn.cursor() as cursor:
        cursor.execute(f"""SELECT backup, count(1)
                           FROM {config.BACKUP}
                           GROUP BY backup
                           ORDER BY backup DESC""")
        result = []
        for backup_name, borders_count in cursor:
            result.append({
                'timestamp': backup_name,
                'text': backup_name,
                'count': borders_count
            })
        # todo: count number of different objects for the last one
    return jsonify(backups=result)


@app.route('/backdelete')
@check_write_access
def backup_delete():
    ts = request.args.get('timestamp')
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT count(1) FROM {config.BACKUP} WHERE backup = %s
            """, (ts,))
        (count,) = cursor.fetchone()
        if count <= 0:
            return jsonify(status='no such timestamp')
        cursor.execute(f"DELETE FROM {config.BACKUP} WHERE backup = %s", (ts,))
    g.conn.commit()
    return jsonify(status='ok')


@app.route('/josm')
@validate_args_types(xmin=float, xmax=float, ymin=float, ymax=float)
def make_osm():
    xmin = request.args.get('xmin')
    xmax = request.args.get('xmax')
    ymin = request.args.get('ymin')
    ymax = request.args.get('ymax')
    borders_table = request.args.get('table')
    borders_table = config.OTHER_TABLES.get(borders_table, config.BORDERS_TABLE)
    borders = fetch_borders(
        table=borders_table,
        where_clause=geom_intersects_bbox_sql(xmin, ymin, xmax, ymax)
    )
    xml = borders_to_xml(borders)
    return Response(xml, mimetype='application/x-osm+xml')


@app.route('/josmbord')
@validate_args_types(id=int)
def josm_borders_along():
    region_id = int(request.args.get('id'))
    line = request.args.get('line')
    # select all outer osm borders inside a buffer of the given line
    borders_table = config.BORDERS_TABLE
    osm_table = config.OSM_TABLE
    with g.conn.cursor() as cursor:
        cursor.execute(f"""
            WITH linestr AS (
                SELECT ST_Intersection(
                        geom,
                        ST_Buffer(ST_GeomFromText(%s, 4326), 0.2)
                    ) as line
                FROM {borders_table}
                WHERE id = %s
            ), osmborders AS (
                SELECT (ST_Dump(way)).geom as g
                FROM {osm_table}, linestr
                WHERE ST_Intersects(line, way)
            )
            SELECT ST_AsGeoJSON((
                    ST_Dump(
                        ST_LineMerge(
                            ST_Intersection(
                                ST_Collect(ST_ExteriorRing(g)),
                                line
                            )
                        )
                    )
                ).geom)
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
@check_write_access
def import_osm():
    # Though this variable is not used it's necessary to consume request.data
    # so that nginx doesn't produce error like "#[error] 13#13: *65 readv()
    # failed (104: Connection reset by peer) while reading upstream"
    data = request.data

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


@app.route('/potential_parents')
@validate_args_types(id=int)
def potential_parents():
    region_id = int(request.args.get('id'))
    parents = find_potential_parents(region_id)
    return jsonify(status='ok', parents=parents)


@app.route('/poly')
@validate_args_types(xmin=(None, float), xmax=(None, float),
                     ymin=(None, float), ymax=(None, float))
def export_poly():
    borders_table = request.args.get('table')
    borders_table = config.OTHER_TABLES.get(borders_table, config.BORDERS_TABLE)

    fetch_borders_args = {'table': borders_table,  'only_leaves': False}

    if 'xmin' in request.args:
        # If one coordinate is given then others are also expected.
        # If at least one coordinate is absent, SQL expressions
        # like ST_Point(NULL, 55.6) would provide NULL, and the
        # whole where_clause would be NULL, and the result set would be empty.
        xmin = request.args.get('xmin') or 'NULL'
        xmax = request.args.get('xmax') or 'NULL'
        ymin = request.args.get('ymin') or 'NULL'
        ymax = request.args.get('ymax') or 'NULL'
        fetch_borders_args['where_clause'] = geom_intersects_bbox_sql(xmin, ymin,
                                                                      xmax, ymax)
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
    borders_table = request.args.get('table')
    borders_table = config.OTHER_TABLES.get(borders_table, config.BORDERS_TABLE)
    with g.conn.cursor() as cursor:
        if group == 'total':
            cursor.execute(f"SELECT count(1) FROM {borders_table}")
            return jsonify(total=cursor.fetchone()[0])
        elif group == 'sizes':
            cursor.execute(f"""
                SELECT
                    name,
                    count_k,
                    ST_NPoints(geom),
                    ST_AsGeoJSON(ST_Centroid(geom)),
                    (CASE
                        WHEN ST_Area(geography(geom)) = 'NaN'::DOUBLE PRECISION
                            THEN 0
                        ELSE ST_Area(geography(geom))/1E6
                    END) AS area,
                    disabled,
                    (CASE
                        WHEN coalesce(cmnt, '') = '' THEN false
                        ELSE true
                    END) AS cmnt
                FROM {borders_table}"""
            )
            result = []
            for res in cursor:
                coord = json.loads(res[3])['coordinates']
                result.append({'name': res[0], 'lat': coord[1], 'lon': coord[0],
                               'size': res[1], 'nodes': res[2], 'area': res[4],
                               'disabled': res[5], 'commented': res[6]})
            return jsonify(regions=result)
        elif group == 'topo':
            cursor.execute(f"""
                SELECT
                    name,
                    count(1),
                    min(
                        CASE
                            WHEN ST_Area(geography(g)) = 'NaN'::DOUBLE PRECISION
                                THEN 0
                            ELSE ST_Area(geography(g))
                        END
                    ) / 1E6,
                    sum(ST_NumInteriorRings(g)),
                    ST_AsGeoJSON(ST_Centroid(ST_Collect(g)))
                FROM (SELECT name, (ST_Dump(geom)).geom AS g FROM {borders_table}) a
                GROUP BY name"""
            )
            result = []
            for (name, outer, min_area, inner, coords) in cursor:
                coord = json.loads(coords)['coordinates']
                result.append({'name': name, 'outer': outer,
                               'min_area': min_area, 'inner': inner,
                               'lon': coord[0], 'lat': coord[1]})
            return jsonify(regions=result)
    return jsonify(status='wrong group id')


@app.route('/border')
@validate_args_types(id=int)
def border():
    region_id = int(request.args.get('id'))
    borders_table = config.BORDERS_TABLE
    simplify_level = request.args.get('simplify')
    simplify = simplify_level_to_postgis_value(simplify_level)
    borders = fetch_borders(
        table=borders_table,
        simplify=simplify,
        only_leaves=False,
        where_clause=f'id = {region_id}'
    )
    if not borders:
        return jsonify(status=f'No border with id={region_id} found')
    return jsonify(status='ok', geojson=borders[0])


@app.route('/auto_divide_country')
@validate_args_types(id=int)
def auto_divide_country_endpoint():
    country_id = int(request.args.get('id'))
    errors, warnings = auto_divide_country(g.conn, country_id)
    if errors:
        return jsonify(status='<br/>'.join(errors[:3]))
    return jsonify(status='ok', warnings=warnings[:10])

@app.route('/start_over')
def start_over():
    try:
        create_countries_initial_structure(g.conn)
    except CountryStructureException as e:
        return jsonify(status=str(e))

    autosplit_table = config.AUTOSPLIT_TABLE
    with g.conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM {autosplit_table}")
    g.conn.commit()
    return jsonify(status='ok')


if __name__ == '__main__':
    app.run(threaded=True)
