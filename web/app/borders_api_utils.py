import itertools
import json

from flask import g, jsonify

import config
from auto_split import split_region
from countries_structure import get_osm_border_name_by_osm_id
from subregions import (
    get_region_country,
    get_subregions_info,
    is_administrative_region,
)


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


def get_free_id():
    cursor = g.conn.cursor()
    table = config.TABLE
    cursor.execute(f"SELECT min(id) FROM {table} WHERE id < -1000000000")
    min_id = cursor.fetchone()[0]
    free_id = min_id - 1 if min_id else -1_000_000_001
    return free_id


def assign_region_to_lowest_parent(region_id):
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
        assign_region_to_lowest_parent(region_id)
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
        feature = {
                'type': 'Feature',
                'geometry': json.loads(rec[3]),
                'properties': props
        }
        parents.append(feature)
    return parents
