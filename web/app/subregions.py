import math
from queue import Queue

from config import (
    BORDERS_TABLE as borders_table,
    MWM_SIZE_PREDICTION_MODEL_LIMITATIONS,
    OSM_TABLE as osm_table,
    OSM_PLACES_TABLE as osm_places_table,
    LAND_POLYGONS_TABLE as land_polygons_table,
    COASTLINE_TABLE as coastline_table,
)
from mwm_size_predictor import MwmSizePredictor
from utils import (
    is_coastline_table_available,
    is_land_table_available,
)


def get_regions_info(conn, region_ids, regions_table, need_cities=False):
    """Get regions info including mwm_size_est in the form of
    dict {region_id => region data}
    """
    regions_info = get_regions_basic_info(conn, region_ids, regions_table)
    _add_mwm_size_estimation(conn, regions_info, regions_table, need_cities)
    keys = ('name', 'mwm_size_est')
    if need_cities:
        keys = keys + ('cities',)
    return {region_id: {k: region_data[k] for k in keys
                                if k in region_data}
            for region_id, region_data in regions_info.items()
    }
    

def get_subregions_info(conn, region_id, region_table,
                        next_level, need_cities=False):
    """
    :param conn: psycopg2 connection
    :param region_id:
    :param region_table: maybe TABLE or OSM_TABLE from config.py
    :param next_level: admin level of subregions to find
    :return: dict {subregion_id => subregion data} including area and population info
    """
    subregions = get_geometrical_subregions(conn, region_id,
                                            region_table, next_level)
    subregion_ids = list(subregions.keys())
    return get_regions_info(conn, subregion_ids, osm_table, need_cities)


def get_geometrical_subregions(conn, region_id, region_table, next_level):
    region_id_column, region_geom_column = (
        ('id', 'geom') if region_table == borders_table else
        ('osm_id', 'way')
    )
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT subreg.osm_id, subreg.name
            FROM {region_table} reg, {osm_table} subreg
            WHERE reg.{region_id_column} = %s AND subreg.admin_level = %s AND
                  ST_Contains(reg.{region_geom_column}, subreg.way)
            """, (region_id, next_level)
        )
        return {s_id: name for s_id, name in cursor}


def get_regions_basic_info(conn, region_ids, regions_table, need_land_area=True):
    """Gets name, land_area for regions in OSM borders table"""
    if not region_ids:
        return {}

    region_id_column, region_geom_column = (
        ('id', 'geom') if regions_table == borders_table else
        ('osm_id', 'way')
    )
    region_ids_str = ','.join(str(x) for x in region_ids)
    land_area_expr = (
        'NULL' if not need_land_area or not is_land_table_available(conn)
        else f"""
              ST_Area(
                geography(
                  ST_Intersection(
                    reg.{region_geom_column},
                    (
                      SELECT ST_Union(c.geom)
                      FROM {land_polygons_table} c
                      WHERE c.geom && reg.{region_geom_column}
                    )
                  )
                )
              ) / 1.0E+6
        """
    )
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT reg.{region_id_column}, reg.name,
              ST_Area(reg.{region_geom_column}) / 1.0E+6 area,
              {land_area_expr} land_area
            FROM {regions_table} reg
            WHERE {region_id_column} in ({region_ids_str})
            """
        )
        regions = {}
        for r_id, name, area, land_area in cursor:
            region_data = {
                'id': r_id,
                'name': name,
                'area': area,
            }
            if need_land_area:
                region_data['land_area'] = land_area
            regions[r_id] = region_data
    return regions


def _add_population_data(conn, regions, regions_table, need_cities):
    """Adds population data only for regions that are suitable
    for mwm size estimation.
    """
    print(regions)
    region_ids = [
        s_id for s_id, s_data in regions.items()
        if s_data.get('land_area') is not None and
            s_data['land_area'] <= MWM_SIZE_PREDICTION_MODEL_LIMITATIONS['land_area']
    ]
    if not region_ids:
        return

    for region_id, data in regions.items():
        data.update({
            'city_pop': 0,
            'city_cnt': 0,
            'hamlet_cnt': 0
        })
        if need_cities:
            data['cities'] = []

    region_id_column, region_geom_column = (
        ('id', 'geom') if regions_table == borders_table else
        ('osm_id', 'way')
    )

    region_ids_str = ','.join(str(x) for x in region_ids)
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT b.{region_id_column}, p.name, coalesce(p.population, 0), p.place
            FROM {regions_table} b, {osm_places_table} p
            WHERE b.{region_id_column} IN ({region_ids_str})
                AND ST_Contains(b.{region_geom_column}, p.center)
            """
        )
        for region_id, place_name, place_population, place_type in cursor:
            region_data = regions[region_id]
            if place_type in ('city', 'town'):
                region_data['city_cnt'] += 1
                region_data['city_pop'] += place_population
                if need_cities:
                    region_data['cities'].append({
                        'name': place_name,
                        'population': place_population
                    })
            else:
                region_data['hamlet_cnt'] += 1


def _add_coastline_length(conn, regions, regions_table):
    if not regions or not is_coastline_table_available(conn):
        return

    for r_data in regions.values():
        r_data['coastline_length'] = 0.0

    region_ids_str = ','.join(str(x) for x in regions.keys())

    region_id_column, region_geom_column = (
        ('id', 'geom') if regions_table == borders_table else
        ('osm_id', 'way')
    )

    with conn.cursor() as cursor:
        cursor.execute(f"""
            WITH buffered_borders AS (
              -- 0.001 degree ~ 100 m - ocean buffer stripe to overcome difference
              -- in coastline and borders
              SELECT {region_id_column} id,
                     ST_Buffer({region_geom_column}, 0.001) geom
              FROM {regions_table}
              WHERE {region_id_column} IN ({region_ids_str})
            )
            SELECT bb.id,
                   SUM(
                     ST_Length(
                       geography(
                         ST_Intersection(
                           bb.geom,
                           c.geom
                         )
                       )
                     )
                   ) / 1e3
            FROM {coastline_table} c, buffered_borders as bb
            WHERE c.geom && bb.geom
            GROUP BY bb.id
            """)
        for b_id, coastline_length in cursor:
            regions[b_id]['coastline_length'] = coastline_length


def _add_mwm_size_estimation(conn, regions, regions_table, need_cities):
    for region_data in regions.values():
        region_data['mwm_size_est'] = None

    _add_population_data(conn, regions, regions_table, need_cities)
    _add_coastline_length(conn, regions, regions_table)

    regions_to_predict = [
        (
            s_id,
            [regions[s_id][f] for f in MwmSizePredictor.factors]
        )
        for s_id in sorted(regions.keys())
            if all(regions[s_id].get(f) is not None and
                   regions[s_id][f] <=
                        MWM_SIZE_PREDICTION_MODEL_LIMITATIONS[f]
                   for f in MwmSizePredictor.factors
                   if f in MWM_SIZE_PREDICTION_MODEL_LIMITATIONS.keys())
    ]

    if not regions_to_predict:
        return

    feature_array = [x[1] for x in regions_to_predict]
    predictions = MwmSizePredictor.predict(feature_array)

    for region_id, mwm_size_prediction in zip(
        (x[0] for x in regions_to_predict),
        predictions
    ):
        regions[region_id]['mwm_size_est'] = mwm_size_prediction


def update_border_mwm_size_estimation(conn, border_id):
    regions = get_regions_basic_info(conn, [border_id], borders_table)

    if math.isnan(regions[border_id]['land_area']):
        e = Exception(f"Area is NaN for border '{regions[border_id]['name']}' ({border_id})")
        raise e

    _add_mwm_size_estimation(conn, regions, borders_table, need_cities=False)
    mwm_size_est = regions[border_id].get('mwm_size_est')
    # mwm_size_est may be None. Python's None is converted to NULL
    # during %s substitution in cursor.execute().
    with conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {borders_table}
            SET mwm_size_est = %s
            WHERE id = %s
            """, (mwm_size_est, border_id,))
    conn.commit()
    return mwm_size_est


def is_administrative_region(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT count(1) FROM {osm_table} WHERE osm_id = %s
        """, (region_id,)
    )
    count = cursor.fetchone()[0]
    return (count > 0)


def is_leaf(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT count(1)
        FROM {borders_table}
        WHERE parent_id = %s
        """, (region_id,)
    )
    count = cursor.fetchone()[0]
    return (count == 0)


def get_region_country(conn, region_id):
    """Returns the uppermost predecessor of the region in the hierarchy,
    possibly itself.
    """
    predecessors = get_predecessors(conn, region_id)
    return predecessors[-1] if predecessors is not None else (None, None)


def get_predecessors(conn, region_id):
    """Returns the list of (id, name)-tuples of all predecessors,
    starting from the very region_id, and None if there is no
    requested region or one of its predecessors in the DB which
    may occur due to other queries to the DB.
    """
    predecessors = []
    cursor = conn.cursor()
    while True:
        cursor.execute(f"""
            SELECT id, name, parent_id
            FROM {borders_table} WHERE id = %s
            """, (region_id,)
        )
        rec = cursor.fetchone()
        if not rec:
            return None
        predecessors.append(rec[0:2])
        parent_id = rec[2]
        if not parent_id:
            break
        region_id = parent_id
    return predecessors


def get_region_full_name(conn, region_id):
    predecessors = get_predecessors(conn, region_id)
    return '_'.join(pr[1] for pr in reversed(predecessors))


def get_parent_region_id(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT parent_id FROM {borders_table} WHERE id = %s
        """, (region_id,))
    rec = cursor.fetchone()
    parent_id = int(rec[0]) if rec and rec[0] is not None else None
    return parent_id


def get_child_region_ids(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT id FROM {borders_table} WHERE parent_id = %s
        """, (region_id,))
    child_ids = []
    for rec in cursor:
        child_ids.append(int(rec[0]))
    return child_ids


def get_similar_regions(conn, region_id, only_leaves=False):
    """Returns ids of regions of the same admin_level in the same country.
    Prerequisite: is_administrative_region(region_id) is True.
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT admin_level FROM {osm_table}
        WHERE osm_id = %s""", (region_id,)
    )
    admin_level = int(cursor.fetchone()[0])
    country_id, country_name = get_region_country(conn, region_id)
    q = Queue()
    q.put({'id': country_id, 'admin_level': 2})
    similar_region_ids = []
    while not q.empty():
        item = q.get()
        if item['admin_level'] == admin_level:
            similar_region_ids.append(item['id'])
        elif item['admin_level'] < admin_level:
            children = find_osm_child_regions(conn, item['id'])
            for ch in children:
                q.put(ch)
    if only_leaves:
        similar_region_ids = [r_id for r_id in similar_region_ids
                                  if is_leaf(conn, r_id)]
    return similar_region_ids


def find_osm_child_regions(conn, region_id):
    children = []
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT c.id, oc.admin_level
            FROM {borders_table} c, {borders_table} p, {osm_table} oc
            WHERE p.id = c.parent_id AND c.id = oc.osm_id
                AND p.id = %s
            """, (region_id,)
        )
        for osm_id, admin_level in cursor:
            children.append({'id': osm_id, 'admin_level': admin_level})
    return children
