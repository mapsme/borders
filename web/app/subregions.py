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


def get_subregions_info(conn, region_id, region_table,
                        next_level, need_cities=False):
    """
    :param conn: psycopg2 connection
    :param region_id:
    :param region_table: maybe TABLE or OSM_TABLE from config.py
    :param next_level: admin level of subregions to find
    :return: dict {subregion_id => subregion data} including area and population info
    """
    subregion_ids = _get_geometrical_subregion_ids(conn, region_id,
                                                   region_table, next_level)
    subregions = _get_regions_basic_info(conn, subregion_ids)
    _add_mwm_size_estimation(conn, subregions, need_cities)
    keys = ('name', 'mwm_size_est')
    if need_cities:
        keys = keys + ('cities',)
    return {subregion_id: {k: subregion_data[k] for k in keys
                                if k in subregion_data}
            for subregion_id, subregion_data in subregions.items()
    }


def _get_geometrical_subregion_ids(conn, region_id, region_table, next_level):
    region_id_column, region_geom_column = (
        ('id', 'geom') if region_table == borders_table else
        ('osm_id', 'way')
    )
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT subreg.osm_id
            FROM {region_table} reg, {osm_table} subreg
            WHERE reg.{region_id_column} = %s AND subreg.admin_level = %s AND
                  ST_Contains(reg.{region_geom_column}, subreg.way)
            """, (region_id, next_level)
        )
        return list(rec[0] for rec in cursor)


def _get_regions_basic_info(conn, region_ids):
    """Gets name, land_area for regions in OSM borders table"""
    if not region_ids:
        return {}

    region_ids_str = ','.join(str(x) for x in region_ids)
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT reg.osm_id, reg.name,
              ST_Area(
                geography(
                  ST_Intersection(
                    reg.way,
                    (
                      SELECT ST_Union(c.geom)
                      FROM {land_polygons_table} c
                      WHERE c.geom && reg.way
                    )
                  )
                )
              ) / 1.0E+6 land_area
            FROM {osm_table} reg
            WHERE osm_id in ({region_ids_str})
            """
        )
        regions = {}
        for osm_id, name, land_area in cursor:
            region_data = {
                'osm_id': osm_id,
                'name': name,
                'land_area': land_area,
            }
            regions[osm_id] = region_data
    return regions


def _add_population_data(conn, regions, need_cities):
    """Adds population data only for regions that are suitable
    for mwm size estimation.
    """
    region_ids = [
        s_id for s_id, s_data in regions.items()
        if s_data['land_area'] <= MWM_SIZE_PREDICTION_MODEL_LIMITATIONS['land_area']
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

    region_ids_str = ','.join(str(x) for x in region_ids)
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT b.osm_id, p.name, coalesce(p.population, 0), p.place
            FROM {osm_table} b, {osm_places_table} p
            WHERE b.osm_id IN ({region_ids_str})
                AND ST_Contains(b.way, p.center)
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


def _add_coastline_length(conn, regions):
    if not regions:
        return

    for r_data in regions.values():
        r_data['coastline_length'] = 0.0

    region_ids_str = ','.join(str(x) for x in regions.keys())

    with conn.cursor() as cursor:
        cursor.execute(f"""
            WITH buffered_borders AS (
              -- 0.001 degree ~ 100 m - ocean buffer stripe to overcome difference
              -- in coastline and borders
              SELECT id, ST_Buffer(geom, 0.001) geom
              FROM {borders_table}
              WHERE id IN ({region_ids_str})
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


def _add_mwm_size_estimation(conn, regions, need_cities):
    for region_data in regions.values():
        region_data['mwm_size_est'] = None

    _add_population_data(conn, regions, need_cities)
    _add_coastline_length(conn, regions)

    #from pprint import pprint as pp
    #pp(regions)
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
    regions = _get_regions_basic_info(conn, [border_id])

    if math.isnan(regions[border_id]['land_area']):
        e = Exception(f"Area is NaN for border '{name}' ({border_id})")
        raise e

    _add_mwm_size_estimation(conn, regions, need_cities=False)
    mwm_size_est = regions[border_id].get('mwm_size_est')
    # mwm_size_est may be None. Python's None is converted to NULL
    # during %s substitution in execute().
    with conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {borders_table}
            SET mwm_size_est = %s
            WHERE id = %s
            """, (mwm_size_est, border_id,))
    conn.commit()


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
    return predecessors[-1]


def get_predecessors(conn, region_id):
    """Returns the list of (id, name)-tuples of all predecessors,
    starting from the very region_id.
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
           raise Exception(
               f"No record in '{borders_table}' table with id = {region_id}"
           )
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
