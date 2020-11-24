import math
from queue import Queue

import config
from mwm_size_predictor import MwmSizePredictor


table = config.TABLE
osm_table = config.OSM_TABLE
osm_places_table = config.OSM_PLACES_TABLE


def get_subregions_info(conn, region_id, region_table,
                        next_level, need_cities=False):
    """
    :param conn: psycopg2 connection
    :param region_id:
    :param region_table: maybe TABLE or OSM_TABLE from config.py
    :param next_level: admin level of subregions to find
    :return: dict {subregion_id => subregion data} including area and population info
    """
    subregions = _get_subregions_basic_info(conn, region_id, region_table,
                                            next_level, need_cities)
    _add_population_data(conn, subregions, need_cities)
    _add_mwm_size_estimation(subregions)
    keys = ('name', 'mwm_size_est')
    if need_cities:
        keys = keys + ('cities',)
    return {subregion_id: {k: subregion_data[k] for k in keys}
            for subregion_id, subregion_data in subregions.items()
    }


def _get_subregions_basic_info(conn, region_id, region_table,
                               next_level, need_cities):
    cursor = conn.cursor()
    region_id_column, region_geom_column = (
        ('id', 'geom') if region_table == table else
        ('osm_id', 'way')
    )
    cursor.execute(f"""
        SELECT subreg.osm_id, subreg.name, ST_Area(geography(subreg.way))/1.0E+6 area
        FROM {region_table} reg, {osm_table} subreg
        WHERE reg.{region_id_column} = %s AND subreg.admin_level = %s AND
              ST_Contains(reg.{region_geom_column}, subreg.way)
        """, (region_id, next_level)
    )
    subregions = {}
    for rec in cursor:
        subregion_data = {
            'osm_id': rec[0],
            'name': rec[1],
            'area': rec[2],
            'urban_pop': 0,
            'city_cnt': 0,
            'hamlet_cnt': 0
        }
        if need_cities:
            subregion_data['cities'] = []
        subregions[rec[0]] = subregion_data
    return subregions


def _add_population_data(conn, subregions, need_cities):
    if not subregions:
        return
    cursor = conn.cursor()
    subregion_ids = ','.join(str(x) for x in subregions.keys())
    cursor.execute(f"""
        SELECT b.osm_id, p.name, coalesce(p.population, 0), p.place
        FROM {osm_table} b, {osm_places_table} p
        WHERE b.osm_id IN ({subregion_ids})
            AND ST_Contains(b.way, p.center)
        """
    )
    for subregion_id, place_name, place_population, place_type in cursor:
        subregion_data = subregions[subregion_id]
        if place_type in ('city', 'town'):
            subregion_data['city_cnt'] += 1
            subregion_data['urban_pop'] += place_population
            if need_cities:
                subregion_data['cities'].append({
                    'name': place_name,
                    'population': place_population
                })
        else:
            subregion_data['hamlet_cnt'] += 1


def _add_mwm_size_estimation(subregions):
    subregions_sorted = [
        (
            s_id,
            [subregions[s_id][f] for f in
                ('urban_pop', 'area', 'city_cnt', 'hamlet_cnt')]
        )
        for s_id in sorted(subregions.keys())
    ]

    feature_array = [x[1] for x in subregions_sorted]
    predictions = MwmSizePredictor.predict(feature_array)

    for subregion_id, mwm_size_prediction in zip(
        (x[0] for x in subregions_sorted),
        predictions
    ):
        subregions[subregion_id]['mwm_size_est'] = mwm_size_prediction


def update_border_mwm_size_estimation(conn, border_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT name, ST_Area(geography(geom))/1.0E+6 area
        FROM {table}
        WHERE id = %s""", (border_id, ))
    name, area = cursor.fetchone()
    if math.isnan(area):
        raise Exception(f"Area is NaN for border '{name}' ({border_id})")
    border_data = {
        'area': area,
        'urban_pop': 0,
        'city_cnt': 0,
        'hamlet_cnt': 0
    }
    cursor.execute(f"""
        SELECT coalesce(p.population, 0), p.place
        FROM {table} b, {config.OSM_PLACES_TABLE} p
        WHERE b.id = %s
            AND ST_Contains(b.geom, p.center)
        """, (border_id, ))
    for place_population, place_type in cursor:
        if place_type in ('city', 'town'):
            border_data['city_cnt'] += 1
            border_data['urban_pop'] += place_population
        else:
            border_data['hamlet_cnt'] += 1

    feature_array = [
        border_data[f] for f in
        ('urban_pop', 'area', 'city_cnt', 'hamlet_cnt')
    ]
    mwm_size_est = MwmSizePredictor.predict(feature_array)
    cursor.execute(f"UPDATE {table} SET mwm_size_est = %s WHERE id = %s",
                   (mwm_size_est, border_id))
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
        FROM {table}
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
            FROM {table} WHERE id = %s
            """, (region_id,)
        )
        rec = cursor.fetchone()
        if not rec:
           raise Exception(f"No record in '{table}' table with id = {region_id}")
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
        SELECT parent_id FROM {table} WHERE id = %s
        """, (region_id,))
    rec = cursor.fetchone()
    parent_id = int(rec[0]) if rec and rec[0] is not None else None
    return parent_id


def get_child_region_ids(conn, region_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT id FROM {table} WHERE parent_id = %s
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
            children = find_osm_child_regions(item['id'])
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
            FROM {table} c, {table} p, {osm_table} oc
            WHERE p.id = c.parent_id AND c.id = oc.osm_id
                AND p.id = %s
            """, (region_id,)
        )
        for rec in cursor:
            children.append({'id': int(rec[0]), 'admin_level': int(rec[1])})
    return children
