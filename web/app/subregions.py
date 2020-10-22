import config
from mwm_size_predictor import MwmSizePredictor


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
        ('id', 'geom') if region_table == config.TABLE else
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
    table = config.TABLE
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT ST_Area(geography(geom))/1.0E+6 area
        FROM {table}
        WHERE id = %s""", (border_id, ))
    rec = cursor.fetchone()
    border_data = {
        'area': rec[0],
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
