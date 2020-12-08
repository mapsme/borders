from config import (
    BORDERS_TABLE as borders_table,
    OSM_TABLE as osm_table
)
from countries_division import country_initial_levels
from subregions import (
    get_subregions_info,
    update_border_mwm_size_estimation,
)


class CountryStructureException(Exception):
    pass


def _clear_borders(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM {borders_table}")
    conn.commit()


def _find_subregions(conn, osm_ids, next_level, regions):
    """Return subregions of level 'next_level' for regions with osm_ids."""
    subregion_ids = []
    for osm_id in osm_ids:
        more_subregions = get_subregions_info(conn, osm_id, borders_table,
                                              next_level, need_cities=False)
        for subregion_id, subregion_data in more_subregions.items():
            region_data = regions.setdefault(subregion_id, {})
            region_data['name'] = subregion_data['name']
            region_data['mwm_size_est'] = subregion_data['mwm_size_est']
            region_data['parent_id'] = osm_id
            subregion_ids.append(subregion_id)
    return subregion_ids


def _create_regions(conn, osm_ids, regions):
    if not osm_ids:
        return
    osm_ids = list(osm_ids)  # to ensure order
    sql_values = ','.join(
            f'({osm_id},'
            '%s,'
            f"{regions[osm_id].get('parent_id', 'NULL')},"
            f"{regions[osm_id].get('mwm_size_est', 'NULL')},"
            f'(SELECT way FROM {osm_table} WHERE osm_id={osm_id}),'
            'now())'
            for osm_id in osm_ids
    )
    with conn.cursor() as cursor:
        cursor.execute(f"""
            INSERT INTO {borders_table} (id, name, parent_id, mwm_size_est,
                                 geom, modified)
            VALUES {sql_values}
            """, tuple(regions[osm_id]['name'] for osm_id in osm_ids)
        )


def _make_country_structure(conn, country_osm_id):
    regions = {}  # osm_id: { 'name': name,
                  #           'mwm_size_est': size,
                  #           'parent_id': parent_id }

    country_name = get_osm_border_name_by_osm_id(conn, country_osm_id)
    country_data = regions.setdefault(country_osm_id, {})
    country_data['name'] = country_name
    # TODO: country_data['mwm_size_est'] = ...

    _create_regions(conn, [country_osm_id], regions)

    if country_initial_levels.get(country_name):
        admin_levels = country_initial_levels[country_name]
        prev_admin_levels = [2] + admin_levels[:-1]
        prev_region_ids = [country_osm_id]

        for admin_level, prev_level in zip(admin_levels, prev_admin_levels):
            if not prev_region_ids:
                raise CountryStructureException(
                        f"Empty prev_region_ids at {country_name}, "
                        f"AL={admin_level}, prev-AL={prev_level}"
                )
            subregion_ids = _find_subregions(conn, prev_region_ids,
                                             admin_level, regions)
            _create_regions(conn, subregion_ids, regions)
            prev_region_ids = subregion_ids
    warning = None
    if len(regions) == 1:
        try:
            update_border_mwm_size_estimation(conn, country_osm_id)
        except Exception as e:
            warning = str(e)
    return warning


def create_countries_initial_structure(conn):
    _clear_borders(conn)
    with conn.cursor() as cursor:
        # TODO: process overlapping countries, like Ukraine and Russia with common Crimea
        cursor.execute(f"""
            SELECT osm_id, name
            FROM {osm_table}
            WHERE admin_level = 2 and name != 'Ukraine'
            """
        )
        warnings = []
        for rec in cursor:
            warning = _make_country_structure(conn, rec[0])
            if warning:
                warnings.append(warning)
    conn.commit()
    return warnings


def get_osm_border_name_by_osm_id(conn, osm_id):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT name FROM {osm_table}
            WHERE osm_id = %s
            """, (osm_id,))
        rec = cursor.fetchone()
        if not rec:
            raise CountryStructureException(
                f'Not found region with osm_id="{osm_id}"'
            )
        return rec[0]


def _get_country_osm_id_by_name(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT osm_id FROM {osm_table}
            WHERE admin_level = 2 AND name = %s
            """, (name,))
        row_count = cursor.rowcount
        if row_count > 1:
            raise CountryStructureException(f'More than one country "{name}"')
        rec = cursor.fetchone()
        if not rec:
            raise CountryStructureException(f'Not found country "{name}"')
        return int(rec[0])
