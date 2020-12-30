from borders_api_utils import (
    copy_region_from_osm,
    divide_region_into_subregions,
    get_osm_border_name_by_osm_id,
)
from config import (
    BORDERS_TABLE as borders_table,
    OSM_TABLE as osm_table
)
from countries_division import country_initial_levels


class CountryStructureException(Exception):
    pass


def _clear_borders(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM {borders_table}")


def _make_country_structure(conn, country_osm_id):
    country_name = get_osm_border_name_by_osm_id(conn, country_osm_id)

    copy_region_from_osm(conn, country_osm_id, parent_id=None)

    if country_initial_levels.get(country_name):
        admin_levels = country_initial_levels[country_name]
        prev_admin_levels = [2] + admin_levels[:-1]
        prev_level_region_ids = [country_osm_id]

        for admin_level, prev_level in zip(admin_levels, prev_admin_levels):
            current_level_region_ids = []
            for region_id in prev_level_region_ids:
                subregion_ids = divide_region_into_subregions(
                                           conn, region_id, admin_level)
                current_level_region_ids.extend(subregion_ids)
            prev_level_region_ids = current_level_region_ids


def create_countries_initial_structure(conn):
    _clear_borders(conn)
    with conn.cursor() as cursor:
        # TODO: process overlapping countries, like Ukraine and Russia with common Crimea
        cursor.execute(f"""
            SELECT osm_id, name
            FROM {osm_table}
            WHERE admin_level = 2
            """
        )
        for country_osm_id, *_ in cursor:
            _make_country_structure(conn, country_osm_id)
    conn.commit()
    return


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
        return rec[0]

