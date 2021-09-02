import itertools

import config

from auto_split import(
    combine_into_clusters,
)
from borders_api_utils import (
    copy_region_from_osm,
    divide_region_into_subregions,
    get_free_id,
    get_osm_border_name_by_osm_id,
)
from config import (
    BORDERS_TABLE as borders_table,
    MWM_SIZE_THRESHOLD,
    OSM_TABLE as osm_table
)
from countries_division import country_levels
from simple_splitting import simple_split
from subregions import (
    get_regions_basic_info,
    get_regions_info,
    get_geometrical_subregions,
    update_border_mwm_size_estimation,
)
from utils import is_land_table_available


class CountryStructureException(Exception):
    pass


def _clear_borders(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM {borders_table}")


def checksum_area(conn, regions, region_id):
    """Returns True if the sum of subregion areas (almost) equal
    to the region area.
    """
    region = regions[region_id]
    children = [r for r in regions.values() if r['parent_id'] == region_id]
    regions_without_area = [r for r in itertools.chain(children, [region])
                                if 'land_area' not in r]
    regions_without_area_ids = [r['id'] for r in regions_without_area]
    regions_info = get_regions_basic_info(conn, regions_without_area_ids, osm_table)
    for r_id, r_data in regions_info.items():
        regions[r_id]['land_area'] = r_data['land_area']

    children_area = sum(r['land_area'] for r in children)
    has_lost_subregions = (children_area < 0.99 * region['land_area'])
    return not has_lost_subregions


def _amend_regions_with_mwm_size(conn, regions):
    region_ids_without_size = [s_id for s_id, s_data in regions.items()
                                    if 'mwm_size_est' not in s_data]
    extra_regions = get_regions_info(conn, region_ids_without_size, osm_table)
    for s_id, s_data in extra_regions.items():
        regions[s_id]['mwm_size_est'] = s_data['mwm_size_est']


def auto_divide_country(conn, country_id):
    country_name = get_osm_border_name_by_osm_id(conn, country_id)
    metalevels = country_levels.get(country_name, None)
    if metalevels is None or not is_land_table_available(conn):
        e, w = copy_region_from_osm(conn, country_id)
        conn.commit()
        return e, w

    regions = {
            country_id: {
                'id': country_id,
                'name': country_name,
                'al': 2,
                'parent_id': None
            }
    }

    all_metalevels = metalevels[0] + metalevels[1]
    fill_regions_structure(conn, regions, country_id, all_metalevels)
    non_mergeable_metalevels = metalevels[0]

    for metalevel, lower_metalevel in list(zip(all_metalevels[:-1], all_metalevels[1:]))[::-1]:
        if lower_metalevel in non_mergeable_metalevels:
            break
        # Find regions at metalevel that composed of subregions at lower_metalevel
        region_ids_at_metalevel = [r['id'] for r in regions.values()
                                       if r['al'] in metalevel]
        for region_id in region_ids_at_metalevel:
            if checksum_area(conn, regions, region_id):
                regions[region_id]['has_lost_subregions'] = False
                children = [r for r in regions.values()
                                    if r['parent_id'] == region_id]
                mergeable_children = {ch['id']: ch for ch in children
                                          if 'clusters' not in ch}
                _amend_regions_with_mwm_size(conn, mergeable_children)
                dcu = combine_into_clusters(conn,
                        mergeable_children, config.MWM_SIZE_THRESHOLD)
                regions[region_id]['mwm_size_est'] = sum(ch['mwm_size_est']
                                                             for ch in children)
                if len(children) == len(mergeable_children):
                    # If the sum of subregions is less than mwm_size_thr
                    # then collapse clusters into one despite of geometrical connectivity
                    dcu.try_collapse_into_one()

                if len(dcu.clusters) == 1 and len(children) == len(mergeable_children):
                    regions[region_id]['merged_up_to_itself'] = True
                    for ch in children:
                        regions[ch['id']]['merged'] = True
                else:
                    real_clusters = {
                        cl_id: cl_data
                        for cl_id, cl_data in dcu.clusters.items()
                        if len(cl_data['subregion_ids']) > 1
                    }
                    regions[region_id]['clusters'] = real_clusters
                    for cluster in real_clusters.values():
                        for s_id in cluster['subregion_ids']:
                            regions[s_id]['merged'] = True
            else:
                regions[region_id]['has_lost_subregions'] = True

    warnings = []
    save_country_structure_to_db(conn, regions)
    conn.commit()
    return [], warnings


def save_country_structure_to_db(conn, regions):
    parent_ids = set(r['parent_id'] for r in regions.values() if r['parent_id'] is not None)
    leaf_ids = set(regions.keys()) - parent_ids
    for leaf_id in leaf_ids:
        regions[leaf_id]['is_leaf'] = True

    def save_clusters_to_db(conn, region_id):
        assert('clusters' in regions[region_id])
        free_id = get_free_id()
        with conn.cursor() as cursor:
            parent_name = regions[region_id]['name']
            counter = 0
            for cl_id, cl_data in regions[region_id]['clusters'].items():
                if len(cl_data['subregion_ids']) == 1:
                    subregion_id = cl_data['subregion_ids'][0]
                    subregion_name = regions[subregion_id]['name']
                    cursor.execute(f"""
                        INSERT INTO {borders_table} (id, name, parent_id, geom,
                                                     modified, count_k, mwm_size_est)
                        VALUES (
                            {subregion_id},
                            %s,
                            {region_id},
                            (
                              SELECT way FROM {osm_table}
                              WHERE osm_id = {subregion_id}
                            ),
                            now(),
                            -1,
                            {cl_data['mwm_size_est']}
                        )
                    """, (subregion_name,))
                else:
                    counter += 1
                    subregion_ids_str = ','.join(str(x) for x in cl_data['subregion_ids'])
                    cursor.execute(f"""
                        INSERT INTO {borders_table} (id, name, parent_id, geom,
                                                     modified, count_k, mwm_size_est)
                        VALUES (
                            {free_id},
                            %s,
                            {region_id},
                            (
                              SELECT ST_Union(way) FROM {osm_table}
                              WHERE osm_id IN ({subregion_ids_str})
                            ),
                            now(),
                            -1,
                            {cl_data['mwm_size_est']}
                        )
                    """, (f"{parent_name}_{counter}",))
                    free_id -= 1

    def save_region_structure_to_db(conn, region_id):
        r_data = regions[region_id]
        if r_data.get('merged') == True:
            return
        copy_region_from_osm(conn, region_id,
                             parent_id=r_data['parent_id'],
                             mwm_size_est=r_data.get('mwm_size_est'))
        if r_data.get('has_lost_subregions') or r_data.get('is_leaf'):
            region_container = {k: v for k, v in regions.items() if k == region_id}
            region_data = region_container[region_id]
            mwm_size_est = update_border_mwm_size_estimation(conn, region_id)
            region_data['mwm_size_est'] = mwm_size_est
            if (mwm_size_est is not None and
                   mwm_size_est > MWM_SIZE_THRESHOLD):
                simple_split(conn, region_data)
        else:
            children_ids = set(r['id'] for r in regions.values()
                                if r['parent_id'] == region_id)
            children_in_clusters = set(itertools.chain.from_iterable(
                    cl['subregion_ids'] for cl in r_data.get('clusters', {}).values()))
            standalone_children_ids = children_ids - children_in_clusters
            if 'clusters' in r_data:
                save_clusters_to_db(conn, region_id)
            for ch_id in standalone_children_ids:
                save_region_structure_to_db(conn, ch_id)


    country_id = [k for k, v in regions.items() if v['parent_id'] is None]
    assert len(country_id) == 1
    country_id = country_id[0]

    save_region_structure_to_db(conn, country_id)
    conn.commit()


def fill_regions_structure(conn, regions, region_id, metalevels):
    """Given regions tree-like dict, amend it by splitting region_id
    region at metalevels.
    """
    leaf_ids = [region_id]
    for metalevel in metalevels:
        for leaf_id in leaf_ids:
            fill_region_structure_at_metalevel(conn, regions, leaf_id, metalevel)
        leaf_ids = [
                r_id for r_id in
                        (set(regions.keys()) - set(r['parent_id'] for r in regions.values()))
                if regions[r_id]['al'] in metalevel
        ]


def fill_region_structure_at_metalevel(conn, regions, region_id, metalevel):
    """Divides a region with "region_id" into subregions of specified admin level(s).
    Updates the "regions" tree-like dict:
    region_id : {'id': region_id, 'al': admin_level, 'parent_id': parent_id}
    """

    def process_subregions_of(region_id):
        subregion_ids_by_level = []
        # "regions" dict is used from the closure
        for sublevel in (lev for lev in metalevel if lev > regions[region_id]['al']):
            subregions = get_geometrical_subregions(
                conn, region_id, osm_table, sublevel
            )
            subregion_ids = list(subregions.keys())
            subregion_ids_by_level.append(subregion_ids)
            for s_id in subregion_ids:
                # As a first approximation, assign all found subregions
                # of all sublevels to the region. This may change in deeper recursion calls.
                if s_id not in regions:
                    regions[s_id] = {
                        'id': s_id,
                        'name': subregions[s_id],
                        'parent_id': region_id,
                        'al': sublevel,

                    }
                else:
                    regions[s_id]['parent_id'] = region_id

        for layer in subregion_ids_by_level:
            for s_id in layer:
                process_subregions_of(s_id)

    process_subregions_of(region_id)


def create_countries_initial_structure(conn):
    _clear_borders(conn)
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT osm_id, name
            FROM {osm_table}
            WHERE admin_level = 2
            """
        )
        for country_osm_id, country_name in cursor:
            # Only create small countries - to not forget to create them manually
            if country_name not in country_levels:
                auto_divide_country(conn, country_osm_id)
