import itertools
import json
import psycopg2

from collections import defaultdict

from config import (
        AUTOSPLIT_TABLE as autosplit_table,
        OSM_TABLE as osm_table,
        MWM_SIZE_THRESHOLD,
)
from subregions import get_subregions_info


class DisjointClusterUnion:
    """Disjoint set union implementation for administrative subregions."""

    def __init__(self, region_id, subregions, mwm_size_thr=None):
        self.region_id = region_id
        self.subregions = subregions
        self.mwm_size_thr = mwm_size_thr or MWM_SIZE_THRESHOLD
        self.representatives = {sub_id: sub_id for sub_id in subregions}
        # A cluster is one or more subregions with common borders
        self.clusters = {}  # representative => cluster object

        # At the beginning, each subregion forms a cluster.
        # Then they would be enlarged by merging.
        for subregion_id, data in subregions.items():
            self.clusters[subregion_id] = {
                'representative': subregion_id,
                'subregion_ids': [subregion_id],
                'mwm_size_est': data['mwm_size_est'],
                'finished': False,  # True if the cluster cannot be merged with another
            }

    def get_smallest_cluster(self):
        """Find minimal cluster without big cities."""
        smallest_cluster_id = min(
            filter(
                lambda cluster_id:
                    not self.clusters[cluster_id]['finished'],
                self.clusters.keys()
            ),
            default=None,
            key=lambda cluster_id: self.clusters[cluster_id]['mwm_size_est']
        )
        return smallest_cluster_id

    def mark_cluster_finished(self, cluster_id):
        self.clusters[cluster_id]['finished'] = True

    def find_cluster(self, subregion_id):
        if self.representatives[subregion_id] == subregion_id:
            return subregion_id
        else:
            representative = self.find_cluster(self.representatives[subregion_id])
            self.representatives[subregion_id] = representative
            return representative

    def get_cluster_mwm_size_est(self, subregion_id):
        cluster_id = self.find_cluster(subregion_id)
        return self.clusters[cluster_id]['mwm_size_est']

    def get_cluster_count(self):
        return len(self.clusters)

    def union(self, cluster_id1, cluster_id2):
        # To make it more deterministic
        retained_cluster_id = max(cluster_id1, cluster_id2)
        dropped_cluster_id = min(cluster_id1, cluster_id2)
        r_cluster = self.clusters[retained_cluster_id]
        d_cluster = self.clusters[dropped_cluster_id]
        r_cluster['subregion_ids'].extend(d_cluster['subregion_ids'])
        r_cluster['mwm_size_est'] += d_cluster['mwm_size_est']
        del self.clusters[dropped_cluster_id]
        self.representatives[dropped_cluster_id] = retained_cluster_id
        return retained_cluster_id

    def get_cluster_subregion_ids(self, subregion_id):
        """Get all elements in a cluster by subregion_id"""
        representative = self.find_cluster(subregion_id)
        return set(self.clusters[representative]['subregion_ids'])

    def get_all_subregion_ids(self):
        subregion_ids = set(itertools.chain.from_iterable(
            cl['subregion_ids'] for cl in self.clusters.values()
        ))
        return subregion_ids


def get_best_cluster_to_join_with(small_cluster_id,
                                  dcu: DisjointClusterUnion,
                                  common_border_matrix):
    if small_cluster_id not in common_border_matrix:
        # This may be if a subregion is isolated,
        # like Bezirk Lienz inside Tyrol, Austria
        return None
    common_borders = defaultdict(lambda: 0.0)  # cluster representative => common border length
    subregion_ids = dcu.get_cluster_subregion_ids(small_cluster_id)
    for subregion_id in subregion_ids:
        for other_subregion_id, length in common_border_matrix[subregion_id].items():
            other_cluster_id = dcu.find_cluster(other_subregion_id)
            if other_cluster_id != small_cluster_id:
                common_borders[other_cluster_id] += length
    if not common_borders:
        return None
    total_common_border_length = sum(common_borders.values())
    total_adjacent_mwm_size_est = sum(dcu.get_cluster_mwm_size_est(x) for x in common_borders)
    choice_criterion = (
        (
          lambda cluster_id: (
            common_borders[cluster_id]/total_common_border_length +
            -dcu.get_cluster_mwm_size_est(cluster_id)/total_adjacent_mwm_size_est
          )
        ) if total_adjacent_mwm_size_est else
        lambda cluster_id: (
            common_borders[cluster_id]/total_common_border_length
        )
    )
    best_cluster_id = max(
        filter(
            lambda cluster_id: (
                dcu.clusters[small_cluster_id]['mwm_size_est'] +
                dcu.clusters[cluster_id]['mwm_size_est'] <= dcu.mwm_size_thr
            ),
            common_borders.keys()
        ),
        default=None,
        key=choice_criterion
    )
    return best_cluster_id


def calculate_common_border_matrix(conn, subregion_ids):
    cursor = conn.cursor()
    subregion_ids_str = ','.join(str(x) for x in subregion_ids)
    # ST_Intersection returns 0 if its parameter is a geometry other than
    # LINESTRING or MULTILINESTRING
    cursor.execute(f"""
        SELECT b1.osm_id AS osm_id1, b2.osm_id AS osm_id2,
               ST_Length(geography(ST_Intersection(b1.way, b2.way))) AS intersection
        FROM {osm_table} b1, {osm_table} b2
        WHERE b1.osm_id IN ({subregion_ids_str}) AND
              b2.osm_id IN ({subregion_ids_str})
              AND b1.osm_id < b2.osm_id
        """
    )
    common_border_matrix = {}  # {subregion_id: { subregion_id: float} } where len > 0
    for rec in cursor:
        border_len = float(rec[2])
        if border_len == 0.0:
            continue
        osm_id1 = int(rec[0])
        osm_id2 = int(rec[1])
        common_border_matrix.setdefault(osm_id1, {})[osm_id2] = border_len
        common_border_matrix.setdefault(osm_id2, {})[osm_id1] = border_len
    return common_border_matrix


def find_golden_splitting(conn, border_id, next_level,
                          country_region_name, mwm_size_thr):
    subregions = get_subregions_info(conn, border_id, osm_table,
                                     next_level, need_cities=True)
    if not subregions:
        return

    dcu = DisjointClusterUnion(border_id, subregions, mwm_size_thr)
    #save_splitting_to_file(dcu, f'all_{country_region_name}')
    all_subregion_ids = dcu.get_all_subregion_ids()
    common_border_matrix = calculate_common_border_matrix(conn, all_subregion_ids)

    i = 0
    while True:
        if dcu.get_cluster_count() == 1:
            return dcu
        i += 1
        smallest_cluster_id = dcu.get_smallest_cluster()
        if not smallest_cluster_id:
            return dcu
        best_cluster_id = get_best_cluster_to_join_with(smallest_cluster_id, dcu, common_border_matrix)
        if not best_cluster_id:
            dcu.mark_cluster_finished(smallest_cluster_id)
            continue
        assert (smallest_cluster_id != best_cluster_id), f"{smallest_cluster_id}"
        dcu.union(smallest_cluster_id, best_cluster_id)
    return dcu


def get_union_sql(subregion_ids):
    assert(len(subregion_ids) > 0)
    if len(subregion_ids) == 1:
        return f"""
            SELECT way FROM {osm_table} WHERE osm_id={subregion_ids[0]}
        """
    else:
        return f"""
            SELECT ST_UNION(
              ({get_union_sql(subregion_ids[0:1])}),
              ({get_union_sql(subregion_ids[1: ])})
            )
            """

def get_geojson(conn, union_sql):
    cursor = conn.cursor()
    cursor.execute(f"""SELECT ST_AsGeoJSON(({union_sql}))""")
    rec = cursor.fetchone()
    return rec[0]

def write_polygons_to_poly(file, polygons, name_prefix):
    pcounter = 1
    for polygon in polygons:
        outer = True
        for ring in polygon:
            inner_mark = '' if outer else '!'
            name = pcounter if outer else -pcounter
            file.write(f"{inner_mark}{name_prefix}_{name}\n")
            pcounter = pcounter + 1
            for coord in ring:
                file.write('\t{:E}\t{:E}\n'.format(coord[0], coord[1]))
            file.write('END\n')
            outer = False


def save_splitting_to_file(conn, dcu: DisjointClusterUnion, filename_prefix=None):
    """May be used for debugging"""
    GENERATE_ALL_POLY=False
    FOLDER='split_results'
    with open(f"{FOLDER}/{filename_prefix}.poly", 'w') as poly_file:
        poly_file.write(f"{filename_prefix}\n")
        for cluster_id, data in dcu.clusters.items():
            subregion_ids = data['subregion_ids']
            cluster_geometry_sql = get_union_sql(subregion_ids)
            geojson = get_geojson(conn, cluster_geometry_sql)
            geometry = json.loads(geojson)
            polygons = [geometry['coordinates']] if geometry['type'] == 'Polygon' else geometry['coordinates']
            name_prefix=f"{filename_prefix}_{abs(cluster_id)}"
            write_polygons_to_poly(poly_file, polygons, name_prefix)
            if GENERATE_ALL_POLY:
                with open(f"{FOLDER}/{filename_prefix}{cluster_id}.poly", 'w') as f:
                    f.write(f"{filename_prefix}_{cluster_id}")
                    write_polygons_to_poly(f, polygons, name_prefix)
                    f.write('END\n')
        poly_file.write('END\n')
    with open(f"{FOLDER}/{filename_prefix}-splitting.json", 'w') as f:
        json.dump(dcu.clusters, f, ensure_ascii=False, indent=2)


def save_splitting_to_db(conn, dcu: DisjointClusterUnion):
    cursor = conn.cursor()
    # remove previous splitting of the region
    cursor.execute(f"""
        DELETE FROM {autosplit_table}
        WHERE osm_border_id = {dcu.region_id}
          AND mwm_size_thr = {dcu.mwm_size_thr}
        """)
    for cluster_id, data in dcu.clusters.items():
        subregion_ids = data['subregion_ids']
        #subregion_ids_array_str = f"{{','.join(str(x) for x in subregion_ids)}}"
        cluster_geometry_sql = get_union_sql(subregion_ids)
        cursor.execute(f"""
          INSERT INTO {autosplit_table} (osm_border_id, subregion_ids, geom,
                                         mwm_size_thr, mwm_size_est)
          VALUES (
            {dcu.region_id},
            '{{{','.join(str(x) for x in subregion_ids)}}}',
            ({cluster_geometry_sql}),
            {dcu.mwm_size_thr},
            {data['mwm_size_est']}
          )
        """)
    conn.commit()


def get_region_and_country_names(conn, region_id):
    cursor = conn.cursor()
    try:
     cursor.execute(
      f"""SELECT name,
          (SELECT name
           FROM {osm_table}
           WHERE admin_level = 2 AND ST_contains(way, b1.way)
          ) AS country_name
          FROM osm_borders b1
          WHERE osm_id = {region_id}
            AND b1.osm_id NOT IN (-9086712)  -- crunch, stub to exclude incorrect subregions
      """
     )
     region_name, country_name = cursor.fetchone()
    except psycopg2.errors.CardinalityViolation:
        conn.rollback()
        cursor.execute(
          f"""SELECT name
              FROM {osm_table} b1
              WHERE osm_id = {region_id}
          """
        )
        region_name = cursor.fetchone()[0]
        country_name = None
        print(f"Many countries for region '{region_name}' id={region_id}")
    return region_name, country_name


def split_region(conn, region_id, next_level,
                 mwm_size_thr,
                 save_to_files=False):
    region_name, country_name = get_region_and_country_names(conn, region_id)
    region_name = region_name.replace('/', '|')
    country_region_name = f"{country_name}_{region_name}" if country_name else region_name
    dcu = find_golden_splitting(conn, region_id, next_level,
                                country_region_name, mwm_size_thr)
    if dcu is None:
        return

    save_splitting(dcu, conn, save_to_files, country_region_name)


def save_splitting(dcu: DisjointClusterUnion, conn,
                   save_to_files=None, country_region_name=None):
    save_splitting_to_db(conn, dcu)
    if save_to_files:
        print(f"Saving {country_region_name}")
        filename_prefix = f"{country_region_name}-{dcu.city_population_thr}"
        save_splitting_to_file(conn, dcu, filename_prefix)
