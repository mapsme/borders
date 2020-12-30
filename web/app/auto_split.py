import itertools
from collections import defaultdict

from config import (
        AUTOSPLIT_TABLE as autosplit_table,
        OSM_TABLE as osm_table,
        MWM_SIZE_THRESHOLD,
)
from subregions import get_subregions_info


class DisjointClusterUnion:
    """Disjoint set union implementation for administrative subregions."""

    def __init__(self, region_id, subregions, next_level, mwm_size_thr=None):
        assert all(s_data['mwm_size_est'] is not None
                    for s_data in subregions.values())
        self.region_id = region_id
        self.subregions = subregions
        self.next_level = next_level
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
        """Find minimal cluster."""
        smallest_cluster_id = min(
            (cluster_id for cluster_id in self.clusters.keys()
                if not self.clusters[cluster_id]['finished']),
            default=None,
            key=lambda cluster_id: self.clusters[cluster_id]['mwm_size_est']
        )
        return smallest_cluster_id

    def find_cluster(self, subregion_id):
        if self.representatives[subregion_id] == subregion_id:
            return subregion_id
        else:
            representative = self.find_cluster(self.representatives[subregion_id])
            self.representatives[subregion_id] = representative
            return representative

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
                                  common_border_matrix,
                                  dcu: DisjointClusterUnion):
    if small_cluster_id not in common_border_matrix:
        # This may be if a subregion is isolated,
        # like Bezirk Lienz inside Tyrol, Austria
        return None
    common_borders = defaultdict(float)  # cluster representative => common border length
    subregion_ids = dcu.get_cluster_subregion_ids(small_cluster_id)
    for subregion_id in subregion_ids:
        for other_subregion_id, length in common_border_matrix[subregion_id].items():
            other_cluster_id = dcu.find_cluster(other_subregion_id)
            if (other_cluster_id != small_cluster_id and
                    not dcu.clusters[other_cluster_id]['finished']):
                common_borders[other_cluster_id] += length
    if not common_borders:
        return None

    total_common_border_length = sum(common_borders.values())
    total_adjacent_mwm_size_est = sum(dcu.clusters[x]['mwm_size_est'] for x in common_borders)

    if total_adjacent_mwm_size_est:
        choice_criterion = lambda cluster_id: (
            common_borders[cluster_id] / total_common_border_length +
            -dcu.clusters[cluster_id]['mwm_size_est'] / total_adjacent_mwm_size_est
        )
    else:
        choice_criterion = lambda cluster_id: (
            common_borders[cluster_id] / total_common_border_length
        )

    best_cluster_id = max(
        filter(
            lambda cluster_id: (
                (dcu.clusters[small_cluster_id]['mwm_size_est']
                    + dcu.clusters[cluster_id]['mwm_size_est']) <= dcu.mwm_size_thr
            ),
            common_borders.keys()
        ),
        default=None,
        key=choice_criterion
    )
    return best_cluster_id


def calculate_common_border_matrix(conn, subregion_ids):
    subregion_ids_str = ','.join(str(x) for x in subregion_ids)
    # ST_Length returns 0 if its parameter is a geometry other than
    # LINESTRING or MULTILINESTRING
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT b1.osm_id AS osm_id1, b2.osm_id AS osm_id2,
                   ST_Length(geography(ST_Intersection(b1.way, b2.way)))
            FROM {osm_table} b1, {osm_table} b2
            WHERE b1.osm_id IN ({subregion_ids_str})
              AND b2.osm_id IN ({subregion_ids_str})
              AND b1.osm_id < b2.osm_id
            """
        )
        common_border_matrix = {}  # {subregion_id: { subregion_id: float} } where len > 0
        for osm_id1, osm_id2, border_len in cursor:
            if border_len == 0.0:
                continue
            common_border_matrix.setdefault(osm_id1, {})[osm_id2] = border_len
            common_border_matrix.setdefault(osm_id2, {})[osm_id1] = border_len
    return common_border_matrix


def find_golden_splitting(conn, border_id, next_level, mwm_size_thr):
    subregions = get_subregions_info(conn, border_id, osm_table,
                                     next_level, need_cities=True)
    if not subregions:
        return
    if any(s_data['mwm_size_est'] is None for s_data in subregions.values()):
        return

    dcu = DisjointClusterUnion(border_id, subregions, next_level, mwm_size_thr)
    all_subregion_ids = dcu.get_all_subregion_ids()
    common_border_matrix = calculate_common_border_matrix(conn, all_subregion_ids)

    while True:
        if len(dcu.clusters) == 1:
            return dcu
        smallest_cluster_id = dcu.get_smallest_cluster()
        if not smallest_cluster_id:
            return dcu
        best_cluster_id = get_best_cluster_to_join_with(smallest_cluster_id,
                                                        common_border_matrix,
                                                        dcu)
        if not best_cluster_id:
            dcu.clusters[smallest_cluster_id]['finished'] = True
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
            SELECT ST_Union(
              ({get_union_sql(subregion_ids[0:1])}),
              ({get_union_sql(subregion_ids[1:])})
            )
            """


def save_splitting_to_db(conn, dcu: DisjointClusterUnion):
    with conn.cursor() as cursor:
        # Remove previous splitting of the region
        cursor.execute(f"""
            DELETE FROM {autosplit_table}
            WHERE osm_border_id = {dcu.region_id}
              AND mwm_size_thr = {dcu.mwm_size_thr}
              AND next_level = {dcu.next_level}
            """)
        for cluster_id, data in dcu.clusters.items():
            subregion_ids = data['subregion_ids']
            subregion_ids_array_str = (
                    '{' + ','.join(str(x) for x in subregion_ids) + '}'
            )
            cluster_geometry_sql = get_union_sql(subregion_ids)
            cursor.execute(f"""
                INSERT INTO {autosplit_table} (osm_border_id, subregion_ids, geom,
                                               next_level, mwm_size_thr, mwm_size_est)
                  VALUES (
                    {dcu.region_id},
                    '{subregion_ids_array_str}',
                    ({cluster_geometry_sql}),
                    {dcu.next_level},
                    {dcu.mwm_size_thr},
                    {data['mwm_size_est']}
                  )
                """)
    conn.commit()


def split_region(conn, region_id, next_level, mwm_size_thr):
    dcu = find_golden_splitting(conn, region_id, next_level, mwm_size_thr)
    if dcu is None:
        return
    save_splitting_to_db(conn, dcu)

    ## May need to debug
    #from auto_split_debug import save_splitting_to_file
    #save_splitting_to_file(conn, dcu)
