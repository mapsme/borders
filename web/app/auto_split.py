import itertools
import json
import psycopg2

from collections import defaultdict

from config import AUTOSPLIT_TABLE as autosplit_table


class DisjointClusterUnion:
    """Disjoint set union implementation for administrative subregions."""

    def __init__(self, region_id, subregions, thresholds):
        self.region_id = region_id
        self.subregions = subregions
        self.city_population_thr, self.cluster_population_thr = thresholds
        self.representatives = {sub_id: sub_id for sub_id in subregions}
        # a cluster is one or more subregions with common borders
        self.clusters = {}  # representative => cluster object

        # At the beginning, each subregion forms a cluster.
        # Then they would be enlarged by merging.
        for subregion_id, data in subregions.items():
            self.clusters[subregion_id] = {
                'representative': subregion_id,
                'subregion_ids': [subregion_id],
                'population': data['population'],
                'big_cities_cnt': sum(1 for c in data['cities'] if self.is_city_big(c)),
                'finished': False,  # True if the cluster cannot be merged with another
            }


    def is_city_big(self, city):
        return city['population'] >= self.city_population_thr 

    def get_smallest_cluster(self):
        """Find minimal cluster without big cities."""
        smallest_cluster_id = min(
            filter(
                lambda cluster_id: (
                    not self.clusters[cluster_id]['finished'] and
                    self.clusters[cluster_id]['big_cities_cnt'] == 0)
                ,
                self.clusters.keys()
            ),
            default=None,
            key=lambda cluster_id: self.clusters[cluster_id]['population']
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

    def get_cluster_population(self, subregion_id):
        cluster_id = self.find_cluster(subregion_id)
        return self.clusters[cluster_id]['population']

    def get_cluster_count(self):
        return len(self.clusters)

    def union(self, cluster_id1, cluster_id2):
        # To make it more deterministic
        retained_cluster_id = max(cluster_id1, cluster_id2)
        dropped_cluster_id = min(cluster_id1, cluster_id2)
        r_cluster = self.clusters[retained_cluster_id]
        d_cluster = self.clusters[dropped_cluster_id]
        r_cluster['subregion_ids'].extend(d_cluster['subregion_ids'])
        r_cluster['population'] += d_cluster['population']
        r_cluster['big_cities_cnt'] += d_cluster['big_cities_cnt']
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


def enrich_with_population_and_cities(conn, subregions):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT b.osm_id, c.name, c.population
        FROM osm_borders b, osm_cities c
        WHERE b.osm_id IN ({ids}) AND ST_CONTAINS(b.way, c.center)
        """.format(ids=','.join(str(x) for x in subregions.keys()))
    )
    for rec in cursor:
        sub_id = int(rec[0])
        subregions[sub_id]['cities'].append({
            'name': rec[1],
            'population': int(rec[2])
        })
        subregions[sub_id]['population'] += int(rec[2])


def find_subregions(conn, region_id, next_level):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT subreg.osm_id, subreg.name
        FROM osm_borders reg, osm_borders subreg
        WHERE reg.osm_id = %s AND subreg.admin_level = %s AND
              ST_Contains(reg.way, subreg.way)
        """,
        (region_id, next_level)
    )
    subregions = {
        int(rec[0]):
        {
            'osm_id': int(rec[0]),
            'name': rec[1],
            'population': 0,
            'cities': []
        }
        for rec in cursor
    }
    if subregions:
        enrich_with_population_and_cities(conn, subregions)
    return subregions


def get_best_cluster_to_join_with(small_cluster_id, dcu: DisjointClusterUnion, common_border_matrix):
    if small_cluster_id not in common_border_matrix:
        return None  # this may be if a subregion is isolated, like Bezirk Lienz inside Tyrol, Austria
    common_borders = defaultdict(lambda: 0.0)  # cluster representative => common border length
    subregion_ids = dcu.get_cluster_subregion_ids(small_cluster_id)
    for subregion_id in subregion_ids:
        for other_subregion_id, length in common_border_matrix[subregion_id].items():
            other_cluster_id = dcu.find_cluster(other_subregion_id)
            if other_cluster_id != small_cluster_id:
                common_borders[other_cluster_id] += length
    #print(f"common_borders={json.dumps(common_borders)} of len {len(common_borders)}")
    #common_borders = {k:v for k,v in common_borders.items() if v > 0.0}
    if not common_borders:
        return None
    total_common_border_length = sum(common_borders.values())
    total_adjacent_population = sum(dcu.get_cluster_population(x) for x in common_borders)
    choice_criterion = (
        (
          lambda cluster_id: (
            common_borders[cluster_id]/total_common_border_length + 
            -dcu.get_cluster_population(cluster_id)/total_adjacent_population
          )
        ) if total_adjacent_population else
        lambda cluster_id: (
            common_borders[cluster_id]/total_common_border_length
        )
    )
    small_cluster_population = dcu.get_cluster_population(small_cluster_id)
    best_cluster_id = max(
        filter(
            lambda cluster_id: (
                small_cluster_population + dcu.get_cluster_population(cluster_id)
                    <= dcu.cluster_population_thr
            ),
            common_borders.keys()
        ),
        default=None,
        key=choice_criterion
    )
    return best_cluster_id


def calculate_common_border_matrix(conn, subregion_ids):
    cursor = conn.cursor()
    # ST_Intersection returns 0 if its parameter is a geometry other than
    # LINESTRING or MULTILINESTRING
    cursor.execute("""
        SELECT b1.osm_id AS osm_id1, b2.osm_id AS osm_id2,
               ST_Length(geography(ST_Intersection(b1.way, b2.way))) AS intersection
        FROM osm_borders b1, osm_borders b2
        WHERE b1.osm_id IN ({subregion_ids_str}) AND
              b2.osm_id IN ({subregion_ids_str})
              AND b1.osm_id < b2.osm_id
        """.format(
            subregion_ids_str=','.join(str(x) for x in subregion_ids),
        )
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
                          country_region_name, thresholds):
    subregions = find_subregions(conn, border_id, next_level)
    if not subregions:
        print(f"No subregions for {border_id} {country_region_name}")
        return

    dcu = DisjointClusterUnion(border_id, subregions, thresholds)
    #save_splitting_to_file(dcu, f'all_{country_region_name}')
    all_subregion_ids = dcu.get_all_subregion_ids()
    common_border_matrix = calculate_common_border_matrix(conn, all_subregion_ids)

    i = 0
    while True:
        with open(f"clusters-{i:02d}.json", 'w') as f:
            json.dump(dcu.clusters, f, ensure_ascii=False, indent=2)
        if dcu.get_cluster_count() == 1:
            return dcu
        i += 1
        #print(f"i = {i}")
        smallest_cluster_id = dcu.get_smallest_cluster()
        if not smallest_cluster_id:
            return dcu # TODO: return target splitting
        #print(f"smallest cluster = {json.dumps(dcu.clusters[smallest_cluster_id])}")
        best_cluster_id = get_best_cluster_to_join_with(smallest_cluster_id, dcu, common_border_matrix)
        if not best_cluster_id: # !!! a case for South West England and popul 500000
            dcu.mark_cluster_finished(smallest_cluster_id)     
            continue
        assert (smallest_cluster_id != best_cluster_id), f"{smallest_cluster_id}"
        #print(f"best cluster = {json.dumps(dcu.clusters[best_cluster_id])}")
        new_cluster_id = dcu.union(smallest_cluster_id, best_cluster_id)
        #print(f"{json.dumps(dcu.clusters[new_cluster_id])}")
        #print()
        #import sys; sys.exit()
    return dcu


def get_union_sql(subregion_ids):
    assert(len(subregion_ids) > 0)
    if len(subregion_ids) == 1:
        return f"""
            SELECT way FROM osm_borders WHERE osm_id={subregion_ids[0]}
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
          AND city_population_thr = {dcu.city_population_thr}
          AND cluster_population_thr = {dcu.cluster_population_thr}
        """)
    for cluster_id, data in dcu.clusters.items():
        subregion_ids = data['subregion_ids']
        cluster_geometry_sql = get_union_sql(subregion_ids)
        cursor.execute(f"""
          INSERT INTO {autosplit_table} (osm_border_id, id, geom, city_population_thr, cluster_population_thr) VALUES (
            {dcu.region_id},
            {cluster_id},
            ({cluster_geometry_sql}),
            {dcu.city_population_thr},
            {dcu.cluster_population_thr}
          )
        """) 
    conn.commit()
    

def prepare_bulk_split():
    need_split = [
    # large region name, admin_level (2 in most cases), admin_level to split'n'merge, into subregions of what admin_level
        ('Germany', 2, 4, 6),  #  Half of the country is covered by units of AL=5
        ('Metropolitan France', 3, 4, 6),
        ('Spain', 2, 4, 6),
        ('Portugal', 2, 4, 6),
        ('Belgium', 2, 4, 6),
        ('Italy', 2, 4, 6),
        ('Switzerland', 2, 2, 4),  # has admin_level=5
        ('Austria', 2, 4, 6),
        ('Poland', 2, 4, 6),  # 380(!) of AL=6
        ('Czechia', 2, 6, 7),
        ('Ukraine', 2, 4, 6),   # should merge back to region=4 level clusters
        ('United Kingdom', 2, 5, 6),  # whole country is divided by level 4; level 5 is necessary but not comprehensive
        ('Denmark', 2, 4, 7),
        ('Norway', 2, 4, 7),
        ('Sweden', 2, 4, 7),   # though division by level 4 is currently ideal
        ('Finland', 2, 6, 7),  # though division by level 6 is currently ideal
        ('Estonia', 2, 2, 6),
        ('Latvia', 2, 4, 6),  # the whole country takes 56Mb, all 6-level units should merge into 4-level clusters
        ('Lithuania', 2, 2, 4), # now Lithuania has 2 mwms of size 60Mb each
        ('Belarus', 2, 2, 4),   # 6 regions + Minsk city. Would it be merged with the region?
        ('Slovakia', 2, 2, 4),  # there are no subregions 5, 6, 7. Must leave all 8 4-level regions
        ('Hungary', 2, 5, 6),
        #('Slovenia', 2, 2, 8),  # no levels 3,4,5,6; level 7 incomplete.
        ('Croatia', 2, 2, 6),
        ('Bosnia and Herzegovina', 2, 2, 4), # other levels - 5, 6, 7 - are incomplete.
        ('Serbia', 2, 4, 6),
        ('Romania', 2, 2, 4),
        ('Bulgaria', 2, 2, 4),
        ('Greece', 2, 4, 5),   # has 7 4-level regions, must merge 5-level to them again
        ('Ireland', 2, 5, 6),  # 5-level don't cover the whole country! Still...
        ('Turkey', 2, 3, 4),
    ]
    cursor = conn.cursor()
    regions_subset = need_split # [x for x in need_split if x[0] in ('Norway',)]
    #cursor.execute("UPDATE osm_borders SET need_split=false WHERE need_split=true")
    #cursor.execute("UPDATE osm_borders SET parent=null WHERE parent is not null")
    for country_name, country_level, split_level, lower_level in regions_subset:
        print(f"start {country_name}")
        cursor.execute(f"""
            SELECT osm_id FROM osm_borders 
            WHERE osm_id < 0 AND admin_level={country_level} AND name=%s
            """, (country_name,))
        country_border_id = None
        for rec in cursor:
            assert (not country_border_id), f"more than one country {country_name}"
            country_border_id = int(rec[0])    
        cursor.execute(f"""
            UPDATE osm_borders b
            SET need_split=true,
            next_admin_level={lower_level},
            parent = {country_border_id}
            WHERE parent IS NULL
                AND osm_id < 0 AND admin_level={split_level} AND ST_Contains(
                (SELECT way FROM osm_borders WHERE osm_id={country_border_id}),
                  b.way
            )""", (country_name,))
        cursor.execute(f"""
            UPDATE osm_borders b
            SET parent = (SELECT osm_id FROM osm_borders
                          WHERE osm_id < 0 AND admin_level={split_level} AND ST_Contains(way, b.way)
                            AND osm_id != -72639  -- crunch to exclude double Crimea region
                         )
            WHERE parent IS NULL
                AND osm_id < 0 and admin_level={lower_level} AND ST_Contains(
                  (SELECT way FROM osm_borders WHERE admin_level={country_level} AND name=%s),
                  b.way
                )""",
            (country_name,))
        conn.commit()


def process_ready_to_split(conn):
    cursor = conn.cursor()
    cursor.execute(
      f"""SELECT osm_id
          FROM osm_borders
          WHERE need_split
            -- AND osm_id IN (-8654)  -- crunch to restrict the whole process to some regions
            -- AND osm_id < -51701  -- crunch to not process what has been already processed
          ORDER BY osm_id DESC
      """
    )
    for rec in cursor:
        region_id = int(rec[0])
        split_region(region_id)


def get_region_and_country_names(conn, region_id):
    #if region_id != -1574364: return
    cursor = conn.cursor()
    try:
     cursor.execute(
      f"""SELECT name,
          (SELECT name
           FROM osm_borders
           WHERE osm_id<0 AND admin_level=2 AND ST_contains(way, b1.way)
          ) AS country_name
          FROM osm_borders b1
          WHERE osm_id={region_id}
            AND b1.osm_id NOT IN (-9086712)  -- crunch, stub to exclude incorrect subregions
      """
     )
     region_name, country_name = cursor.fetchone()
    except psycopg2.errors.CardinalityViolation:
        conn.rollback()
        cursor.execute(
          f"""SELECT name
              FROM osm_borders b1
              WHERE osm_id={region_id}
          """
        )
        region_name = cursor.fetchone()[0]
        country_name = None
        print(f"Many countries for region '{region_name}' id={region_id}")
    return region_name, country_name

DEFAULT_CITY_POPULATION_THRESHOLD = 500000
DEFAULT_CLUSTER_POPULATION_THR = 500000

def split_region(conn, region_id, next_level,
                 thresholds=(DEFAULT_CITY_POPULATION_THRESHOLD,
                             DEFAULT_CLUSTER_POPULATION_THR),
                 save_to_files=False):
    region_name, country_name = get_region_and_country_names(conn, region_id)
    region_name = region_name.replace('/', '|')
    country_region_name = f"{country_name}_{region_name}" if country_name else region_name
    dcu = find_golden_splitting(conn, region_id, next_level,
                                country_region_name, thresholds)
    if dcu is None:
        return

    save_splitting(dcu, conn, save_to_files, country_region_name)


def save_splitting(dcu: DisjointClusterUnion, conn,
                   save_to_files=None, country_region_name=None):
    save_splitting_to_db(conn, dcu)
    if save_to_files:
        print(f"Saving {country_region_name}")
        filename_prefix = f"{country_region_name}-{dcu.city_population_thrR}"
        save_splitting_to_file(conn, dcu, filename_prefix)


#PREFIX = ''
GENERATE_ALL_POLY=False
FOLDER='split_results'
#CITY_POPULATION_THR = 500000
#CLUSTER_POPULATION_THR = 500000

if __name__ == '__main__':
    conn = psycopg2.connect("dbname=az_gis3")
    prepare_bulk_split()

    import sys; sys.exit()

    process_ready_to_split(conn)
    #with open('splitting-162050.json') as f:
    import sys; sys.exit()
    #    clusters = json.load(f)
    #make_polys(clusters)
    #import sys; sys.exit()

    PREFIX = "UBavaria"
    CITY_POPULATION_THR = 500000
    CLUSTER_POPULATION_THR = 500000

    region_id = -162050 # -165475 # California  ## -162050  # Florida
    region_id = -2145274 # Upper Bavaria
    #region_id = -151339  # South West England
    #region_id = -58446   # Scotland
    dcu = find_golden_splitting(region_id)
    make_polys(dcu.clusters)
    with open(f"{PREFIX}_{CITY_POPULATION_THR}_splitting{region_id}-poplen.json", 'w') as f:
        json.dump(dcu.clusters, f, ensure_ascii=False, indent=2)



