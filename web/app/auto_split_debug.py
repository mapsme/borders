import json
import os

from auto_split import (
    DisjointClusterUnion,
    get_union_sql,
)
from subregions import (
    get_region_full_name,
)


GENERATE_ALL_POLY = False
FOLDER = 'split_results'


def save_splitting_to_file(conn, dcu: DisjointClusterUnion):
    if not os.path.exists(FOLDER):
        os.mkdir(FOLDER)
    region_full_name = get_region_full_name(conn, dcu.region_id)
    filename_prefix = f"{region_full_name}-{dcu.mwm_size_thr}"
    with open(os.path.join(FOLDER, f"{filename_prefix}.poly"), 'w') as poly_file:
        poly_file.write(f"{filename_prefix}\n")
        for cluster_id, data in dcu.clusters.items():
            subregion_ids = data['subregion_ids']
            cluster_geometry_sql = get_union_sql(subregion_ids)
            geojson = get_geojson(conn, cluster_geometry_sql)
            geometry = json.loads(geojson)
            polygons = ([geometry['coordinates']]
                        if geometry['type'] == 'Polygon'
                        else geometry['coordinates'])
            name_prefix=f"{filename_prefix}_{abs(cluster_id)}"
            write_polygons_to_poly(poly_file, polygons, name_prefix)
            if GENERATE_ALL_POLY:
                with open(os.path.join(FOLDER, f"{filename_prefix}{cluster_id}.poly"), 'w') as f:
                    f.write(f"{filename_prefix}_{cluster_id}")
                    write_polygons_to_poly(f, polygons, name_prefix)
                    f.write('END\n')
        poly_file.write('END\n')
    with open(os.path.join(FOLDER, f"{filename_prefix}-splitting.json"), 'w') as f:
        json.dump(dcu.clusters, f, ensure_ascii=False, indent=2)


def get_geojson(conn, sql_geometry_expr):
    with conn.cursor() as cursor:
        cursor.execute(f"""SELECT ST_AsGeoJSON(({sql_geometry_expr}))""")
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
