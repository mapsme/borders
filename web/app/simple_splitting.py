import json

from borders_api_utils import (
    get_free_id,
)
from config import (
    BORDERS_TABLE as borders_table,
    MWM_SIZE_THRESHOLD,
)
from subregions import (
    update_border_mwm_size_estimation,
)


def simple_split(conn, region):
    """Split region {'id', 'name', 'mwm_size_est'} (already present in borders table)
    into 2 or 4 parts"""

    mwm_size_est = region['mwm_size_est']
    #print(f"simple_split, size = {mwm_size_est}, MWM_SIZE_THRESHOLD={MWM_SIZE_THRESHOLD}")

    if mwm_size_est is None or mwm_size_est > 2 * MWM_SIZE_THRESHOLD:
        return split_into_4_parts(conn, region)
    else:
        return split_into_2_parts(conn, region)


def split_into_2_parts(conn, region):
    bbox = get_region_bbox(conn, region['id'])
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    split_vertically = (width > height)

    if split_vertically:
        mid_lon = (bbox[2] + bbox[0]) / 2
        min_lat = bbox[1]
        max_lat = bbox[3]
        line_sql = f"LINESTRING({mid_lon} {min_lat}, {mid_lon} {max_lat})"
        position_tag = f"(ST_XMin(geom) + ST_XMax(geom)) / 2 < {mid_lon}"
        name_tags = ('west', 'east')
    else:
        mid_lat = (bbox[3] + bbox[1]) / 2
        min_lon = bbox[0]
        max_lon = bbox[2]
        line_sql = f"LINESTRING({min_lon} {mid_lat}, {max_lon} {mid_lat})"
        position_tag = f"(ST_YMin(geom) + ST_YMax(geom)) / 2 < {mid_lat}"
        name_tags = ('south', 'north')

    free_id = get_free_id()
    ids = (free_id, free_id - 1)

    with conn.cursor() as cursor:
      with conn.cursor() as insert_cursor:
        cursor.execute(f"""
            SELECT ST_AsText(ST_CollectionExtract(ST_MakeValid(ST_Collect(geom)), 3)) AS geom,
                   {position_tag} AS is_lower
            FROM (
                  SELECT 
                    (ST_DUMP(
                       ST_Split(
                                 (
                                   SELECT geom FROM {borders_table}
                                   WHERE id = {region['id']}
                                 ),
                                 ST_GeomFromText('{line_sql}', 4326)
                               )
                            )
                    ).geom as geom
                 ) q
            GROUP BY {position_tag}
            ORDER BY 2 DESC
            """)
        if cursor.rowcount < 2:
            return False
        for i, ((geom, is_lower), b_id, name_tag) in enumerate(zip(cursor, ids, name_tags)):
            insert_cursor.execute(f"""
                INSERT INTO {borders_table} (id, name, parent_id, geom,
                                             modified, count_k, mwm_size_est)
                VALUES (
                    {b_id},
                    %s,
                    {region['id']},
                    ST_GeomFromText(%s, 4326),
                    now(),
                    -1,
                    NULL
                )
                """, (f"{region['name']}_{name_tag}", geom)
            )
        for b_id in ids:
            update_border_mwm_size_estimation(conn, b_id)
        return True


def split_into_4_parts(conn, region):
    bbox = get_region_bbox(conn, region['id'])
    mid_lon = (bbox[2] + bbox[0]) / 2
    mid_lat = (bbox[3] + bbox[1]) / 2
    min_lat = bbox[1]
    max_lat = bbox[3]
    min_lon = bbox[0]
    max_lon = bbox[2]
    position_tag_X = f"(ST_XMin(geom) + ST_XMax(geom)) / 2 < {mid_lon}"
    position_tag_Y = f"(ST_YMin(geom) + ST_YMax(geom)) / 2 < {mid_lat}"
    line_sql = (
            "LINESTRING("
            f"{min_lon} {mid_lat},"
            f"{max_lon} {mid_lat},"
            f"{max_lon} {min_lat},"
            f"{mid_lon} {min_lat},"
            f"{mid_lon} {max_lat}"
            ")"
    )

    # 4 quadrants are defined by a pair of (position_tag_X, position_tag_Y)
    name_tags = {
        (True, True)  : 'southwest',
        (True, False) : 'northwest',
        (False, True) : 'southeast',
        (False, False): 'northeast'
    }


    with conn.cursor() as cursor:
      with conn.cursor() as insert_cursor:
        query = f"""
            SELECT ST_AsText(ST_CollectionExtract(ST_MakeValid(ST_Collect(geom)), 3)) AS geom,
                   {position_tag_X},
                   {position_tag_Y}
            FROM (
                  SELECT
                    (ST_DUMP(
                       ST_Split(
                                 (
                                   SELECT geom FROM {borders_table}
                                   WHERE id = {region['id']}
                                 ),
                                 ST_GeomFromText('{line_sql}', 4326)
                               )
                            )
                    ).geom as geom
                 ) q
            GROUP BY {position_tag_X}, {position_tag_Y}
            """
        cursor.execute(query)
        if cursor.rowcount < 2:
            return False

        free_id = get_free_id()
        used_ids = []
        for geom, is_lower_X, is_lower_Y in cursor:
            name_tag = name_tags[(is_lower_X, is_lower_Y)]
            insert_cursor.execute(f"""
                INSERT INTO {borders_table} (id, name, parent_id, geom,
                                             modified, count_k, mwm_size_est)
                VALUES (
                    {free_id},
                    %s,
                    {region['id']},
                    ST_GeomFromText(%s, 4326),
                    now(),
                    -1,
                    NULL
                )
                """, (f"{region['name']}_{name_tag}", geom)
            )
            used_ids.append(free_id)
            free_id -= 1
        for b_id in used_ids:
            update_border_mwm_size_estimation(conn, b_id)
        return True


def get_region_bbox(conn, region_id):
    """Return [xmin, ymin, xmax, ymax] array for the region from borders table""" 
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT ST_AsGeoJSON(BOX2D(geom))
            FROM {borders_table}
            WHERE id = %s
            """, (region_id,))
        geojson = json.loads(cursor.fetchone()[0])
        bb = geojson['coordinates'][0]
        # bb[0] is the [xmin, ymin] corner point, bb[2] - [xmax, ymax]
        return bb[0] + bb[2]

