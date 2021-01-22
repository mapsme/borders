import psycopg2
import sys
import json
from time import time


def get_regions_info(upper_region):
    upper_region_identifier_column = 'name'
    try:
        upper_region = int(upper_region)
        upper_region_identifier_column = 'id'
    except ValueError:
        pass

    query = f"""
      WITH RECURSIVE regions AS (
         SELECT id FROM borders WHERE {upper_region_identifier_column} = %s
         UNION ALL
         SELECT child.id
         FROM borders child JOIN regions ON child.parent_id = regions.id
      )
      SELECT id, parent_id,
          (SELECT admin_level FROM osm_borders WHERE osm_id = id) AL,
          name,
          ST_Area(geography(geom))/1e6 area,
          ST_Area(geography(ST_Intersection(
                               b.geom,
                               (SELECT ST_Union(c.geom)
                                FROM coasts c
                                WHERE c.geom && b.geom
                               )
                 ))) / 1e6 land_area
      FROM borders b
      WHERE id IN (SELECT id FROM regions) AND (SELECT admin_level FROM osm_borders WHERE osm_id = id) > 2
      """

    t1 = time()
    regions = {}  # id => data
    with conn.cursor() as cursor:
        cursor.execute(query, (upper_region,))
        for region_id, parent_id, al, name, area, land_area in cursor:
            regions[region_id] = {
                'id': region_id,
                'parent_id': parent_id,
                'al': al,
                'name': name,
                'full_area': area,
                'land_area': land_area,
                'city_cnt': 0,
                'city_pop': 0,
                'hamlet_cnt': 0,
                'hamlet_pop': 0,
            }
    t2 = time()
    print(f"General info time (including area calculation): {(t2-t1)/60:.2f} m")
    count_population(regions)
    t3 = time()
    print(f"Population  time: General info time           : {(t3-t2)/60:.2f} m")
    return regions


def count_population(regions):
    ids_str = ','.join(str(x) for x in regions.keys())
    query = f"""
        SELECT b_id id, b_name region_name, place_type, count(*) cnt, SUM(population) pop
        FROM (
          SELECT c.population, b.id b_id, b.name b_name,
          CASE WHEN c.place IN ('city', 'town') THEN 'city' ELSE 'hamlet' END AS place_type
          FROM borders b, osm_places c
          WHERE b.id IN ({ids_str})
              AND ST_CONTAINS(b.geom, c.center)
        ) q
        GROUP BY b_id, b_name, place_type
        ORDER BY b_id
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        for b_id, b_name, place_type, cnt, pop in cursor:
            cnt_key = f"{place_type}_cnt"
            pop_key = f"{place_type}_pop" or 0
            regions[b_id][cnt_key] = cnt
            regions[b_id][pop_key] = pop or 0

    return regions


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <region_name_or_id>")
        print(f"       where region is primarily a country")
        sys.exit()

    upper_region = sys.argv[1]
    regions = get_regions_info(upper_region)
    upper_region_name = regions[upper_region]['name'] if isinstance(upper_region, int) else upper_region

    with open(f"{upper_region_name}_regions.json", "w") as f:
        json.dump(regions, f, ensure_ascii=False, indent=2)



def make_japan():
    """In Japan (4)-level regions are divided into subregions
    of different AL = {6,7}, and we take them as leaf. So first we take
    (6)-subregions of (4)-regions, and then those (7)-subregions of (4) that
    do not intersect with already created (6)-subregions.
    """

    def copy_from_osm(reg_id, parent_id):
        print(f"copy_from_osm id={reg_id}")
        with conn.cursor() as cursor:
          cursor.execute(f"""
            INSERT INTO borders
                (id, geom, name, parent_id, modified, count_k)
              SELECT osm_id, way, name, %s, now(), -1
              FROM osm_borders
              WHERE osm_id = %s
              """, (parent_id, reg_id,))

    def get_subregion_ids(reg_id, next_level, check_intersection=False):
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT subreg.osm_id
                FROM borders reg, osm_borders subreg
                WHERE reg.id = %s AND subreg.admin_level = %s AND
                      ST_Contains(reg.geom, subreg.way)
                """, (reg_id, next_level)
            )
            subreg_ids = [rec[0] for rec in cursor]
            if check_intersection and subreg_ids:
                cursor.execute(f"""
                    SELECT b1.osm_id
                    FROM osm_borders b1, osm_borders b2
                    WHERE b1.osm_id in ({','.join(str(x) for x in subreg_ids)})
                      AND b2.osm_id IN (SELECT id FROM borders WHERE parent_id={reg_id})
                      AND (b1.way && b2.way AND ST_Relate(b1.way, b2.way, '2********'))
                    """)
                intersect_ids = [rec[0] for rec in cursor]
                #print(f"intersect_ids amonth children of {reg_id} = {intersect_ids}")
                subreg_ids = list(set(subreg_ids) - set(intersect_ids))
        return subreg_ids

    japan_id = -382313

    copy_from_osm(japan_id, None)
    # Prefectures
    pref_region_ids = get_subregion_ids(japan_id, 4)
    for pref_id in pref_region_ids:
        copy_from_osm(pref_id, japan_id)
        # Counties
        county_ids = get_subregion_ids(pref_id, 6)
        for c_id in county_ids:
            copy_from_osm(c_id, pref_id)
        # Cities that are direct children on prefectures
        city_ids = get_subregion_ids(pref_id, 7, check_intersection=True)
        for c_id in city_ids:
            copy_from_osm(c_id, pref_id)
    conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect('dbname=az_gis3')
    main()
    #make_japan()

