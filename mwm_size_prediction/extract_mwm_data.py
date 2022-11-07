import psycopg2
import sys
import json
from time import time


BORDERS_TABLE = 'borders'
OSM_BORDERS_TABLE = 'osm_borders'
OSM_PLACES_TABLE = 'osm_places'
COASTLINE_TABLE = 'coastlines'
LAND_POLYGONS_TABLE = 'coasts2'


def get_regions_in_hierarchy(upper_region):
    """We excluded country-level regions since they are too computationally
    costly, and there is no need in MWMs of Germany/Japan size.
    We would be able to calculate their parameters later by summing up
    childrens.
    """
    upper_region_identifier_column = 'name'
    try:
        upper_region = int(upper_region)
        upper_region_identifier_column = 'id'
    except ValueError:
        pass

    query = f"""
      WITH RECURSIVE regions AS (
         SELECT id FROM {BORDERS_TABLE} WHERE {upper_region_identifier_column} = %s
         UNION ALL
         SELECT child.id
         FROM {BORDERS_TABLE} child JOIN regions ON child.parent_id = regions.id
      )
      SELECT id, parent_id, name,
          (SELECT admin_level FROM {OSM_BORDERS_TABLE} WHERE osm_id = id) AL
      FROM {BORDERS_TABLE}
      WHERE id IN (SELECT id FROM regions)
          AND (SELECT admin_level FROM {OSM_BORDERS_TABLE} WHERE osm_id = id) > 2
      """

    regions = {}  # id => data
    with conn.cursor() as cursor:
        cursor.execute(query, (upper_region,))
        for region_id, parent_id, name, al in cursor:
            regions[region_id] = {
                'id': region_id,
                'parent_id': parent_id,
                'name': name,
                'al': al,
                'full_area': None,
                'land_area': None,
                'coastline_length': None,
                'city_cnt': 0,
                'city_pop': 0,
                'hamlet_cnt': 0,
                'hamlet_pop': 0,
            }
        return regions


def calculate_area(regions):
    """regions parameter may be calculated by the get_regions_in_hierarchy()
    function or may be loaded from CSV/JSON, enriched with area and unloaded back.
    """
    if not regions:
        return
    ids_str = ','.join(str(x) for x in regions.keys())
    query = f"""
      SELECT id,
          ST_Area(geography(geom))/1e6 full_area,
          ST_Area(
              geography(
                  ST_Intersection(
                      b.geom,
                      (
                        SELECT ST_Union(c.geom)
                        FROM {LAND_POLYGONS_TABLE} c
                        WHERE c.geom && b.geom
                      )
                  )
              )
          ) / 1e6 land_area
      FROM {BORDERS_TABLE} b
      WHERE id IN ({ids_str})
      """

    with conn.cursor() as cursor:
        cursor.execute(query)
        for region_id, full_area, land_area in cursor:
            data = regions[region_id]
            data['full_area'] = full_area
            data['land_area'] = land_area

    return regions


def calculate_population(regions):
    """regions parameter may be calculated by the get_regions_in_hierarchy()
    function or may be loaded from CSV/JSON, enriched with population
    and unloaded back.
    """
    if not regions:
        return
    ids_str = ','.join(str(x) for x in regions.keys())
    query = f"""
        SELECT b_id id, place_type, count(*) cnt, SUM(population) pop
        FROM (
          SELECT c.population, b.id b_id, b.name b_name,
          CASE WHEN c.place IN ('city', 'town') THEN 'city' ELSE 'hamlet' END AS place_type
          FROM {BORDERS_TABLE} b, {OSM_PLACES_TABLE} c
          WHERE b.id IN ({ids_str})
              AND ST_CONTAINS(b.geom, c.center)
        ) q
        GROUP BY b_id, b_name, place_type
        ORDER BY b_id
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        for b_id, place_type, cnt, pop in cursor:
            cnt_key = f"{place_type}_cnt"
            pop_key = f"{place_type}_pop"
            regions[b_id][cnt_key] = cnt       # count(*) cannot be NULL
            regions[b_id][pop_key] = pop or 0  # sum(col) can be NULL

    return regions


def calculate_coastline_length(regions):
    """regions parameter may be calculated by the get_regions_in_hierarchy()
    function or may be loaded from CSV/JSON, enriched with coastline length
    and unloaded back.
    """
    if not regions:
        return

    for b_data in regions.values():
        b_data['coastline_length'] = 0.0

    ids_str = ','.join(str(x) for x in regions.keys())
    #print(f"ids_str = {ids_str}")
    query = f"""
        WITH brds AS (
            SELECT id, name, ST_Buffer(geom, 0.001) geom
            FROM {BORDERS_TABLE}
            WHERE id IN ({ids_str})
        )
        SELECT brds.id,
               brds.name,
               SUM(
                   ST_Length(
                       geography(
                           ST_Intersection(
                               brds.geom,
                               c.geom
                           )
                       )
                   )
               ) / 1e3
        FROM {COASTLINE_TABLE} c, brds
        WHERE c.geom && brds.geom
        GROUP BY brds.id, brds.name
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        for b_id, name, coastline_length in cursor:
            #print(f"got length of {name} = {coastline_length} km")
            regions[b_id]['coastline_length'] = coastline_length

    return regions


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <region_name_or_id>")
        print(f"       where region is primarily a country")
        sys.exit()

    upper_region = sys.argv[1]

    t1 = time()
    regions = get_regions_in_hierarchy(upper_region)
    t2 = time()
    print(f"get_regions_in_hierarchy() time: {(t2-t1)/60:.2f} m")

    calculate_area(regions)
    t3 = time()
    print(f"calculate_area()           time: {(t3-t2)/60:.2f} m")

    calculate_population(regions)
    t4 = time()
    print(f"calculate_population()     time: {(t4-t3)/60:.2f} m")

    calculate_coastline_length(regions)
    t5 = time()
    print(f"calculate_coastline_length() ti: {(t5-t4)/60:.2f} m")

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
    pref_region_ids = get_subregion_ids(japan_id, 4)
    for pref_id in pref_region_ids:
        copy_from_osm(pref_id, japan_id)
        county_ids = get_subregion_ids(pref_id, 6)
        for c_id in county_ids:
            copy_from_osm(c_id, pref_id)
        city_ids = get_subregion_ids(pref_id, 7, check_intersection=True)
        for c_id in city_ids:
            copy_from_osm(c_id, pref_id)
    conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect('dbname=az_gis3')
    main()
    #make_japan()

