import psycopg2

from config import (
    COASTLINE_TABLE as coastline_table,
    LAND_POLYGONS_TABLE as land_polygons_table,
)


def is_land_table_available(conn):
    with conn.cursor() as cursor:
        try:
            cursor.execute(f"""SELECT * FROM {land_polygons_table} LIMIT 2""")
            return True
        except psycopg2.Error as e:
            conn.rollback()
            return False


def is_coastline_table_available(conn):
    with conn.cursor() as cursor:
        try:
            cursor.execute(f"""SELECT * FROM {coastline_table} LIMIT 2""")
            return True
        except psycopg2.Error as e:
            conn.rollback()
            return False

