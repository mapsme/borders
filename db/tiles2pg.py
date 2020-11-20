#!/usr/bin/python3

"""This script takes a file where each line of the form

<count> <lat_x_100> <lon_x_100>

represents the number of OSM nodes in a rectangular tile
[lat, lon, lat + 0.01, lon + 0.01].
lat_x_100 is latitude multiplied by 100 and truncated to an integer.
"""


import argparse
import logging
import sys

import psycopg2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import tiles from CSV into a database')
    parser.add_argument('-t', '--table', default='tiles', help='Target directory (default=tiles)')
    parser.add_argument('-d', '--database', default='borders', help='Database name (default=borders)')
    parser.add_argument('-v', dest='verbose', action='store_true', help='Print status messages')
    options = parser.parse_args()

    log_level = logging.INFO if options.verbose else logging.WARNING
    logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')

    TILE_SIDE = 0.01  # degrees

    with psycopg2.connect(f'dbname={options.database}') as conn:
        with conn.cursor() as cur:
            cnt = 0
            for line in sys.stdin:
                tokens = line.split()
                if len(tokens) == 3:
                    try:
                        (count, lat, lon) = (int(t) for t in tokens)
                    except ValueError:
                        logging.critical(f"Wrong number format at line {cnt}")
                        conn.rollback()
                        sys.exit(1)

                    lat /= 100.0
                    lon /= 100.0
                    cur.execute(f"""
                        INSERT INTO {options.table} (count, tile) 
                            VALUES (%s,
                                    ST_SetSRID(ST_MakeBox2d(ST_Point(%s, %s),
                                                            ST_Point(%s, %s)),
                                               4326)
                                   )
                        """, (count, lon, lat, lon + TILE_SIDE, lat + TILE_SIDE)
                    )
                    cnt += 1
                else:
                    logging.warning(f"Incorrect count-lat-lon line '{line}'")

            logging.info("Commit")
            conn.commit()
            logging.info(f"Uploaded {cnt} tiles")
