#!/usr/bin/python3
import argparse
import re
import sys

import psycopg2


parser = argparse.ArgumentParser(description='Import tiles from CSV into a database')
parser.add_argument('-t', '--table', default='tiles', help='Target directory (default=tiles)')
parser.add_argument('-d', '--database', default='borders', help='Database name (default=borders)')
parser.add_argument('-v', dest='verbose', action='store_true', help='Print status messages')
options = parser.parse_args()

with psycopg2.connect(f'dbname={options.database}') as conn:
    with conn.cursor() as cur:
        cnt = 0
        for line in sys.stdin:
            m = re.match(r'^\s*(\d+)\s+(-?\d+)\s+(-?\d+)', line)
            if m:
                (count, lat, lon) = (int(m.group(1)), float(m.group(2))/100, float(m.group(3))/100)
                cur.execute(f'''
                    INSERT INTO {options.table} (count, tile) 
                        VALUES (
                                 %s,
                                 ST_SetSRID(
                                             ST_MakeBox2d(
                                                           ST_Point(%s, %s),
                                                           ST_Point(%s, %s)
                                                         ),
                                             4326
                                           )
                               )
                    ''', (count, lon, lat, lon + 0.01, lat + 0.01)
                )
                cnt += 1
            else:
                print(line)

        if options.verbose:
            print('Commit')
        conn.commit()
        if options.verbose:
            print(f'Uploaded {cnt} tiles')

