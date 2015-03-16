#!/usr/bin/python
import psycopg2, sys, re, argparse

parser = argparse.ArgumentParser(description='Import tiles from CSV into a database')
parser.add_argument('-t', '--table', default='tiles', help='Target directory (default=tiles)')
parser.add_argument('-d', '--database', default='borders', help='Database name (default=borders)')
parser.add_argument('-v', dest='verbose', action='store_true', help='Print status messages')
options = parser.parse_args()

conn = psycopg2.connect("dbname={}".format(options.database))
cur = conn.cursor()

cnt = 0
for line in sys.stdin:
	m = re.match(r'^\s*(\d+)\s+(-?\d+)\s+(-?\d+)', line)
	if m:
		(count, lat, lon) = (int(m.group(1)), float(m.group(2))/100, float(m.group(3))/100)
		cur.execute('insert into {} (count, tile) values (%s, ST_SetSRID(ST_MakeBox2d(ST_Point(%s, %s), ST_Point(%s, %s)), 4326));'.format(options.table), (count, lon, lat, lon + 0.01, lat + 0.01))
		cnt = cnt + 1
	else:
		print line

if options.verbose:
	print 'Commit'
conn.commit()
if options.verbose:
	print 'Uploaded {} tiles'.format(cnt)
cur.close()
conn.close()
