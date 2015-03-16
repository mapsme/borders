#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2, json
import os, argparse
import unicodedata, re

parser = argparse.ArgumentParser(description='Export borders to poly files')
parser.add_argument('-t', '--target', default='poly', help='Target directory (default=./poly)')
parser.add_argument('-k', '--keep', action='store_true', help='Keep old borders that are not present in new array')
parser.add_argument('-d', dest='database', default='borders', help='Database name (default=borders)')
parser.add_argument('-v', dest='verbose', action='store_true', help='Print status messages')
options = parser.parse_args()

if not os.path.exists(options.target):
	os.makedirs(options.target)

if not options.keep:
	if options.verbose:
		print 'Removing old polygon files...',
	counter = 0
	for filename in os.listdir(options.target):
		filepath = os.path.join(options.target, filename)
		if filepath.endswith('.poly') and os.path.isfile(filepath):
			os.unlink(filepath)
			counter = counter + 1
	if options.verbose:
		print 'Done,', counter

if options.verbose:
	print 'Requesting borders'
conn = psycopg2.connect('dbname={}'.format(options.database))
cur = conn.cursor()
cur.execute('select name, ST_AsGeoJSON(geom) from borders where disabled = false')

counter = 0
for res in cur:
	name = res[0].decode('utf-8')
	if options.verbose:
		print name,
	geometry = json.loads(res[1])
	polygons = [geometry['coordinates']] if geometry['type'] == 'Polygon' else geometry['coordinates']
	# sanitize name, src: http://stackoverflow.com/a/295466/1297601
	name = unicodedata.normalize('NFKD', name)
	name = name.encode('ascii', 'ignore')
	name = re.sub('[^\w\s_-]', '', name).strip()
	name = name + '.poly'

	while os.path.exists(os.path.join(options.target, name)):
		name = name + '_1'

	if options.verbose:
		print '→', name
	with open(os.path.join(options.target, name), 'w') as poly:
		poly.write(res[0] + '\n')
		pcounter = 1
		for polygon in polygons:
			outer = True
			for ring in polygon:
				poly.write('{}\n'.format(pcounter if outer else -pcounter))
				pcounter = pcounter + 1
				for coord in ring:
					poly.write('\t{:E}\t{:E}\n'.format(coord[0], coord[1]))
				poly.write('END\n')
				outer = False
		poly.write('END\n')
		counter = counter + 1

conn.close()
if options.verbose:
	print 'Done,', counter
