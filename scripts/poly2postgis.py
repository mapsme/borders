#!/usr/bin/python
import glob

import psycopg2


def read_polygon(f):
	"""Reads an array of coordinates with the final 'END' line."""
	coords = []
	while True:
		line = f.readline()
		# stop on EOF
		if not line:
			break
		line = line.strip()
		# stop on polygon end
		if line == 'END':
			break
		# skip whitespace lines
		if not line:
			continue
		# append coords
		ords = line.split()
		coords.append("%f %f" % (float(ords[0]), float(ords[1])))
	if len(coords) < 3:
		return None
	if coords[0] != coords[-1]:
		coords.append(coords[0])
	return '({})'.format(','.join(coords))


def read_multipolygon(f):
	"""Read the entire poly file and parse in into a WKT."""
	polygons = []
	cur_poly = []
	while True:
		title = f.readline().strip()
		if not title:
			return None
		if title == 'END':
			break
		outer = title.strip()[0] != '!'
		polygon = read_polygon(f)
		if polygon != None:
			if outer:
				if cur_poly:
					polygons.append('({})'.format(','.join(cur_poly)))
				cur_poly = [polygon]
			else:
				cur_poly.append(polygon)
	if cur_poly:
		polygons.append('({})'.format(','.join(cur_poly)))
		
	if len(polygons) == 1:
		return "POLYGON" + polygons[0]
	else:
		return "MULTIPOLYGON({})".format(','.join(polygons))


def convert_poly(input_file, cur):
	"""Reads a multipolygon from input_file and inserts it into borders table."""
	with open(input_file, 'r') as f:
		name = f.readline().strip()
		wkt = read_multipolygon(f)
	print '  ', name
	try:
		cur.execute('INSERT INTO borders (name, geom, modified) VALUES (%s, ST_GeomFromText(%s), now())', (name, wkt))
	except psycopg2.Error as e:
		print wkt
		raise e


if __name__ == "__main__":
	conn = psycopg2.connect('dbname=borders')
	cur = conn.cursor()
	for f in glob.iglob('*.poly'):
		convert_poly(f, cur)
	conn.commit()
	cur.close()
	conn.close()
