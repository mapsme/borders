#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import os, argparse

def parse_double_points(line):
    if "Double" in line:
        words = line.split()
        lat = words[9].split("(")[1][:-1]
        lon = words[10].split(")")[0]
        return float(lon), float(lat), 1

def parse_unknown_outgoing(line):
    if "Unknowing" in line:
        words = line.split()
        lat = words[9]
        lon = words[10]
        return float(lon), float(lat), 2

filters = (parse_double_points, parse_unknown_outgoing)

parser = argparse.ArgumentParser(description='Extract borders warning points from generator log files to databse.')
parser.add_argument('-s', '--source', help='Generator log file path.')
parser.add_argument('-c', '--connection', help='Database connection string.')
parser.add_argument('-t', '--truncate', action='store_true', help='Truncate old data. WARINIG old data will be lost!')
parser.add_argument('-v', dest='verbose', action='store_true', help='Print status messages.')
options = parser.parse_args()

# Check log file for existance.
if not os.path.exists(options.source):
    print "Generator log file", options.source, "does not exists."
    exit(1)

# Process the log.
points = []
with open(options.source) as logfile:
    for line in logfile.readlines():
        for f in filters:
            result = f(line)
            if result:
                points.append(result)
                break

# Print stats.
print "Found {} points".format(len(points))
print "There are {} points with no external mwm, and {} points if features that have many border intersections". format(
        len([a for a in points if a[2] == 2]), len([a for a in points if a[2] == 1])
        )

# Commit to the database
conn = psycopg2.connect(options.connection)
cursor = conn.cursor()

if options.truncate:
    print "Truncating  old data..."
    cursor.execute("TRUNCATE TABLE points")

cursor.execute("INSERT into points (geom, type) VALUES {}".format(",".
    join(["(ST_GeomFromText('POINT({} {})', 4326), {})".format(*p) for p in points])))
conn.commit()
