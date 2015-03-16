#!/bin/sh
OSMCONVERT=./osmconvert
DATABASE=borders
TABLE=tiles

if [[ ! -r "$1" ]]
then
	echo Calculate tile densities for a planet
	echo Syntax: $0 \<planet_file\>
	exit 1
fi

set -e -u

if ! which -s psql; then
	echo "Do you have postgresql installed?"
	exit 1
fi
if [ ! -x "$OSMCONVERT" ]; then
	wget -O - http://m.m.i24.cc/osmconvert.c | cc -x c - -lz -O3 -o $OSMCONVERT
fi

PLANET=$(echo $(basename $1) | sed 's/\..*//')

echo Extracting node coordinates
$OSMCONVERT --out-osm $1 | perl -n -e 'print sprintf "%d %d\n", $1*100, $2*100 if /<node.+lat="([^"]+)".+lon="([^"]+)"/;' > $PLANET-nodes.csv

echo Sorting node list
LC_ALL=C sort -o $PLANET-nodes-sorted.csv $PLANET-nodes.csv
rm $PLANET-nodes.csv

echo Counting unique tiles
LC_ALL=C uniq -c $PLANET-nodes-sorted.csv $PLANET-tiles.csv
rm $PLANET-nodes-sorted.csv

echo Cleaning up tiles table and index
psql $DATABASE -c "DELETE FROM $TABLE; DROP INDEX IF EXISTS ${TABLE}_idx;"

echo Loading tiles into the database
pv $PLANET-tiles.csv | python tiles2pg.py -d $DATABASE -t $TABLE
rm $PLANET-tiles.csv

echo Indexing tiles
psql $DATABASE -c "CREATE INDEX ${TABLE}_idx ON $TABLE USING GIST (tile);"

echo Done!
