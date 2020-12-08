#!/bin/sh
OSMCONVERT=osmconvert

if [[ ! -r "$PLANET" ]]; then
	echo "Planet file cannot be found or read."
	exit 1
fi

set -e -u

echo Extracting node coordinates
$OSMCONVERT --out-osm $PLANET | perl -n -e 'print sprintf "%d %d\n", $1*100, $2*100 if /<node.+lat="([^"]+)".+lon="([^"]+)"/;' > $PLANET-nodes.csv

echo Sorting node list
LC_ALL=C sort -o $PLANET-nodes-sorted.csv $PLANET-nodes.csv
rm $PLANET-nodes.csv

echo Counting unique tiles
LC_ALL=C uniq -c $PLANET-nodes-sorted.csv $PLANET-tiles.csv
rm $PLANET-nodes-sorted.csv

