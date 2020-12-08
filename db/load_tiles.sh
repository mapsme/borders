#!/bin/sh
DATABASE=borders
TABLE=tiles
DB_USER=borders

if [ ! -r "$PLANET-tiles.csv" ]; then
	echo "Planet file cannot be found or read."
	exit 1
fi

set -e -u

echo Loading tiles into the database
cat $PLANET-tiles.csv | python3 tiles2pg.py -d $DATABASE -t $TABLE
rm -f $PLANET-tiles.csv

psql -U $DB_USER -d $DATABASE -c "CREATE INDEX ${TABLE}_idx ON ${TABLE} USING gist (tile)"
