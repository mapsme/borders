#!/bin/sh
OSM2PGSQL=osm2pgsql
OSMFILTER=./osmfilter
OSMCONVERT=./osmconvert
DATABASE=gis
DATABASE_BORDERS=borders
OSM2PGSQL_KEYS='--cache 2000 --number-processes 6'
OSM2PGSQL_STYLE=

if [[ ! -r "$1" ]]
then
	echo Import borders and towns from the planet into osm_borders table
	echo Syntax: $0 \<planet_file\>
	exit 1
fi
PLANET=$1

# 0. Test for all required tools and files
if ! which -s psql; then
	echo "Do you have postgresql installed?"
	exit 1
fi
if ! which -s $OSM2PGSQL; then
	echo "No osm2pgsql found."
	exit 1
fi
if [ ! -x "$OSMFILTER" ]; then
	wget -O - http://m.m.i24.cc/osmfilter.c |cc -x c - -O3 -o $OSMFILTER
fi
if [ ! -x "$OSMCONVERT" ]; then
	wget -O - http://m.m.i24.cc/osmconvert.c | cc -x c - -lz -O3 -o $OSMCONVERT
fi

# 1. Filter planet file, leaving only administrative borders (and cities)
echo Filtering planet
FILTERED=$(mktemp -t osmadm)
$OSMFILTER $PLANET --keep="boundary=administrative or place=" --out-o5m -o=$FILTERED || exit 3

# 2. Load filtered data into an osm2pgsql database
echo Loading data into the database

# Creating a style file if we weren't provided with one
if [ -z "$OSM2PGSQL_STYLE" ]; then
	OSM2PGSQL_STYLE=$(mktemp -t osm2pgsql_style)
	OSM2PGSQL_STYLE_TMP=1
	cat > $OSM2PGSQL_STYLE <<EOSTYLE
way      admin_level text polygon
way      area        text
way      boundary    text polygon
node,way name        text linear
node,way name:en     text linear
node,way name:ru     text linear
node,way place       text polygon
node,way population  text linear
EOSTYLE
fi

$OSM2PGSQL --slim --drop --hstore --style $OSM2PGSQL_STYLE -d $DATABASE -r o5m $OSM2PGSQL_KEYS $FILTERED
RET=$?
rm $FILTERED
if [ "$OSM2PGSQL_STYLE_TMP" == "1" ]; then
	rm $OSM2PGSQL_STYLE
fi
[ $RET != 0 ] && exit 3

# 3. Make osm_borders table
echo Creating osm_borders table
psql $DATABASE -c "drop table if exists osm_borders; create table osm_borders as select min(osm_id) as osm_id, ST_Buffer(ST_Transform(ST_Collect(way),4326), 0) as way, admin_level::int as admin_level, coalesce(max(\"name:en\"), name) as name from planet_osm_polygon where boundary='administrative' and admin_level in ('2', '3', '4', '5', '6') group by name, admin_level;" || exit 3

# 4. Copy it to the borders database
echo Copying osm_borders table to the borders database
psql $DATABASE_BORDERS -c "drop table if exists osm_borders;" || exit 3
pg_dump -t osm_borders $DATABASE | psql $DATABASE_BORDERS

echo Done!
