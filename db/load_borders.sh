#!/bin/sh
set -e
OSM2PGSQL=osm2pgsql
DATABASE=gis
DATABASE_BORDERS=borders
OSM2PGSQL_KEYS='--cache 2000 --number-processes 6'
OSM2PGSQL_STYLE=

if [[ "`uname`" == 'Darwin' ]]; then
	WHICH='which -s'
	MKTEMP='mktemp -t '
else
	WHICH=which
	MKTEMP='mktemp --suff='
fi

if ! $WHICH $OSM2PGSQL; then
	echo "No osm2pgsql found."
	exit 1
fi

# Load filtered data into an osm2pgsql database
echo Loading data into the database

# Creating a style file if we weren't provided with one
if [ -z "$OSM2PGSQL_STYLE" ]; then
	OSM2PGSQL_STYLE=$(${MKTEMP}osm2pgsql_style)
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

$OSM2PGSQL --slim --drop --hstore --style $OSM2PGSQL_STYLE -d $DATABASE -r o5m $OSM2PGSQL_KEYS $FILTERED_PLANET
RET=$?
rm -f $FILTERED_PLANET
if [ "$OSM2PGSQL_STYLE_TMP" == "1" ]; then
	rm -f $OSM2PGSQL_STYLE
fi
[ $RET != 0 ] && exit 3


# Make osm_borders table
echo Creating osm_borders table
psql $DATABASE -c "
DROP TABLE IF EXISTS osm_borders;
CREATE TABLE osm_borders AS
  SELECT
    osm_id,
    ST_Buffer(ST_Transform(ST_Collect(way),4326), 0) AS way,
    admin_level::INT AS admin_level,
    coalesce(max(\"name:en\"), max(name)) AS name
  FROM planet_osm_polygon
  WHERE boundary='administrative' AND osm_id < 0 AND admin_level IN ('2', '3', '4', '5', '6', '7')
  GROUP BY osm_id, admin_level
  HAVING coalesce(max(\"name:en\"), max(name)) IS NOT NULL;
  ALTER TABLE osm_borders ADD PRIMARY KEY (osm_id); 
;" || exit 3

# Copy it to the borders database
echo Copying osm_borders table to the borders database
pg_dump -O -t osm_borders $DATABASE | psql -U borders $DATABASE_BORDERS

