DATABASE=gis
DATABASE_BORDERS=borders

pg_dump -O -t osm_places $DATABASE | psql -U borders $DATABASE_BORDERS
