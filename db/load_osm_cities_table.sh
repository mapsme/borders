DATABASE=gis
DATABASE_BORDERS=borders

pg_dump -O -t osm_cities $DATABASE | psql -U borders $DATABASE_BORDERS
