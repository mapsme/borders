\c gis postgres

----------- Collect city polygons
CREATE TABLE osm_cities AS
  SELECT
    osm_id,
    place,
    'polygon'::text AS g_type, -- geometry_type
    max(regexp_replace(population, '[ .,]+', '', 'g')::int) AS population,
    ST_Buffer(ST_Transform(ST_Collect(way),4326), 0) AS way,
    coalesce(max("name"), max("name:en")) AS name
  FROM planet_osm_polygon
  WHERE place IN ('city', 'town')
      AND regexp_replace(population, '[ .,]+', '', 'g') ~ '^\d+$'
  GROUP BY osm_id, place;

----------- Collect city nodes
INSERT INTO osm_cities
  SELECT
    osm_id,
    place,
    'point'::text AS g_type, -- geometry_type
    regexp_replace(population, '[ .,]+', '', 'g')::int AS population,
    ST_Transform(way,4326) AS way,
    coalesce("name", "name:en") AS name
  FROM planet_osm_point
  WHERE place IN ('city', 'town')
      AND regexp_replace(population, '[ .,]+', '', 'g') ~ '^\d+$';


create index osm_cities_gist_idx on osm_cities using gist(way);


-- Delete polygons where exists a node within it with the same name

DELETE from osm_cities WHERE g_type='polygon' and osm_id IN
 (
  SELECT p.osm_id
  FROM osm_cities n, osm_cities p
  WHERE p.g_type='polygon' AND n.g_type='point'
    AND ST_Contains(p.way, n.way)
    AND (strpos(n.name, p.name) > 0 OR strpos(p.name, n.name) > 0)
 );


-- Convert [multi]polygons to points - for further faster requests "is city in region"

ALTER TABLE osm_cities ADD COLUMN center geometry;

UPDATE osm_cities c SET center =
  (
    CASE WHEN ST_Contains(way, ST_Centroid(way)) --true for 42972 out of 42999
      THEN ST_Centroid(way)
      -- for the rest 27 cities choose arbitrary point as a center
      ELSE (
             SELECT (ST_DumpPoints(way)).geom
             FROM osm_cities
             WHERE osm_id = c.osm_id
             LIMIT 1
           )
    END
  );

CREATE INDEX osm_cities_center_gist_idx ON osm_cities USING gist(center);
DROP INDEX osm_cities_gist_idx;
ALTER TABLE osm_cities DROP column way;

