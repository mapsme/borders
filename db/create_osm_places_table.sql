\c gis postgres

----------- Collect city polygons
CREATE TABLE osm_places AS
  SELECT
    osm_id,
    place,
    'polygon'::text AS g_type, -- geometry_type
    max(CASE
          WHEN regexp_replace(population, '[ .,]+', '', 'g') ~ '^\d+$'
          THEN regexp_replace(population, '[ .,]+', '', 'g')::int
          ELSE NULL
        END
       ) AS population,
    ST_Buffer(ST_Transform(ST_Collect(way),4326), 0) AS way,
    coalesce(max("name"), max("name:en")) AS name
  FROM planet_osm_polygon
  WHERE place IN ('city', 'town', 'village', 'hamlet', 'isolated_dwelling')
  GROUP BY osm_id, place;

----------- Collect city nodes
INSERT INTO osm_places
  SELECT
    osm_id,
    place,
    'point'::text AS g_type, -- geometry_type
    CASE
      WHEN regexp_replace(population, '[ .,]+', '', 'g') ~ '^\d+$'
      THEN regexp_replace(population, '[ .,]+', '', 'g')::int
      ELSE NULL
    END AS population,
    ST_Transform(way,4326) AS way,
    coalesce("name", "name:en") AS name
  FROM planet_osm_point
  WHERE place IN ('city', 'town', 'village', 'hamlet', 'isolated_dwelling');


create index osm_places_gist_idx on osm_places using gist(way);

-- Update node population with polygon population where
-- the polygon duplicates the node and node has no population

select count(*) from osm_places where g_type='point' and population is null;

UPDATE osm_places
SET population = q.max_population
FROM
(
  SELECT n.osm_id node_id, greatest(p.population, n.population) max_population
  FROM osm_places n, osm_places p
  WHERE p.g_type='polygon' AND n.g_type='point'
    AND ST_Contains(p.way, n.way)
    AND (strpos(n.name, p.name) > 0 OR strpos(p.name, n.name) > 0)
) q
WHERE g_type='point' and osm_id = q.node_id;


-- Delete polygons where exists a node within it with the same name

DELETE from osm_places WHERE g_type='polygon' and osm_id IN
 (
  SELECT p.osm_id
  FROM osm_places n, osm_places p
  WHERE p.g_type='polygon' AND n.g_type='point'
    AND ST_Contains(p.way, n.way)
    AND (strpos(n.name, p.name) > 0 OR strpos(p.name, n.name) > 0)
 );


-- Convert [multi]polygons to points - for further faster requests "is city in region"

ALTER TABLE osm_places ADD COLUMN center geometry;

UPDATE osm_places c SET center =
  (
    CASE WHEN ST_Contains(way, ST_Centroid(way)) --true for 42972 out of 42999
      THEN ST_Centroid(way)
      -- for the rest 27 cities choose arbitrary point as a center
      ELSE (
             SELECT (ST_DumpPoints(way)).geom
             FROM osm_places
             WHERE osm_id = c.osm_id
             LIMIT 1
           )
    END
  );

CREATE INDEX osm_places_center_gist_idx ON osm_places USING gist(center);
DROP INDEX osm_places_gist_idx;
ALTER TABLE osm_places DROP column way;
