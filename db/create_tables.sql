\c borders borders

CREATE TABLE tiles (
	tile geometry NOT NULL,
	count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE borders (
	id BIGINT PRIMARY KEY,
	parent_id BIGINT REFERENCES borders(id),
	name VARCHAR(200),
	geom geometry NOT NULL,
	disabled boolean NOT NULL DEFAULT FALSE,
	count_k INTEGER,
	modified TIMESTAMP NOT NULL,
	cmnt VARCHAR(500)
);
CREATE INDEX borders_idx ON borders USING gist (geom);
CREATE INDEX borders_parent_id_idx ON borders (parent_id);

CREATE TABLE borders_backup (
	backup VARCHAR(30) NOT NULL,
	id BIGINT NOT NULL,
	parent_id BIGINT,
	name VARCHAR(200) NOT NULL,
	geom geometry NOT NULL,
	disabled boolean NOT NULL DEFAULT FALSE,
	count_k INTEGER,
	modified TIMESTAMP NOT NULL,
	cmnt VARCHAR(500),
	PRIMARY KEY (backup, id)
);

CREATE TABLE splitting (
    osm_border_id BIGINT NOT NULL REFERENCES osm_borders(osm_id), -- reference to parent osm region
    id BIGINT NOT NULL, -- representative subregion id
    city_population_thr INT NOT NULL,
    cluster_population_thr INT NOT NULL,
    geom geometry NOT NULL
);
CREATE INDEX splitting_idx ON splitting (osm_border_id, city_population_thr, cluster_population_thr);
