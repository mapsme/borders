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
	cmnt VARCHAR(500),
	mwm_size_est REAL
);
CREATE INDEX borders_geom_gits_idx ON borders USING gist (geom);
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
	mwm_size_est REAL,
	PRIMARY KEY (backup, id)
);

CREATE TABLE splitting (
    osm_border_id BIGINT NOT NULL REFERENCES osm_borders(osm_id), -- reference to parent osm region
    subregion_ids BIGINT[] NOT NULL,
    mwm_size_est REAL NOT NULL,
    mwm_size_thr INTEGER NOT NULL, -- mwm size threshold in Kb, 4-bytes INTEGER is enough
    geom geometry NOT NULL
);
CREATE INDEX splitting_idx ON splitting (osm_border_id, mwm_size_thr);
