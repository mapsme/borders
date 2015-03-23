create table tiles (
	tile geometry not null,
	count integer not null default 0
);

create table borders (
	name varchar(200) not null primary key,
	geom geometry not null,
	disabled boolean not null default FALSE,
	count_k integer,
	modified timestamp not null,
	cmnt varchar(500)
);

create table borders_backup (
	backup varchar(30) not null,
	name varchar(200) not null,
	geom geometry not null,
	disabled boolean not null default FALSE,
	count_k integer,
	modified timestamp not null,
	cmnt varchar(500),
	primary key (backup, name)
);

create index border_idx on borders using gist (geom);
create index tiles_idx on tiles using gist (tile);
