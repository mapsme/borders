#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER borders WITH PASSWORD 'borders';
    CREATE DATABASE gis;
    CREATE DATABASE borders;
    GRANT ALL PRIVILEGES ON DATABASE borders TO borders;
    
    -- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO borders;
EOSQL
