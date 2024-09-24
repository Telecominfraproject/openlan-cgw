#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $CGW_DB_USER WITH ENCRYPTED PASSWORD '$CGW_DB_PASSWORD';
    CREATE DATABASE $CGW_DB OWNER $CGW_DB_USER;
    \c $CGW_DB;
    CREATE TABLE infrastructure_groups ( id INT PRIMARY KEY, reserved_size INT, actual_size INT);
    CREATE TABLE infras ( mac MACADDR PRIMARY KEY, infra_group_id INT, FOREIGN KEY(infra_group_id) REFERENCES infrastructure_groups(id) ON DELETE CASCADE);
    ALTER DATABASE $CGW_DB OWNER TO $CGW_DB_USER;
    ALTER TABLE infrastructure_groups OWNER TO $CGW_DB_USER;
    ALTER TABLE infras OWNER TO $CGW_DB_USER;
EOSQL
