#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER $BUSINESS_USER WITH PASSWORD '$BUSINESS_PASSWORD';
  CREATE DATABASE $BUSINESS_DB;
  GRANT ALL PRIVILEGES ON DATABASE $BUSINESS_DB TO $BUSINESS_USER;
  \connect $BUSINESS_DB $BUSINESS_USER
EOSQL