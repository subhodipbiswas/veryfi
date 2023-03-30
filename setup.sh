#!/bin/bash
docker system prune -a
rm -rf ./plugins
rm -rf  ./logs
# Setup airflow env and run the dockercompose
set AIRFLOW_UID=50000
docker compose up --build