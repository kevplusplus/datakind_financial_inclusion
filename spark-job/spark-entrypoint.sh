#!/bin/bash
set -e  #exit on any error

#wait for postgres to be ready
echo "Waiting for Postgres to be ready..."
until pg_isready -h "${DB_HOST}" -U "${PG_USER}" -d "${DB_NAME}" -p "${DB_PORT}"; do
  echo "Waiting..."
  sleep 3
done

echo "Postgres is up - continuing"

echo "Postgres is ready. Running Spark job."

spark-submit --master local[*] data_ingestion.py

echo "Spark job finished. Creating completion flag..."
touch /shared/ingestion_done.flag
chown 1000:1000 /shared/ingestion_done.flag
chmod 644 /shared/ingestion_done.flag