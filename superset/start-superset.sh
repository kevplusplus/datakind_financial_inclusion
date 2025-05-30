#!/bin/bash
set -e  #exit on any error

echo "Waiting for Spark job to finish..."
until [ -f /shared/ingestion_done.flag ]; do
  sleep 5
done
echo "Spark job finished — continuing Superset startup"

#remove the flag for next run
#TODO need to figure out how to give permissions for this container
# rm -f /shared/ingestion_done.flag

#run superset migrations and initialization
echo "Upgrading Superset DB..."
superset db upgrade

echo "Creating user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin

echo "Initializing Superset..."
superset init

echo "Starting Superset server..."
exec superset run -h 0.0.0.0 -p 8088