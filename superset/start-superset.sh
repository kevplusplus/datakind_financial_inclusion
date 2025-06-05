#!/bin/bash
set -e  #exit on any error

echo "Waiting for Spark job to finish..."
until [ -f /shared/analysis_done.flag ]; do
  sleep 5
done
echo "Spark analysis finished — continuing Superset startup"

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