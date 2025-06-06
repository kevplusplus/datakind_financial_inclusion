#!/bin/bash
set -e  # Stop if any job fails

echo "Cleaning old flags..."
rm -f ./shared-data/ingestion_done.flag
rm -f ./shared-data/transform_done.flag
rm -f ./shared-data/analysis_done.flag

echo "Starting ingestion job..."
docker-compose run --rm ingestion-job

sleep 10 #TODO need to find permanent fix (job will run too fast before postgres updates rows)

echo "Starting transformation job..."
docker-compose run --rm transform-job

sleep 10

echo "Starting analysis job..."
docker-compose run --rm analysis-job

sleep 10

echo "Launching Superset UI..."
docker-compose up superset