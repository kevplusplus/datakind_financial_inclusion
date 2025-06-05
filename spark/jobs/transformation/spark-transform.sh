#!/bin/bash
set -e  #exit on any error

echo "Waiting for Spark job to finish..."
until [ -f /shared/ingestion_done.flag ]; do
  sleep 5
done
echo "Spark ingestion finished — continuing Spark transform job"

spark-submit --master local[*] transform.py

echo "Spark transform finished. Creating completion flag..."
touch /shared/transform_done.flag