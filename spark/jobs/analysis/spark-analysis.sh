#!/bin/bash
set -e  #exit on any error

echo "Waiting for Spark job to finish..."
until [ -f /shared/transform_done.flag ]; do
  sleep 5
done
echo "Spark transform finished — continuing Spark analysis job"

spark-submit --master local[*] analysis.py

echo "Spark analysis finished. Creating completion flag..."
touch /shared/analysis_done.flag