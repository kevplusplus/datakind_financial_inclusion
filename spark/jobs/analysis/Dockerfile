FROM pyspark-base:1.0.0

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY analysis.py .

COPY spark-analysis.sh . 

ENTRYPOINT ["/app/spark-analysis.sh"]