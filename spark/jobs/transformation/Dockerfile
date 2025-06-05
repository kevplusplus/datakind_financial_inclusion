FROM pyspark-base:1.0.0

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY transform.py .

COPY spark-transform.sh .

ENTRYPOINT ["/app/spark-transform.sh"]