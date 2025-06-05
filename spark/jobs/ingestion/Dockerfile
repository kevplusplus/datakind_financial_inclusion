FROM pyspark-base:1.0.0

WORKDIR /app

RUN install_packages postgresql-client && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion.py .

COPY spark-ingestion.sh . 

ENTRYPOINT ["/app/spark-ingestion.sh"]