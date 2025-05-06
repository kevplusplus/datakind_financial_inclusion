FROM bitnami/spark:3.5.1

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY drivers/postgresql-42.7.5.jar /opt/bitnami/spark/jars

COPY data_ingestion.py .

ENTRYPOINT ["spark-submit" , "data_ingestion.py"]