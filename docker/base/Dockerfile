FROM bitnami/spark:3.5.1

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

#jdbc driver to default bitnami jar folder
COPY drivers/postgresql-42.7.5.jar /opt/bitnami/spark/jars