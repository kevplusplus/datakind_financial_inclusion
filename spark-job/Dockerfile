FROM bitnami/spark:3.5.1

USER root

WORKDIR /app

RUN install_packages postgresql-client && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

#jdbc driver to default bitnami jar folder
COPY drivers/postgresql-42.7.5.jar /opt/bitnami/spark/jars

#copy scripts to container
COPY scripts/ .
COPY spark-entrypoint.sh .

#change permissions if necessary
RUN chmod +x spark-entrypoint.sh

ENTRYPOINT ["/app/spark-entrypoint.sh"]