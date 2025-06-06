FROM apache/superset:3.0.2

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY register_database.py .

#copy entrypoint script and make it executable
COPY start-superset.sh .

RUN chmod +x start-superset.sh

#switch back to superset user
USER superset

ENTRYPOINT ["/app/start-superset.sh"]