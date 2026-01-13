FROM apache/superset:3.0.2

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY register_database.py .

#copy entrypoint script and make it executable
USER root

COPY start-superset.sh .

RUN chmod +x start-superset.sh

USER superset
#switch back to superset user

ENTRYPOINT ["/app/start-superset.sh"]