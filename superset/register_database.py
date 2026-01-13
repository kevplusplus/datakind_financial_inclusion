import os
from superset import db
from superset.app import create_app
from superset.models.core import Database


app = create_app()

# --- Configuration ---
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

SQLALCHEMY_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_DISPLAY_NAME = "My Postgres DB"

# --- End configuration ---

with app.app_context():
    #check if DB already exists
    existing = db.session.query(Database).filter_by(database_name=DB_DISPLAY_NAME).first()
    if existing:
        print(f"Database '{DB_DISPLAY_NAME}' already exists. Skipping creation.")
    else:
        new_db = Database(
            database_name=DB_DISPLAY_NAME,
            sqlalchemy_uri=SQLALCHEMY_URI,
            extra='{"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}, "schemas_allowed_for_csv_upload": []}'
        )
        db.session.add(new_db)
        db.session.commit()
        print(f"Database '{DB_DISPLAY_NAME}' registered successfully.")