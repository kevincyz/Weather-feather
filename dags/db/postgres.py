import psycopg2
import os, json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
cursor = conn.cursor()

def store_metadata(user_id, data_type, location, date_range, status):
    cursor.execute("""
        INSERT INTO metadata (user_id, data_type, location, date_range, accessed_at, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user_id, data_type, location, json.dumps(date_range), datetime.utcnow(), status))
    conn.commit()

def store_postgres_data(data, data_type):
    cursor.execute("""
        INSERT INTO data (data_type, payload, created_at)
        VALUES (%s, %s, %s)
    """, (data_type, json.dumps(data), datetime.utcnow()))
    conn.commit()
