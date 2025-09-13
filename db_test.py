import os
from dotenv import load_dotenv
import psycopg2

# Load the .env file
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

with conn.cursor() as cur:
    cur.execute("SELECT current_database(), current_user;")
    print("✅ Connected:", cur.fetchone())

conn.close()
