import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "fall")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

REQUIRED_COLS = [
    "volunteer_id", "activity", "timestamp",
    "accel_x", "accel_y", "accel_z",
    "gyro_x", "gyro_y", "gyro_z"
]

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def load_csv_to_raw(path_csv: str):
    if not os.path.exists(path_csv):
        print(f"[ERROR] CSV not found: {path_csv}")
        sys.exit(1)

    # Read CSV
    df = pd.read_csv(path_csv)

    # Validate required columns
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing columns in CSV: {missing}")
        sys.exit(1)

    # Convert timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    rows = list(df[REQUIRED_COLS].itertuples(index=False, name=None))

    sql = """
        INSERT INTO raw_sensor_data
        (volunteer_id, activity, timestamp,
         accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z)
        VALUES %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

    print(f"[OK] Inserted {len(rows)} rows from {path_csv} into raw_sensor_data")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python services/etl/load_raw.py <path_to_csv>")
        sys.exit(1)

    load_csv_to_raw(sys.argv[1])
