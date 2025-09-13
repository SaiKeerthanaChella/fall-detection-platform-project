WINDOW_SECONDS = 1.0
STRIDE_SECONDS = 0.5
# services/etl/make_windows.py
import os
from datetime import timedelta
from typing import Dict, Iterable, Tuple, Optional

import numpy as np
import pandas as pd

import psycopg2
from psycopg2.extras import Json
from sqlalchemy import create_engine
from dotenv import load_dotenv


# -------------------------
# Config (overridable via .env)
# -------------------------
load_dotenv()

# Database
DB_URL = os.getenv("DB_URL")  # e.g., postgresql+psycopg2://postgres:pass@localhost:5432/fall
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "fall")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Windowing
SAMPLE_RATE_HZ   = float(os.getenv("SAMPLE_RATE_HZ", "18.0"))   # UP-Fall wearable ~18 Hz
WINDOW_SECONDS   = float(os.getenv("WINDOW_SECONDS", "2.56"))    # common HAR window
STRIDE_SECONDS   = float(os.getenv("STRIDE_SECONDS", "0.50"))    # 50%+ overlap good

SENSOR_COLS = ["accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z"]

# Engines / connections
ENGINE = create_engine(DB_URL) if DB_URL else None


def get_conn():
    """psycopg2 connection for inserts (fast, explicit)."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def fetch_raw_df() -> pd.DataFrame:
    """Load raw sensor rows ordered by volunteer & time."""
    sql = """
    SELECT volunteer_id, activity, timestamp,
           accel_x, accel_y, accel_z, gyro_x, gyro_y, gyro_z
    FROM raw_sensor_data
    ORDER BY volunteer_id, timestamp;
    """
    if ENGINE is not None:
        return pd.read_sql(sql, ENGINE, parse_dates=["timestamp"])
    # Fallback if DB_URL not provided (may show pandas warning)
    with get_conn() as conn:
        return pd.read_sql(sql, conn, parse_dates=["timestamp"])


def window_iter(
    df_v: pd.DataFrame,
    win_s: float,
    stride_s: float
) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp, pd.DataFrame]]:
    """Yield (t_start, t_end, chunk_df) for one volunteer, sorted by time."""
    if df_v.empty:
        return
    t_min, t_max = df_v["timestamp"].min(), df_v["timestamp"].max()
    t_start = t_min
    while t_start + timedelta(seconds=win_s) <= t_max:
        t_end = t_start + timedelta(seconds=win_s)
        chunk = df_v[(df_v["timestamp"] >= t_start) & (df_v["timestamp"] < t_end)]
        yield t_start, t_end, chunk
        t_start = t_start + timedelta(seconds=stride_s)


def _stats_for(name: str, arr: np.ndarray, out: Dict[str, float]) -> None:
    out[f"{name}_mean"]   = float(np.mean(arr))
    out[f"{name}_std"]    = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
    out[f"{name}_min"]    = float(np.min(arr))
    out[f"{name}_max"]    = float(np.max(arr))
    out[f"{name}_p25"]    = float(np.percentile(arr, 25))
    out[f"{name}_p50"]    = float(np.percentile(arr, 50))
    out[f"{name}_p75"]    = float(np.percentile(arr, 75))
    out[f"{name}_energy"] = float(np.mean(arr**2))


def _safe_corr(a: np.ndarray, b: np.ndarray) -> float:
    if a.size > 2 and b.size > 2 and np.std(a) > 0 and np.std(b) > 0:
        return float(np.corrcoef(a, b)[0, 1])
    return 0.0


def featureize(chunk: pd.DataFrame) -> Dict[str, float]:
    """Compute robust, simple features for a window."""
    if len(chunk) < 5:
        return {}

    out: Dict[str, float] = {}

    # Magnitudes
    acc_mag = np.sqrt(chunk["accel_x"]**2 + chunk["accel_y"]**2 + chunk["accel_z"]**2)
    gyro_mag = np.sqrt(chunk["gyro_x"]**2 + chunk["gyro_y"]**2 + chunk["gyro_z"]**2)

    series = {
        "accel_x": chunk["accel_x"].to_numpy(float),
        "accel_y": chunk["accel_y"].to_numpy(float),
        "accel_z": chunk["accel_z"].to_numpy(float),
        "gyro_x":  chunk["gyro_x"].to_numpy(float),
        "gyro_y":  chunk["gyro_y"].to_numpy(float),
        "gyro_z":  chunk["gyro_z"].to_numpy(float),
        "acc_mag": acc_mag.to_numpy(float),
        "gyro_mag": gyro_mag.to_numpy(float),
    }

    # Per-signal summary stats
    for name, arr in series.items():
        _stats_for(name, arr, out)

    # Simple cross-axis correlations (accel only, most informative)
    out["corr_accel_x_accel_y"] = _safe_corr(series["accel_x"], series["accel_y"])
    out["corr_accel_x_accel_z"] = _safe_corr(series["accel_x"], series["accel_z"])
    out["corr_accel_y_accel_z"] = _safe_corr(series["accel_y"], series["accel_z"])

    return out


def majority_label(chunk: pd.DataFrame) -> Optional[str]:
    if "activity" not in chunk.columns or chunk.empty:
        return None
    counts = chunk["activity"].value_counts()
    return None if counts.empty else counts.idxmax()


def ensure_windows_table():
    """Create the table if it isn't there (safe to call every run)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS windows (
      window_id SERIAL PRIMARY KEY,
      volunteer_id INT NOT NULL,
      t_start TIMESTAMP NOT NULL,
      t_end   TIMESTAMP NOT NULL,
      label   VARCHAR(50),
      features JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_windows_vol_time
      ON windows(volunteer_id, t_start);
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def main():
    ensure_windows_table()

    df = fetch_raw_df()
    if df.empty:
        print("[WARN] raw_sensor_data is empty. Load CSV first (services/etl/load_raw.py).")
        return

    total = 0
    with get_conn() as conn, conn.cursor() as cur:
        for vid, df_v in df.groupby("volunteer_id"):
            df_v = df_v.sort_values("timestamp")
            for t_start, t_end, chunk in window_iter(df_v, WINDOW_SECONDS, STRIDE_SECONDS):
                feats = featureize(chunk)
                if not feats:
                    continue
                label = majority_label(chunk)
                cur.execute(
                    """
                    INSERT INTO windows (volunteer_id, t_start, t_end, label, features)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (int(vid), t_start.to_pydatetime(), t_end.to_pydatetime(), label, Json(feats))
                )
                total += 1
        conn.commit()

    print(f"[OK] Created {total} windows. "
          f"(win={WINDOW_SECONDS}s, stride={STRIDE_SECONDS}s, sample_rate≈{SAMPLE_RATE_HZ}Hz)")


if __name__ == "__main__":
    main()
