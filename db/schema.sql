-- Create database (run this only once in pgAdmin query tool)
CREATE DATABASE fall_detection;

-- Connect to the database
\c fall_detection;

-- Users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    gender VARCHAR(10)
);

-- Sensor readings table
CREATE TABLE sensor_readings (
    reading_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    timestamp TIMESTAMP NOT NULL,
    accel_x FLOAT,
    accel_y FLOAT,
    accel_z FLOAT,
    gyro_x FLOAT,
    gyro_y FLOAT,
    gyro_z FLOAT,
    label VARCHAR(50)  -- e.g. fall, walk, sit, etc.
);

-- Model predictions table
CREATE TABLE predictions (
    pred_id SERIAL PRIMARY KEY,
    reading_id INT REFERENCES sensor_readings(reading_id),
    predicted_label VARCHAR(50),
    confidence FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Windows (one row per time window with features)
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
