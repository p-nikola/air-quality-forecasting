 CREATE TABLE IF NOT EXISTS offline_test_results (
            city TEXT NOT NULL,
            sensor_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            pollutant TEXT NOT NULL,
            actual_value REAL,
            predicted_value REAL,
            model_version TEXT NOT NULL,
            model_type TEXT NOT NULL,
            PRIMARY KEY (city, sensor_id, timestamp, pollutant, model_version)
        );


 CREATE TABLE IF NOT EXISTS online_forecasts (
            city TEXT NOT NULL,
            sensor_id TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            target_at TEXT NOT NULL,
            horizon_hours INTEGER NOT NULL,
            pollutant TEXT NOT NULL,
            predicted_value REAL NOT NULL,
            model_version TEXT,
            PRIMARY KEY (city, sensor_id, issued_at, target_at, pollutant, model_version)
        );


CREATE INDEX IF NOT EXISTS idx_online_forecasts_filters
ON online_forecasts (city, pollutant, model_version, issued_at, target_at);

CREATE INDEX IF NOT EXISTS idx_online_forecasts_latest
ON online_forecasts (city, issued_at);

CREATE INDEX IF NOT EXISTS idx_online_forecasts_timeline
ON online_forecasts (city, issued_at, pollutant, sensor_id, model_version, target_at);

CREATE INDEX IF NOT EXISTS idx_offline_test_results_timeline
ON offline_test_results (city, timestamp, pollutant, sensor_id);


-- streaming measurements data table

CREATE TABLE IF NOT EXISTS online_sensor_weather_features (
    city TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    lat REAL,
    lon REAL,
    temperature_2m REAL,
    relative_humidity_2m REAL,
    wind_speed_10m REAL,
    wind_direction_10m REAL,
    surface_pressure REAL,
    pm10 REAL,
    pm25 REAL,
    PRIMARY KEY (city, sensor_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_online_sensor_weather_features_timeline
ON online_sensor_weather_features (city, timestamp);

CREATE INDEX IF NOT EXISTS idx_online_sensor_weather_features_sensor_timeline
ON online_sensor_weather_features (city, sensor_id, timestamp);


-- raw Pulse Eco measurements for online forecast comparison

CREATE TABLE IF NOT EXISTS online_raw_measurements (
    city TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    timestamp_local TEXT,
    measurement_type TEXT NOT NULL,
    value REAL NOT NULL,
    lat REAL,
    lon REAL,
    source_file TEXT,
    UNIQUE (city, sensor_id, timestamp_utc, measurement_type)
);

CREATE INDEX IF NOT EXISTS idx_online_raw_measurements_timeline
ON online_raw_measurements (city, measurement_type, timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_online_raw_measurements_sensor_timeline
ON online_raw_measurements (city, sensor_id, measurement_type, timestamp_utc);
