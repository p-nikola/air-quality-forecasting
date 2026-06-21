CREATE TABLE offline_test_results (
    city TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    pollutant TEXT NOT NULL,
    actual_value REAL NOT NULL,
    predicted_value REAL NOT NULL,
    model_version TEXT,
    PRIMARY KEY (city, sensor_id, timestamp, pollutant)
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
