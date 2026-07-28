import json
import os
import sqlite3
from pathlib import Path
import uuid

import pandas as pd
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("BITOLA_FORECAST_DB_GROUP_ID", f"bitola_forecast_db_consumer_cacko")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = Path(os.getenv("PROJECT_DB_PATH", str(BASE_DIR / "data" / "project.db"))).expanduser()
CITY = "Bitola"
BATCH_SIZE = int(os.getenv("BITOLA_DB_CONSUMER_BATCH_SIZE", "1000"))

TOPIC_METADATA = {
    "FullPm10WeatherData": ("pm10", "chronos2_pm10_bitola_fine_tuned_24h"),
    "FullPm25WeatherData": ("pm25", "chronos2_pm25_bitola_fine_tuned_24h"),
    "FullPm10WeatherData_ZeroShot": ("pm10", "chronos2_pm10_bitola_zero_shot_24h"),
    "FullPm25WeatherData_ZeroShot": ("pm25", "chronos2_pm25_bitola_zero_shot_24h"),
}


def normalize_timestamp(value):
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.tz_convert(None).strftime("%Y-%m-%d %H:%M:%S")


def ensure_online_forecasts_table(conn):
    conn.execute(
        """
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
        )
        """
    )


def forecast_record(topic, payload):
    if topic not in TOPIC_METADATA:
        return None

    pollutant, model_version = TOPIC_METADATA[topic]
    predicted_value = pd.to_numeric(payload.get(pollutant), errors="coerce")
    issued_at = normalize_timestamp(payload.get("forecast_origin"))
    target_at = normalize_timestamp(payload.get("timestamp"))
    horizon_hours = pd.to_numeric(payload.get("horizon_hours"), errors="coerce")

    if (
        predicted_value is None
        or pd.isna(predicted_value)
        or issued_at is None
        or target_at is None
        or horizon_hours is None
        or pd.isna(horizon_hours)
        or payload.get("sensorId") is None
    ):
        return None

    return (
        CITY,
        str(payload["sensorId"]),
        issued_at,
        target_at,
        int(horizon_hours),
        pollutant,
        float(predicted_value),
        model_version,
    )


def save_records(conn, records):
    if not records:
        return 0

    conn.executemany(
        """
        INSERT OR REPLACE INTO online_forecasts (
            city, sensor_id, issued_at, target_at, horizon_hours, pollutant, predicted_value, model_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        records,
    )
    return len(records)


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    topics = list(TOPIC_METADATA)

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    print(f"Consuming Bitola forecast topics: {', '.join(topics)}")
    print(f"Writing forecasts to {DB_PATH}")

    buffer = []
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        ensure_online_forecasts_table(conn)

        try:
            for message in consumer:
                record = forecast_record(message.topic, message.value)
                if record is not None:
                    buffer.append(record)

                if len(buffer) >= BATCH_SIZE:
                    try:
                        saved = save_records(conn, buffer)
                        conn.commit()
                        consumer.commit()
                        print(f"Saved {saved} online forecast rows at timestamp {pd.Timestamp.now()}")
                        buffer.clear()
                    except Exception:
                        conn.rollback()
                        buffer.clear()
                        raise
        finally:
            if buffer:
                saved = save_records(conn, buffer)
                conn.commit()
                consumer.commit()
                print(f"Saved {saved} online forecast rows")
            consumer.close()


if __name__ == "__main__":
    main()
