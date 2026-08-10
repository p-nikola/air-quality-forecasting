import json
import time
import pandas as pd
from kafka import KafkaProducer
import os
from pathlib import Path


def nullable_float(value):
    return None if pd.isna(value) else float(value)


def optional_float(row, column):
    if column not in row:
        return None
    return nullable_float(row[column])

if __name__ == "__main__":

    base_dir = Path(__file__).resolve().parents[2]
    skopje_dir = Path(os.getenv("SKOPJE_PROJECT_DIR", str(base_dir / "skopje"))).expanduser()
    default_data_path = skopje_dir / "data" / "streaming" / "skopje_sensor_weather_features_online_short_gap_interpolated.csv"
    data_path = Path(os.getenv("SKOPJE_DATA_CSV_PATH", str(default_data_path))).expanduser()
    topic_prefix = os.getenv("SKOPJE_SENSOR_TOPIC_PREFIX", "skopje_sensor_")

    if not data_path.exists():
        raise FileNotFoundError(f"Input CSV not found at: {data_path}")

    df = pd.read_csv(data_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.sort_values(["timestamp", "sensorId"])

    producer = KafkaProducer(
        bootstrap_servers= os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        security_protocol="PLAINTEXT"
    )

    for timestamp, ts_df in df.groupby("timestamp"):

        for _, row in ts_df.iterrows():

            record = {
                "timestamp": str(row["timestamp"]),
                "sensorId": str(row["sensorId"]),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "humidity": float(row["relative_humidity_2m"]),
                "pressure": float(row["surface_pressure"]),
                "temperature": float(row["temperature_2m"]),
                "wind_speed": float(row["wind_speed_10m"]),
                "pm10": optional_float(row, "pm10"),
                "pm25": optional_float(row, "pm25"),
            }

            topic = f"{topic_prefix}{row['sensorId']}"

            producer.send(
                topic=topic,
                value=json.dumps(record).encode("utf-8")
            )

            print(f"Sent record to {topic} at {timestamp}")

        time.sleep(1)

    producer.flush()
