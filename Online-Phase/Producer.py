import json
import time
import pandas as pd
from kafka import KafkaProducer

if __name__ == "__main__":

    df = pd.read_csv("data/raw/bitola_sensor_weather_features_online.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.sort_values(["timestamp", "sensorId"])

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
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
                "wind_speed": float(row["wind_speed_10m"])
            }

            topic = f"sensor_{row['sensorId']}"

            producer.send(
                topic=topic,
                value=json.dumps(record).encode("utf-8")
            )

            print(f"Sent record to {topic} at {timestamp}")

        time.sleep(1)

    producer.flush()