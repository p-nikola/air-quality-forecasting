import json
import time
import pandas as pd
from kafka import KafkaProducer

if __name__ == "__main__":

    df = pd.read_csv("data/raw/bitola_sensor_weather_features_online.csv")
    context = pd.read_csv("Online-Phase/Context_pm10_bitola.csv")
    valid_ids = context['sensorId'].unique()
    df = df[df['sensorId'].isin(valid_ids)]
    df["timestamp"] = pd.to_datetime(df["timestamp"])


    df = df.sort_values(["timestamp", "sensorId"])

    df["block"] = (
        (df["timestamp"] - df["timestamp"].min())
        .dt.total_seconds() // (24 * 3600)
    )

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        security_protocol="PLAINTEXT"
    )

    for block_id, block_df in df.groupby("block"):

        block_df = block_df.sort_values(["sensorId", "timestamp"])


        counts = block_df.groupby("sensorId").size()

        if not all(counts == 24):
            print(f"Skipping block {block_id} (invalid size)")
            continue

 
        records = []
        for _, row in block_df.iterrows():
            records.append({
                "timestamp": str(row["timestamp"]),
                "sensorId": str(row["sensorId"]),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "humidity": float(row["relative_humidity_2m"]),
                "pressure": float(row["surface_pressure"]),
                "temperature": float(row["temperature_2m"]),
                "wind_speed": float(row["wind_speed_10m"])
            })

        producer.send(
            topic="rawSensorWeatherData",
            value=json.dumps(records).encode("utf-8")
        )

        print(f"Sent block {block_id} with {len(records)} rows")

        # Optional delay between blocks
        time.sleep(5)

    producer.flush()