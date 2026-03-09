import os
import csv
import json
import time
import random
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = "localhost:9092"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CITY = os.getenv("CITY")

DATA_FILE = os.path.join(BASE_DIR, "data/streaming", f"{CITY}_streaming.csv")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def stream_events():

    with open(DATA_FILE) as f:
        reader = csv.DictReader(f)

        for row in reader:

            message = {
                "timestamp": row["timestamp"],
                "sensorId": row["sensorId"],
                "city" : CITY,
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "type": row["topic"],
                "value": float(row["value"])
            }

            producer.send(
                row["topic"],
                key=row["sensorId"],
                value=message
            )

            print(f"Sent {row['topic']} | {message}")

            time.sleep(random.randint(500, 2000) / 1000.0)


def main():

    print("Starting Kafka stream...")
    print(f"Reading dataset: {DATA_FILE}")

    stream_events()


if __name__ == "__main__":
    main()