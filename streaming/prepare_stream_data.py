import os
import csv
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CITY = os.getenv("CITY")

PULSE_DIR = os.path.join(BASE_DIR, "data/raw/pulse_data", CITY)
WIND_DIR = os.path.join(BASE_DIR, "data/raw/Wind_data", CITY)
OUTPUT_FILE = os.path.join(BASE_DIR, "data/streaming", f"{CITY}_streaming.csv")

VALID_TOPICS = {
    "pm10",
    "pm25",
    "temperature",
    "humidity",
    "pressure",
    "no2",
    "co",
    "o3"
}


def parse_timestamp(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def load_pulse_events():
    events = []

    for root, _, files in os.walk(PULSE_DIR):
        for file in files:
            if not file.endswith(".csv"):
                continue

            path = os.path.join(root, file)

            with open(path) as f:
                reader = csv.DictReader(f)

                for row in reader:
                    if row["type"] not in VALID_TOPICS:
                        continue

                    events.append({
                        "topic": row["type"],
                        "timestamp": parse_timestamp(row["timestamp"]),
                        "sensorId": row["sensorId"],
                        "lat": float(row["lat"]),
                        "lon": float(row["lon"]),
                        "value": float(row["value"])
                    })

    return events


def load_wind_events():
    events = []

    for file in os.listdir(WIND_DIR):
        if not file.endswith(".csv"):
            continue

        path = os.path.join(WIND_DIR, file)

        with open(path) as f:
            reader = csv.DictReader(f)

            for row in reader:
                events.append({
                    "topic": "wind_speed",
                    "timestamp": parse_timestamp(row["timestamp"]),
                    "sensorId": row["sensorId"],
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "value": float(row["value"])
                })

    return events


def main():

    print("Loading Pulse events...")
    pulse_events = load_pulse_events()

    print("Loading Wind events...")
    wind_events = load_wind_events()

    all_events = pulse_events + wind_events

    print(f"Loaded {len(all_events)} events.")

    all_events.sort(key=lambda x: x["timestamp"])

    print("Writing streaming dataset...")

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "sensorId", "lat", "lon", "topic", "value"]
        )

        writer.writeheader()

        for event in all_events:
            writer.writerow({
                "timestamp": event["timestamp"].isoformat(),
                "sensorId": event["sensorId"],
                "lat": event["lat"],
                "lon": event["lon"],
                "topic": event["topic"],
                "value": event["value"]
            })

    print(f"Dataset saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()