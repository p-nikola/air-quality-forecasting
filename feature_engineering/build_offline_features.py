import os
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_FILE = os.path.join(BASE_DIR, "data/streaming/bitola_streaming.csv")
NEIGHBORS_FILE = os.path.join(BASE_DIR, "data/neighbors_data/neighbors.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "data/training/bitola_features.csv")

NEIGHBOR_FEATURES = ["pm10", "pm25", "wind_speed"]


def load_data():
    print("Loading dataset...")
    df = pd.read_csv(INPUT_FILE)

    if "city" not in df.columns:
        df["city"] = "bitola"

    return df


def pivot_measurements(df):
    print("Pivoting measurements...")

    df = df.pivot_table(
        index=["sensorId", "timestamp", "city"],
        columns="topic",
        values="value",
        aggfunc="first"
    ).reset_index()

    return df


def load_neighbors():
    print("Loading neighbors...")

    with open(NEIGHBORS_FILE) as f:
        neighbors = json.load(f)

    neighbors = {str(k): [str(n) for n in v] for k, v in neighbors.items()}
    return neighbors


def add_neighbor_features(df, neighbors):
    print("Adding neighbor features...")

    sensor_features = df[["sensorId", "timestamp"] + NEIGHBOR_FEATURES]

    for i in range(3):
        df[f"neighbor_{i+1}_pm10"] = None
        df[f"neighbor_{i+1}_pm25"] = None
        df[f"neighbor_{i+1}_wind_speed"] = None

    for sensor, neighs in neighbors.items():

        for i, neigh in enumerate(neighs[:3]):

            merged = df.merge(
                sensor_features[sensor_features["sensorId"] == neigh],
                on="timestamp",
                how="left",
                suffixes=("", "_neighbor")
            )

            mask = df["sensorId"].astype(str) == str(sensor)

            df.loc[mask, f"neighbor_{i+1}_pm10"] = merged.loc[mask, "pm10_neighbor"]
            df.loc[mask, f"neighbor_{i+1}_pm25"] = merged.loc[mask, "pm25_neighbor"]
            df.loc[mask, f"neighbor_{i+1}_wind_speed"] = merged.loc[mask, "wind_speed_neighbor"]

    return df


def main():
    df = load_data()

    df = pivot_measurements(df)

    neighbors = load_neighbors()

    df = add_neighbor_features(df, neighbors)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    print("Saving dataset...")
    df.to_csv(OUTPUT_FILE, index=False)

    print("Dataset saved to:", OUTPUT_FILE)


if __name__ == "__main__":
    main()