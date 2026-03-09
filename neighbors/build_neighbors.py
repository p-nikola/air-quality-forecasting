import pandas as pd
import json

K = 3

df = pd.read_csv("../data/neighbors_data/bitola_sensor_distances.csv")

neighbors = {}

for _, row in df.iterrows():

    s = str(row["sensor_id"])
    n = str(row["neighbor_id"])
    d = row["distance_km"]

    neighbors.setdefault(s, []).append((n, d))


neighbors_k = {}

for sensor, items in neighbors.items():

    items_sorted = sorted(items, key=lambda x: x[1])

    neighbors_k[sensor] = [n for n, _ in items_sorted[:K]]


with open("../data/neighbors_data/bitola_neighbors.json", "w") as f:
    json.dump(neighbors_k, f, indent=2)

print("Neighbors saved to ../data/neighbors_data/neighbors.json")