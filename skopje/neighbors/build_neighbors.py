import pandas as pd
import json
import os
K = 5

base_dir = os.path.dirname(os.path.abspath(__file__))

csv_path = os.path.join(base_dir, "..", "data", "neighbors_data", "skopje_neighbors.csv")

df = pd.read_csv(csv_path)

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



json_path = os.path.join(base_dir, "..", "data", "neighbors_data", "skopje_neighbors.json")
with open(json_path, "w") as f:
    json.dump(neighbors_k, f, indent=2)

print("Neighbors saved to ../data/neighbors_data/neighbors.json")