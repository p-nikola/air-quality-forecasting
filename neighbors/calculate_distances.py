from dotenv import load_dotenv
import os
import requests
from requests.auth import HTTPBasicAuth
from haversine import haversine
import csv


load_dotenv()

CITY = os.getenv("CITY")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
BASE_URL = f"https://{CITY}.pulse.eco/rest"


def get_sensors():
    url = f"{BASE_URL}/sensor"
    r = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD),timeout=10)
    if r.status_code != 200:
        raise Exception("Failed to fetch sensors:", r.text)
    return r.json()


valid_statuses = {
    "ACTIVE",
    "ACTIVE_UNCONFIRMED",
    "NOT_CLAIMED",
    "NOT_CLAIMED_UNCONFIRMED"
}

sensors = get_sensors()
filtered_sensors = [sensor for sensor in sensors if sensor["status"] in valid_statuses] 

sensors_dict ={}
for sensor in filtered_sensors:
    sensor_id = sensor["sensorId"]
    lat,lon = sensor["position"].split(",")
   
    sensors_dict[sensor_id] = (
        float(lat),
        float(lon)  
    )


csv_file = f"{CITY}_neighbors.csv"

if not os.path.exists("neighbors_data"):
    os.makedirs("neighbors_data")

csv_path = os.path.join("neighbors_data", csv_file) 

sensor_ids = list(sensors_dict.keys())

with open(csv_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["sensor_id", "neighbor_id", "distance_km"])  
    
    for i in range(len(sensor_ids)):
        for j in range(i + 1, len(sensor_ids)):
            sensor_id_1 = sensor_ids[i]
            sensor_id_2 = sensor_ids[j]

            coord_1 = sensors_dict[sensor_id_1]
            coord_2 = sensors_dict[sensor_id_2]

            distance = haversine(coord_1, coord_2)

            print(f"Distance between {sensor_id_1} and {sensor_id_2}: {distance:.2f} km")

            writer.writerow([sensor_id_1, sensor_id_2, f"{distance:.6f}"])
            writer.writerow([sensor_id_2, sensor_id_1, f"{distance:.6f}"])
            
            
       