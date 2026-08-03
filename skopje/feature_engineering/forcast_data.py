import pandas as pd
import requests
import os
import pandas as pd
# Load existing dataset

base_dir = os.path.dirname(os.path.abspath(__file__))

csv_path = os.path.join(base_dir, "..", "data", "streaming", "skopje_sensor_weather_features_online.csv")
output_path = os.path.join(base_dir,"..","data","raw","skopje_forecast_weather.csv")
existing_df = pd.read_csv(csv_path)
existing_df["timestamp"] = pd.to_datetime(existing_df["timestamp"], utc=True)

sensors_geo = existing_df[["sensorId", "lat", "lon"]].drop_duplicates().copy()
sensors_geo["lat_r"] = sensors_geo["lat"].round(3)
sensors_geo["lon_r"] = sensors_geo["lon"].round(3)

unique_coords = sensors_geo[["lat_r", "lon_r"]].drop_duplicates().reset_index(drop=True)

# 3. Format Array of Latitudes/Longitudes into Comma-Separated Strings
lats_param = ",".join(unique_coords["lat_r"].astype(str))
lons_param = ",".join(unique_coords["lon_r"].astype(str))

# 4. Construct Request Parameters
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
params = {
    "latitude": lats_param,
    "longitude": lons_param,
    "start_hour": "2025-11-09T16:00",
    "end_hour": "2025-11-30T22:00",
    "hourly": ["temperature_2m", "relative_humidity_2m", "surface_pressure", "wind_speed_10m"],
    "timezone": "UTC"
}

# 5. Send ONE Single Multi-Location API Call
print(f"Sending 1 single API request covering {len(unique_coords)} unique grid points...")
response = requests.get(url, params=params, timeout=30)
response.raise_for_status()

data = response.json()

# Handle case where only 1 unique coordinate exists (returns dict instead of list)
if isinstance(data, dict):
    data = [data]

# 6. Parse Multi-Location Response Array
fetched_chunks = []
for idx, loc_response in enumerate(data):
    coord_row = unique_coords.iloc[idx]
    hourly = loc_response["hourly"]
    
    chunk_df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly["time"], utc=True),
        "temperature_2m": hourly["temperature_2m"],
        "relative_humidity_2m": hourly["relative_humidity_2m"],
        "surface_pressure": hourly["surface_pressure"],
        "wind_speed_10m": hourly["wind_speed_10m"],
        "lat_r": coord_row["lat_r"],
        "lon_r": coord_row["lon_r"]
    })
    fetched_chunks.append(chunk_df)

fetched_df = pd.concat(fetched_chunks, ignore_index=True)

# 7. Fast Vectorized Join Back to original sensorIds
# Maps weather data to all sensor IDs sharing the same rounded coordinates
merged_new_df = pd.merge(sensors_geo, fetched_df, on=["lat_r", "lon_r"]).drop(columns=["lat_r", "lon_r"])

# 8. Append to Base Dataset, Deduplicate, and Output
combined_df = pd.concat([existing_df, merged_new_df], ignore_index=True)
combined_df.drop_duplicates(subset=["timestamp", "sensorId"], inplace=True)
combined_df.sort_values(by=["sensorId", "timestamp"], inplace=True)
combined_df.reset_index(drop=True, inplace=True)

# 9. Save output
combined_df.to_csv(output_path, index=False)
print(f"Done! Extended dataset saved to {output_path} ({len(combined_df)} total rows).")