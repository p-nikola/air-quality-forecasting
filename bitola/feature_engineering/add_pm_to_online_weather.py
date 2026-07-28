from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
PULSE_DATA_DIR = Path(__file__).resolve().parent / "pulse_data"
ONLINE_WEATHER_PATH = BASE_DIR / "data" / "raw" / "bitola_sensor_weather_features_online.csv"
ONLINE_START = pd.Timestamp("2025-12-01 00:00:00", tz="UTC")

POLLUTANTS = ["pm10", "pm25"]


def load_hourly_pollution():
    pulse_files = sorted(PULSE_DATA_DIR.rglob("*.csv"))
    if not pulse_files:
        raise FileNotFoundError(f"No PulseEco CSV files found under {PULSE_DATA_DIR}")

    frames = []
    for path in pulse_files:
        df = pd.read_csv(path, usecols=["timestamp", "sensorId", "type", "value"])
        df = df[df["type"].isin(POLLUTANTS)].copy()
        if df.empty:
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.floor("h")
        df["sensorId"] = df["sensorId"].astype(str)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["timestamp", "sensorId", "type", "value"])
        frames.append(df)

    if not frames:
        raise ValueError(f"No PM10/PM2.5 rows found in {PULSE_DATA_DIR}")

    pollution = pd.concat(frames, ignore_index=True)
    hourly = (
        pollution
        .groupby(["sensorId", "timestamp", "type"], as_index=False)["value"]
        .mean()
        .pivot(index=["sensorId", "timestamp"], columns="type", values="value")
        .reset_index()
    )

    for pollutant in POLLUTANTS:
        if pollutant not in hourly.columns:
            hourly[pollutant] = pd.NA

    return hourly[["sensorId", "timestamp", "pm10", "pm25"]]


def add_pollution_to_online_weather():
    if not ONLINE_WEATHER_PATH.exists():
        raise FileNotFoundError(f"Online weather CSV not found: {ONLINE_WEATHER_PATH}")

    weather = pd.read_csv(ONLINE_WEATHER_PATH)
    original_columns = list(weather.columns)

    weather["timestamp"] = pd.to_datetime(weather["timestamp"], utc=True, errors="coerce").dt.floor("h")
    weather["sensorId"] = weather["sensorId"].astype(str)
    weather = weather[weather["timestamp"] >= ONLINE_START].copy()
    weather = weather.drop(columns=[col for col in POLLUTANTS if col in weather.columns])

    hourly_pollution = load_hourly_pollution()

    merged = weather.merge(
        hourly_pollution,
        on=["sensorId", "timestamp"],
        how="left",
    )

    for pollutant in POLLUTANTS:
        merged[pollutant] = (
            merged
            .sort_values(["sensorId", "timestamp"])
            .groupby("sensorId")[pollutant]
            .transform(lambda values: values.interpolate(method="linear", limit=3, limit_direction="both"))
        )

        hourly_median = merged.groupby("timestamp")[pollutant].transform("median")
        merged[pollutant] = merged[pollutant].fillna(hourly_median)

        sensor_median = merged.groupby("sensorId")[pollutant].transform("median")
        merged[pollutant] = merged[pollutant].fillna(sensor_median)

        global_median = merged[pollutant].median()
        merged[pollutant] = merged[pollutant].fillna(global_median)

    original_without_pollutants = [col for col in original_columns if col not in POLLUTANTS]
    merged = merged[original_without_pollutants + POLLUTANTS]
    merged = merged.sort_values(["sensorId", "timestamp"]).reset_index(drop=True)
    merged["timestamp"] = merged["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

    matched = merged[POLLUTANTS].notna().all(axis=1).sum()
    total = len(merged)
    print(f"Loaded hourly pollution rows: {len(hourly_pollution)}")
    print(f"Merged PM10/PM2.5 into {matched}/{total} online weather rows")
    print(f"PM10 missing rows: {merged['pm10'].isna().sum()}")
    print(f"PM2.5 missing rows: {merged['pm25'].isna().sum()}")

    merged.to_csv(ONLINE_WEATHER_PATH, index=False)
    print(f"Updated {ONLINE_WEATHER_PATH}")


if __name__ == "__main__":
    add_pollution_to_online_weather()
