import argparse
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CSV_PATH = BASE_DIR / "data" / "streaming" / "bitola_sensor_weather_features_online.csv"
DEFAULT_DB_PATH = BASE_DIR / "data" / "project.db"
DEFAULT_CITY = "Bitola"

REQUIRED_COLUMNS = {
    "timestamp",
    "sensorId",
    "lat",
    "lon",
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
    "pm10",
    "pm25",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import Bitola online sensor/weather feature rows into the project SQLite database."
    )
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CSV_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--city", default=DEFAULT_CITY)
    return parser.parse_args()


def normalize_timestamp(value):
    value = (value or "").strip()
    if not value:
        return None

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)

    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def optional_float(value):
    value = (value or "").strip()
    if not value:
        return None
    return float(value)


def ensure_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS online_sensor_weather_features (
            city TEXT NOT NULL,
            sensor_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            lat REAL,
            lon REAL,
            temperature_2m REAL,
            relative_humidity_2m REAL,
            wind_speed_10m REAL,
            wind_direction_10m REAL,
            surface_pressure REAL,
            pm10 REAL,
            pm25 REAL,
            PRIMARY KEY (city, sensor_id, timestamp)
        )
        """
    )

def row_to_record(row, city):
    sensor_id = (row.get("sensorId") or "").strip()
    timestamp = normalize_timestamp(row.get("timestamp"))
    if not sensor_id or not timestamp:
        return None

    return (
        city,
        sensor_id,
        timestamp,
        optional_float(row.get("lat")),
        optional_float(row.get("lon")),
        optional_float(row.get("temperature_2m")),
        optional_float(row.get("relative_humidity_2m")),
        optional_float(row.get("wind_speed_10m")),
        optional_float(row.get("wind_direction_10m")),
        optional_float(row.get("surface_pressure")),
        optional_float(row.get("pm10")),
        optional_float(row.get("pm25")),
    )


def load_records(csv_path, city):
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header row: {csv_path}")

        missing_columns = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"CSV is missing required columns: {missing}")

        records = []
        skipped = 0
        for row in reader:
            record = row_to_record(row, city)
            if record is None:
                skipped += 1
                continue
            records.append(record)

    return records, skipped


def insert_records(conn, records):
    conn.executemany(
        """
        INSERT OR REPLACE INTO online_sensor_weather_features (
            city,
            sensor_id,
            timestamp,
            lat,
            lon,
            temperature_2m,
            relative_humidity_2m,
            wind_speed_10m,
            wind_direction_10m,
            surface_pressure,
            pm10,
            pm25
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        records,
    )


def main():
    args = parse_args()
    csv_path = args.csv_path.expanduser().resolve()
    db_path = args.db_path.expanduser().resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    records, skipped = load_records(csv_path, args.city)

    with sqlite3.connect(db_path) as conn:
        ensure_table(conn)
        insert_records(conn, records)
        conn.commit()

    print(f"Inserted {len(records)} online sensor/weather rows into {db_path}")
    if skipped:
        print(f"Skipped {skipped} malformed rows")


if __name__ == "__main__":
    main()
