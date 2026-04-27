import joblib
import os
from pathlib import Path

import findspark
import numpy as np
import pandas as pd
import pyspark as spark
import torch
from chronos import Chronos2Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, from_json, struct, to_json, to_timestamp
from pyspark.sql.types import FloatType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
BASE_DIR = Path(__file__).resolve().parents[2]
OFFLINE_DIR = BASE_DIR / "offline-Phase"
DEFAULT_CONTEXT_PATH = Path(__file__).resolve().parent / "context_bitola.csv"
CONTEXT_PATH = Path(os.getenv("BITOLA_CONTEXT_CSV_PATH", str(DEFAULT_CONTEXT_PATH))).expanduser()
ZERO_SHOT_DIR = Path(
    os.getenv("BITOLA_ZERO_SHOT_MODEL_PATH", str(OFFLINE_DIR / "chronos2_zero_shot"))
).expanduser()

PM10_TOPIC = "FullPm10WeatherData"
PM25_TOPIC = "FullPm25WeatherData"
PM10_ZERO_SHOT_TOPIC = "FullPm10WeatherData_ZeroShot"
PM25_ZERO_SHOT_TOPIC = "FullPm25WeatherData_ZeroShot"

if not CONTEXT_PATH.exists():
    raise FileNotFoundError(f"Context CSV not found at: {CONTEXT_PATH}")

pipeline_pm10 = Chronos2Pipeline.from_pretrained(
    str(OFFLINE_DIR / "bitola_chronos_pipeline_pm10"),
    local_files_only=True,
)
pipeline_pm25 = Chronos2Pipeline.from_pretrained(
    str(OFFLINE_DIR / "bitola_chronos_pipeline_pm25"),
    local_files_only=True,
)

if ZERO_SHOT_DIR.exists():
    chronos2_pipeline = Chronos2Pipeline.from_pretrained(
        str(ZERO_SHOT_DIR),
        local_files_only=True,
    )
    print(f"Loaded zero-shot model from {ZERO_SHOT_DIR.resolve()}")
else:
    chronos2_pipeline = Chronos2Pipeline.from_pretrained(
        "amazon/chronos-2",
        device_map="auto",
        dtype=torch.bfloat16,
    )
    print(f"Loaded zero-shot model from {chronos2_pipeline}")

feature_scaler = joblib.load(OFFLINE_DIR / "feature_scaler.pkl")
pm10_scaler_obj = joblib.load(OFFLINE_DIR / "pm10_scaler.pkl")
pm25_scaler_obj = joblib.load(OFFLINE_DIR / "pm25_scaler.pkl")


def extract_time_features(df, timestamp_col="timestamp"):
    df["hour_sin"] = np.sin(2 * np.pi * df[timestamp_col].dt.hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df[timestamp_col].dt.hour / 24)

    df["month_sin"] = np.sin(2 * np.pi * (df[timestamp_col].dt.month - 1) / 12)
    df["month_cos"] = np.cos(2 * np.pi * (df[timestamp_col].dt.month - 1) / 12)

    df["day_sin"] = np.sin(2 * np.pi * df[timestamp_col].dt.dayofweek / 7)
    df["day_cos"] = np.cos(2 * np.pi * df[timestamp_col].dt.dayofweek / 7)

    df["is_weekend"] = df[timestamp_col].dt.dayofweek.isin([5, 6]).astype(int)
    df["is_heating_season"] = df[timestamp_col].dt.month.isin([11, 12, 1, 2, 3]).astype(int)
    return df


def append_neighbors(df_hourly, neighbors_df, weather_cols=["humidity", "pressure","temperature", "wind_speed"], k_search=20, k_keep=3):
    # 1. Standardize the neighbor list
    # Ensure we only take the top K based on distance
    neighbors_topk = (
        neighbors_df.sort_values(["sensor_id", "distance_km"])
        .groupby("sensor_id")
        .head(k_search)
        .copy()
    )

    # Track original distance rank
    neighbors_topk['dist_rank'] = neighbors_topk.groupby("sensor_id").cumcount() + 1

    # 2. Merge with main data
    # We use 'neighbor_id' from the matrix to match 'sensorId' in the hourly data
    neighbor_values = neighbors_topk.merge(
        df_hourly[['sensorId', 'timestamp'] + weather_cols],
        left_on='neighbor_id',
        right_on='sensorId',
        how='inner'
    )

    # 3. Filter for availability
    # The 'sensor_id' here is the ORIGINAL sensor we are finding neighbors for
    available_topk = (
        neighbor_values.sort_values(['sensor_id', 'timestamp', 'dist_rank'])
        .groupby(['sensor_id', 'timestamp'])
        .head(k_keep)
        .copy()
    )

    # Create the 1, 2, 3 rank for the wide-format columns
    available_topk['final_rank'] = available_topk.groupby(['sensor_id', 'timestamp']).cumcount() + 1

    # 4. Pivot to wide format
    pivot_df = available_topk.pivot(
        index=['sensor_id', 'timestamp'],
        columns='final_rank',
        values=weather_cols
    )

    # Clean up column names: neighbor1_temp, neighbor2_temp, etc.
    if isinstance(pivot_df.columns, pd.MultiIndex):
        pivot_df.columns = [f"neighbor{rank}_{col}" for col, rank in pivot_df.columns]
    else:
        # Handle case with only one weather column
        pivot_df.columns = [f"neighbor{i}_{weather_cols[0]}" for i in pivot_df.columns]

    pivot_df = pivot_df.reset_index()

    # 5. Final Join back to original data
    df_result = df_hourly.merge(
        pivot_df,
        left_on=['sensorId', 'timestamp'],
        right_on=['sensor_id', 'timestamp'],
        how='left'
    ).drop(columns=['sensor_id'])

    return df_result


NEIGHBORS_PATH = BASE_DIR / "data" / "neighbors_data" / "bitola_sensor_distances.csv"
print("Neighbors path:", NEIGHBORS_PATH)
print("Exists:", NEIGHBORS_PATH.exists())
neighbourhood_matrix = pd.read_csv(NEIGHBORS_PATH)


def load_context():
    context_df = pd.read_csv(CONTEXT_PATH)
    context_df = context_df.drop(columns=["city"])
    context_df["timestamp"] = pd.to_datetime(context_df["timestamp"])
    return context_df


def predict_target_fine_tuned(pdf, context_df, target, scaler, pipeline, id_col, time_col, numeric_features):
    context_target = context_df.copy()
    context_target[target] = scaler.transform(context_target[[target]])

    forecast_df = pipeline.predict_df(
        df=context_target,
        prediction_length=1,
        target=target,
        id_column=id_col,
        future_df=pdf,
        validate_inputs=False,
    )

    result_df = pdf.merge(
        forecast_df[[id_col, time_col, "predictions"]],
        on=[id_col, time_col],
        how="left",
    )
    result_df[numeric_features] = feature_scaler.inverse_transform(result_df[numeric_features])
    result_df["predictions"] = scaler.inverse_transform(result_df[["predictions"]])
    return result_df.rename(columns={"predictions": target})


def predict_target_zero_shot(pdf, context_df, target, pipeline, id_col, time_col):
    forecast_df = pipeline.predict_df(
        df=context_df,
        prediction_length=1,
        target=target,
        id_column=id_col,
        future_df=pdf,
        validate_inputs=False,
    )

    result_df = pdf.merge(
        forecast_df[[id_col, time_col, "predictions"]],
        on=[id_col, time_col],
        how="left",
    )
    return result_df.rename(columns={"predictions": target})


def process_batch(pdf, context_df):
    id_col = "sensorId"
    time_col = "timestamp"
    numeric_features = [
        'humidity', 'pressure', 'temperature', 'wind_speed',
        'neighbor1_humidity', 'neighbor2_humidity', 'neighbor3_humidity',
        'neighbor1_pressure', 'neighbor2_pressure', 'neighbor3_pressure',
        'neighbor1_temperature', 'neighbor2_temperature', 'neighbor3_temperature',
        'neighbor1_wind_speed', 'neighbor2_wind_speed', 'neighbor3_wind_speed'
    ]

    pdf[time_col] = pd.to_datetime(pdf[time_col], utc=True)
    context_df[time_col] = pd.to_datetime(context_df[time_col], utc=True)

    # go pravime ova za da osigurame deka i pdf i context_df imaat site potrebni koloni, ako ne, da gi dodademe so NaN vrednosti
    # za da ne crashne modelot posle

    for feature in numeric_features:
        if feature not in pdf.columns:
            pdf[feature] = np.nan
        if feature not in context_df.columns:
            context_df[feature] = np.nan

    context_sorted = context_df.copy().sort_values([id_col, time_col])
    print("Context max timestamp:", context_sorted[time_col].max())

    fine_tuned_pdf = pdf.copy()
    fine_tuned_context = context_sorted.copy()
    fine_tuned_pdf[numeric_features] = feature_scaler.transform(fine_tuned_pdf[numeric_features])
    fine_tuned_context[numeric_features] = feature_scaler.transform(fine_tuned_context[numeric_features])

    pm10_df = predict_target_fine_tuned(
        fine_tuned_pdf,
        fine_tuned_context,
        target="pm10",
        scaler=pm10_scaler_obj,
        pipeline=pipeline_pm10,
        id_col=id_col,
        time_col=time_col,
        numeric_features=numeric_features,
    )
    pm25_df = predict_target_fine_tuned(
        fine_tuned_pdf,
        fine_tuned_context,
        target="pm25",
        scaler=pm25_scaler_obj,
        pipeline=pipeline_pm25,
        id_col=id_col,
        time_col=time_col,
        numeric_features=numeric_features,
    )

    zero_shot_pdf = pdf.copy()
    pm10_zero_shot_df = predict_target_zero_shot(
        zero_shot_pdf.copy(),
        context_sorted,
        target="pm10",
        pipeline=chronos2_pipeline,
        id_col=id_col,
        time_col=time_col,
    )
    pm25_zero_shot_df = predict_target_zero_shot(
        zero_shot_pdf.copy(),
        context_sorted,
        target="pm25",
        pipeline=chronos2_pipeline,
        id_col=id_col,
        time_col=time_col,
    )

    return pm10_df, pm25_df, pm10_zero_shot_df, pm25_zero_shot_df


def write_to_kafka(df, topic):
    spark_df = spark.createDataFrame(df)

    kafka_df = spark_df.select(
        col("sensorId").cast("string").alias("key"),
        to_json(struct(*spark_df.columns)).alias("value"),
    )

    kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", topic) \
    .save() 


context_df = load_context()


def foreach_batch(batch_df, epoch_id):
    global context_df

    print(f"\nBatch received! Epoch: {epoch_id}")

    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    if pdf.empty:
        print("Empty batch - skipping")
        return

    pdf = pdf.sort_values("timestamp")

    for _, row in pdf.iterrows():
        ts = row["timestamp"]
        ts_df = pd.DataFrame([r.asDict() for r in row["rows"]])

        print(f"\nProcessing timestamp: {ts}")
        print(f"Sensor count: {len(ts_df)}")

        context_max_ts = pd.to_datetime(context_df["timestamp"], utc=True).max() if not context_df.empty else None
        if context_max_ts is not None and pd.to_datetime(ts, utc=True) <= context_max_ts:
            print(f"Skipping timestamp {ts} because it is not after context max {context_max_ts}")
            continue

        incoming_ids = set(ts_df["sensorId"].unique())
        existing_ids = set(context_df["sensorId"].unique()) if not context_df.empty else set()
        new_ids = incoming_ids - existing_ids

        if new_ids:
            print(f"New sensors detected: {new_ids}")

            numeric_columns = [
                column
                for column in context_df.columns
                if column in ["temperature", "wind_speed", "humidity", "pm10", "pm25", "pressure"]
            ]
            city_baseline = context_df.groupby("timestamp")[numeric_columns].median().reset_index()

            proxy_rows = []
            for sensor_id in new_ids:
                proxy_history = city_baseline.copy()
                proxy_history["sensorId"] = sensor_id

                temp_combined = pd.concat([context_df, proxy_history], ignore_index=True)
                refined_data = append_neighbors(temp_combined, neighbourhood_matrix)
                refined_data = extract_time_features(refined_data)

                proxy_rows.append(refined_data[refined_data["sensorId"] == sensor_id])

            context_df = pd.concat([context_df, *proxy_rows], ignore_index=True)

        ts_df["timestamp"] = pd.to_datetime(ts_df["timestamp"], utc=True)
        ts_df = extract_time_features(ts_df)
        ts_df = append_neighbors(ts_df, neighbourhood_matrix)

        pm10_df, pm25_df, pm10_zero_shot_df, pm25_zero_shot_df = process_batch(ts_df.copy(), context_df)

        write_to_kafka(pm10_df, PM10_TOPIC)
        write_to_kafka(pm25_df, PM25_TOPIC)

        print("Writing to kafka for zero-shot predictions...")
        write_to_kafka(pm10_zero_shot_df, PM10_ZERO_SHOT_TOPIC)
        write_to_kafka(pm25_zero_shot_df, PM25_ZERO_SHOT_TOPIC)

        context_df = pd.concat([context_df, ts_df], ignore_index=True)
        context_df = (
            context_df.sort_values(["sensorId", "timestamp"])
            .drop_duplicates(subset=["sensorId", "timestamp"], keep="last")
            .groupby("sensorId")
            .tail(72)
            .reset_index(drop=True)
        )

        print(f"Prediction done for {ts}")
        print(f"Context size: {len(context_df)}")


findspark.init()

spark = (
    SparkSession.builder.appName("KafkaConsumerExample")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
    .getOrCreate()
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("subscribePattern", "sensor_.*")
    .load()
)

df.printSchema()

schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("sensorId", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("pressure", FloatType(), True),
        StructField("temperature", FloatType(), True),
        StructField("wind_speed", FloatType(), True),
    ]
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .drop("lat", "lon")
)
parsed_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))
parsed_df.printSchema()

grouped_df = (
    parsed_df.withWatermark("timestamp", "5 minutes")
    .groupBy("timestamp")
    .agg(collect_list(struct("*")).alias("rows"), count("*").alias("sensor_count"))
)

query = grouped_df.writeStream.foreachBatch(foreach_batch).start()
query.awaitTermination()
