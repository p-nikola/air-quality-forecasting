import joblib
import pandas as pd
import numpy as np
import findspark
import pyspark as spark
from pyspark.sql import SparkSession
from chronos import Chronos2Pipeline
from pyspark.sql.types import StructType, StructField,FloatType,TimestampType,StringType
from pyspark.sql.functions import col,to_json,struct,from_json,to_timestamp,count, collect_list
from pathlib import Path
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
BASE_DIR = Path(__file__).resolve().parents[2]   # project/
OFFLINE_DIR = BASE_DIR / "offline-Phase"
DEFAULT_CONTEXT_PATH = Path(__file__).resolve().parent / "context_bitola.csv"
CONTEXT_PATH = Path(os.getenv("BITOLA_CONTEXT_CSV_PATH", str(DEFAULT_CONTEXT_PATH))).expanduser()

if not CONTEXT_PATH.exists():
    raise FileNotFoundError(f"Context CSV not found at: {CONTEXT_PATH}")

pipeline_pm10 = Chronos2Pipeline.from_pretrained(
    str(OFFLINE_DIR / "bitola_chronos_pipeline_pm10"),
    local_files_only=True
)
pipeline_pm25 = Chronos2Pipeline.from_pretrained(
    str(OFFLINE_DIR / "bitola_chronos_pipeline_pm25"),
    local_files_only=True
)

feature_scaler = joblib.load(OFFLINE_DIR / "feature_scaler.pkl")
pm10_scaler_obj = joblib.load(OFFLINE_DIR / "pm10_scaler.pkl")
pm25_scaler_obj = joblib.load(OFFLINE_DIR / "pm25_scaler.pkl")

# ### Pandas Functions from the offline phase


def extract_time_features(df, timestamp_col='timestamp'):


    df['hour_sin'] = np.sin(2 * np.pi * df[timestamp_col].dt.hour / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df[timestamp_col].dt.hour / 24)


    df['month_sin'] = np.sin(2 * np.pi * (df[timestamp_col].dt.month - 1) / 12)
    df['month_cos'] = np.cos(2 * np.pi * (df[timestamp_col].dt.month - 1) / 12)
    
    
    df['day_sin'] = np.sin(2 * np.pi * df[timestamp_col].dt.dayofweek / 7)
    df['day_cos'] = np.cos(2 * np.pi * df[timestamp_col].dt.dayofweek / 7)

    df['is_weekend'] = df[timestamp_col].dt.dayofweek.isin([5, 6]).astype(int)

 
    df['is_heating_season'] = df[timestamp_col].dt.month.isin([11, 12, 1, 2, 3]).astype(int)

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
neighbourhood_matrix

def load_context():
    context_df = pd.read_csv(CONTEXT_PATH)
    context_df = context_df.drop(columns=['city'])
    context_df['timestamp'] = pd.to_datetime(context_df['timestamp'])
    return context_df

def predict_target(pdf, context_scaled, target, scaler, pipeline, ID_COL, TIME_COL, numeric_features):
    
    context_target = context_scaled.copy()

    context_target[target] = scaler.transform(context_target[[target]])

    forecast_df = pipeline.predict_df(
        df=context_target,
        prediction_length=1,
        target=target,
        id_column=ID_COL,
        future_df=pdf,
        validate_inputs=False
    )

    result_df = pdf.merge(
        forecast_df[[ID_COL, TIME_COL, "predictions"]],
        on=[ID_COL, TIME_COL],
        how="left"
    )

    result_df[numeric_features] = feature_scaler.inverse_transform(result_df[numeric_features])

    result_df["predictions"] = scaler.inverse_transform(result_df[["predictions"]])

    result_df = result_df.rename(columns={"predictions": target})

    return result_df

def process_batch(pdf, context_df):
    ID_COL = "sensorId"
    TIME_COL = "timestamp"

    numeric_features = [
        'humidity', 'pressure', 'temperature', 'wind_speed',
        'neighbor1_humidity', 'neighbor2_humidity', 'neighbor3_humidity',
        'neighbor1_pressure', 'neighbor2_pressure', 'neighbor3_pressure',
        'neighbor1_temperature', 'neighbor2_temperature', 'neighbor3_temperature',
        'neighbor1_wind_speed', 'neighbor2_wind_speed', 'neighbor3_wind_speed'
    ]

    pdf[TIME_COL] = pd.to_datetime(pdf[TIME_COL], utc=True)
    context_df[TIME_COL] = pd.to_datetime(context_df[TIME_COL], utc=True)

    # go pravime ova za da osigurame deka i pdf i context_df imaat site potrebni koloni, ako ne, da gi dodademe so NaN vrednosti
    # za da ne crashne modelot posle
    for col in numeric_features:
        if col not in pdf.columns:
            pdf[col] = np.nan
        if col not in context_df.columns:
            context_df[col] = np.nan

    context_scaled = context_df.copy().sort_values([ID_COL, TIME_COL])

    print("Context max timestamp:", context_scaled[TIME_COL].max())

    pdf[numeric_features] = feature_scaler.transform(pdf[numeric_features])
    context_scaled[numeric_features] = feature_scaler.transform(context_scaled[numeric_features])

    pm10_df = predict_target(
    pdf, context_scaled,
    target="pm10",
    scaler=pm10_scaler_obj,
    pipeline=pipeline_pm10,
    ID_COL=ID_COL,
    TIME_COL=TIME_COL,
    numeric_features=numeric_features
    )

    pm25_df = predict_target(
        pdf, context_scaled,
        target="pm25",
        scaler=pm25_scaler_obj,
        pipeline=pipeline_pm25,
        ID_COL=ID_COL,
        TIME_COL=TIME_COL,
        numeric_features=numeric_features
    )

    return pm10_df, pm25_df

def write_to_kafka(df, topic):
    spark_df = spark.createDataFrame(df)

    kafka_df = spark_df.select(
        col("sensorId").cast("string").alias("key"),
        to_json(struct(*spark_df.columns)).alias("value")
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
        print("Empty batch — skipping")
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

        incoming_ids = set(ts_df['sensorId'].unique())
        existing_ids = set(context_df['sensorId'].unique()) if not context_df.empty else set()
        new_ids = incoming_ids - existing_ids

        if new_ids:
            print(f"New sensors detected: {new_ids}")

            numeric_columns = [
                col for col in context_df.columns
                if col in ['temperature','wind_speed','humidity','pm10','pm25','pressure']
            ]

            city_baseline = context_df.groupby('timestamp')[numeric_columns].median().reset_index()

            proxy_rows = []

            for sid in new_ids:
                proxy_history = city_baseline.copy()
                proxy_history['sensorId'] = sid

                temp_combined = pd.concat([context_df, proxy_history], ignore_index=True)

                refined_data = append_neighbors(temp_combined, neighbourhood_matrix)
                refined_data = extract_time_features(refined_data)

                new_sensor_proxy = refined_data[refined_data['sensorId'] == sid]
                proxy_rows.append(new_sensor_proxy)

            context_df = pd.concat([context_df, *proxy_rows], ignore_index=True)

        ts_df["timestamp"] = pd.to_datetime(ts_df["timestamp"], utc=True)

        ts_df = extract_time_features(ts_df)
        ts_df = append_neighbors(ts_df, neighbourhood_matrix)

        future_df = ts_df.copy()

        pm10_df, pm25_df = process_batch(future_df, context_df)

        if pm10_df is None or pm25_df is None:
            print("Prediction skipped")
            continue

        write_to_kafka(pm10_df, topic="FullPm10WeatherData")
        write_to_kafka(pm25_df, topic="FullPm25WeatherData")

        context_df = pd.concat([context_df, ts_df], ignore_index=True)

        context_df = (
            context_df
            .sort_values(["sensorId", "timestamp"])
            .drop_duplicates(subset=["sensorId", "timestamp"], keep="last")
            .groupby("sensorId")
            .tail(72)
            .reset_index(drop=True)
        )

        print(f"Prediction done for {ts}")
        print(f"Context size: {len(context_df)}")

# # Online Phase (Main Program)


findspark.init()

spark = SparkSession.builder \
    .appName("KafkaConsumerExample") \
    .config( "spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")  \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
    .option("subscribePattern", "sensor_.*") \
    .load()

df.printSchema()

schema = StructType([
    StructField("timestamp",TimestampType(),True),
    StructField("sensorId",StringType(),True),
    StructField("lat",FloatType(),True),
    StructField("lon",FloatType(),True),
    StructField("humidity",FloatType(),True),
    StructField("pressure",FloatType(),True),
    StructField("temperature",FloatType(),True),
    StructField("wind_speed",FloatType(),True)
])

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .drop("lat", "lon")

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

parsed_df.printSchema()

grouped_df = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy("timestamp") \
    .agg(
        collect_list(struct("*")).alias("rows"),
        count("*").alias("sensor_count")
    )

query = grouped_df.writeStream \
    .foreachBatch(foreach_batch) \
    .start()

query.awaitTermination()
