import json
import pandas as pd
import streamlit as st
import plotly.express as px
from kafka import KafkaConsumer
import os

st.set_page_config(page_title="Bitola Air Quality Dashboard", layout="wide")

st.title("Bitola Air Quality Dashboard")

PM10_TOPIC = "FullPm10WeatherData"
PM25_TOPIC = "FullPm25WeatherData"
PM10_ZERO_SHOT_TOPIC = "FullPm10WeatherData_ZeroShot"
PM25_ZERO_SHOT_TOPIC = "FullPm25WeatherData_ZeroShot"
ALL_TOPICS = [PM10_TOPIC, PM25_TOPIC, PM10_ZERO_SHOT_TOPIC, PM25_ZERO_SHOT_TOPIC]
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAX_POINTS = 24000


@st.cache_resource
def create_consumer():
    return KafkaConsumer(
        *ALL_TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000
    )


def normalize_row(row, topic):
    row = dict(row)

    if "timestamp" in row:
        row["timestamp"] = pd.to_datetime(row["timestamp"], utc=True, errors="coerce")
    if "forecast_origin" in row:
        row["forecast_origin"] = pd.to_datetime(row["forecast_origin"], utc=True, errors="coerce")

    if "sensorId" in row:
        row["sensorId"] = str(row["sensorId"])

    row["topic"] = topic

    if topic == PM10_TOPIC:
        row["metric"] = "pm10"
        row["model"] = "fine_tuned"
        row["value"] = row.get("pm10")
    elif topic == PM25_TOPIC:
        row["metric"] = "pm25"
        row["model"] = "fine_tuned"
        row["value"] = row.get("pm25")
    elif topic == PM10_ZERO_SHOT_TOPIC:
        row["metric"] = "pm10"
        row["model"] = "zero_shot"
        row["value"] = row.get("pm10")
    elif topic == PM25_ZERO_SHOT_TOPIC:
        row["metric"] = "pm25"
        row["model"] = "zero_shot"
        row["value"] = row.get("pm25")
    else:
        row["metric"] = "unknown"
        row["model"] = "unknown"
        row["value"] = None

    row["value"] = pd.to_numeric(row["value"], errors="coerce")
    row["horizon_hours"] = pd.to_numeric(row.get("horizon_hours"), errors="coerce")

    return row


def add_forecast_columns(df):
    if "forecast_origin" not in df.columns:
        df["forecast_origin"] = pd.NaT

    df["forecast_origin"] = pd.to_datetime(df["forecast_origin"], utc=True, errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["forecast_origin"] = df["forecast_origin"].fillna(df["timestamp"])

    if "horizon_hours" not in df.columns:
        df["horizon_hours"] = pd.NA

    inferred_horizon = (df["timestamp"] - df["forecast_origin"]).dt.total_seconds() / 3600
    df["horizon_hours"] = pd.to_numeric(df["horizon_hours"], errors="coerce").fillna(inferred_horizon)
    df["horizon_hours"] = df["horizon_hours"].round().astype("Int64")
    df["forecast_origin_label"] = df["forecast_origin"].dt.strftime("%Y-%m-%d %H:%M")
    return df


def poll_messages(consumer, max_records=200):
    batch = consumer.poll(timeout_ms=1000, max_records=max_records)
    rows = []

    for topic_partition, messages in batch.items():
        topic = topic_partition.topic
        for msg in messages:
            row = normalize_row(msg.value, topic)
            rows.append(row)

    return rows


# -------------------------
# State
# -------------------------
if "rows" not in st.session_state:
    st.session_state.rows = []

consumer = create_consumer()


# -------------------------
# Controls
# -------------------------
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    selected_metric = st.selectbox("Metric", ["pm10", "pm25", "both"])

with col2:
    selected_model = st.selectbox("Model", ["fine_tuned", "zero_shot", "both"])

with col3:
    forecast_view = st.selectbox("Forecast view", ["latest cycle", "compare last 3", "all buffered"])

with col4:
    auto_refresh = st.toggle("Auto refresh", value=True)

with col5:
    refresh_now = st.button("Refresh now")


# -------------------------
# Data fetch
# -------------------------
if auto_refresh or refresh_now:
    new_rows = poll_messages(consumer)
    if new_rows:
        st.session_state.rows.extend(new_rows)
        st.session_state.rows = st.session_state.rows[-MAX_POINTS:]


df = pd.DataFrame(st.session_state.rows)


# -------------------------
# Visualization
# -------------------------
if df.empty:
    st.info("No data received yet from Kafka topics.")
else:
    df = df.dropna(subset=["timestamp"])
    if "sensorId" in df.columns:
        df["sensorId"] = df["sensorId"].astype(str)
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = add_forecast_columns(df)
    df = df.sort_values(["forecast_origin", "timestamp"])

    # -------------------------
    # Metric filtering
    # -------------------------
    if selected_metric == "both":
        metric_df = df.copy()
    else:
        metric_df = df[df["metric"] == selected_metric].copy()

    if selected_model != "both":
        metric_df = metric_df[metric_df["model"] == selected_model].copy()

    if metric_df.empty:
        st.warning(f"No data available.")
    else:
        # -------------------------
        # Sensor filtering
        # -------------------------
        sensor_ids = sorted(metric_df["sensorId"].astype(str).unique())

        selected_sensors = st.multiselect(
            "Sensors",
            options=sensor_ids,
            default=sensor_ids[:5] if len(sensor_ids) > 5 else sensor_ids
        )

        if selected_sensors:
            metric_df = metric_df[metric_df["sensorId"].astype(str).isin(selected_sensors)]

        origins = sorted(metric_df["forecast_origin"].dropna().unique())
        if forecast_view == "latest cycle" and origins:
            shown_origins = origins[-1:]
            plot_df = metric_df[metric_df["forecast_origin"].isin(shown_origins)].copy()
        elif forecast_view == "compare last 3" and origins:
            shown_origins = origins[-3:]
            plot_df = metric_df[metric_df["forecast_origin"].isin(shown_origins)].copy()
        else:
            shown_origins = origins
            plot_df = metric_df.copy()

        if plot_df.empty:
            st.warning("No data available for the selected forecast view.")
            st.stop()

        # -------------------------
        # KPIs
        # -------------------------
        latest_value = plot_df["value"].dropna().iloc[-1] if not plot_df["value"].dropna().empty else None
        sensor_count = plot_df["sensorId"].nunique()
        latest_origin = max(shown_origins).strftime("%Y-%m-%d %H:%M") if shown_origins else "N/A"

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Metric", selected_metric.upper())
        k2.metric("Sensors shown", sensor_count)
        k3.metric("Forecast cycle", latest_origin)
        k4.metric("Latest value", f"{latest_value:.2f}" if latest_value is not None else "N/A")

        # -------------------------
        # Plot
        # -------------------------
        plot_df = plot_df.sort_values(["forecast_origin", "timestamp"])

        if selected_metric == "both":
            plot_df["label"] = (
                plot_df["sensorId"].astype(str)
                + " - "
                + plot_df["metric"]
                + " - "
                + plot_df["model"]
            )
            if forecast_view != "latest cycle":
                plot_df["label"] = plot_df["label"] + " - " + plot_df["forecast_origin_label"]

            fig = px.line(
                plot_df,
                x="timestamp",
                y="value",
                color="label",
                line_dash="metric",
                markers=True,
                title="Bitola predictions by metric and model"
            )

        else:
            plot_df["label"] = plot_df["sensorId"].astype(str) + " - " + plot_df["model"]
            if forecast_view != "latest cycle":
                plot_df["label"] = plot_df["label"] + " - " + plot_df["forecast_origin_label"]
            fig = px.line(
                plot_df,
                x="timestamp",
                y="value",
                color="label",
                line_dash="model",
                markers=True,
                title=f"Bitola {selected_metric.upper()} predictions over time"
            )

        st.plotly_chart(fig, width="stretch")

        # -------------------------
        # Tables
        # -------------------------
        latest_per_sensor = (
            plot_df.sort_values(["forecast_origin", "timestamp"])
            .groupby(["sensorId", "model", "metric"], as_index=False)
            .tail(1)
            .sort_values("value", ascending=False)
        )

        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.subheader("Latest records")
            st.dataframe(
                plot_df[
                    ["forecast_origin", "horizon_hours", "timestamp", "sensorId", "model", "metric", "value"]
                ].tail(30),
                width="stretch"
            )

        with col_right:
            st.subheader("Latest per sensor")
            st.dataframe(
                latest_per_sensor[
                    ["sensorId", "model", "metric", "forecast_origin", "horizon_hours", "timestamp", "value"]
                ],
                width="stretch"
            )


# -------------------------
# Auto refresh
# -------------------------
if auto_refresh:
    st.rerun()
