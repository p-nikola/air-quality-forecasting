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
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAX_POINTS = 1000


@st.cache_resource
def create_consumer():
    return KafkaConsumer(
        PM10_TOPIC,
        PM25_TOPIC,
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

    if "sensorId" in row:
        row["sensorId"] = str(row["sensorId"])

    row["topic"] = topic

    if topic == PM10_TOPIC:
        row["metric"] = "pm10"
        row["value"] = row.get("pm10")
    elif topic == PM25_TOPIC:
        row["metric"] = "pm25"
        row["value"] = row.get("pm25")
    else:
        row["metric"] = "unknown"
        row["value"] = None

    row["value"] = pd.to_numeric(row["value"], errors="coerce")

    return row


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
col1, col2, col3 = st.columns(3)

with col1:
    selected_metric = st.selectbox("Metric", ["pm10", "pm25", "both"])

with col2:
    auto_refresh = st.toggle("Auto refresh", value=True)

with col3:
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
    df = df.sort_values("timestamp")

    # -------------------------
    # Metric filtering
    # -------------------------
    if selected_metric == "both":
        metric_df = df.copy()
    else:
        metric_df = df[df["metric"] == selected_metric].copy()

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

        # -------------------------
        # KPIs
        # -------------------------
        latest_value = metric_df["value"].dropna().iloc[-1] if not metric_df["value"].dropna().empty else None
        sensor_count = metric_df["sensorId"].nunique()

        k1, k2, k3 = st.columns(3)
        k1.metric("Metric", selected_metric.upper())
        k2.metric("Sensors shown", sensor_count)
        k3.metric("Latest value", f"{latest_value:.2f}" if latest_value is not None else "N/A")

        # -------------------------
        # Plot
        # -------------------------
        metric_df = metric_df.sort_values("timestamp")

        if selected_metric == "both":
            metric_df["label"] = metric_df["sensorId"].astype(str) + " - " + metric_df["metric"]

            fig = px.line(
                metric_df,
                x="timestamp",
                y="value",
                color="label",
                line_dash="metric", 
                markers=True,
                title="PM10 (solid) vs PM25 (dotted)"
            )

        else:
            fig = px.line(
                metric_df,
                x="timestamp",
                y="value",
                color="sensorId",
                markers=True,
                title=f"Bitola {selected_metric.upper()} predictions over time"
            )

        st.plotly_chart(fig, width="stretch")

        # -------------------------
        # Tables
        # -------------------------
        latest_per_sensor = (
            metric_df.sort_values("timestamp")
            .groupby("sensorId", as_index=False)
            .tail(1)
            .sort_values("value", ascending=False)
        )

        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.subheader("Latest records")
            st.dataframe(
                metric_df[["timestamp", "sensorId", "metric", "value"]].tail(20),
                width="stretch"
            )

        with col_right:
            st.subheader("Latest per sensor")
            st.dataframe(
                latest_per_sensor[["sensorId", "timestamp", "metric", "value"]],
                width="stretch"
            )


# -------------------------
# Auto refresh
# -------------------------
if auto_refresh:
    st.rerun()
