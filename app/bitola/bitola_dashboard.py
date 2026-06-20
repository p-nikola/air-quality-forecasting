import sqlite3
import time as time_module
from datetime import datetime, time
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import os

st.set_page_config(page_title="Bitola Air Quality Dashboard", layout="wide")

st.title("Bitola Air Quality Dashboard")

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = Path(os.getenv("PROJECT_DB_PATH", str(BASE_DIR / "data" / "project.db"))).expanduser()
CITY = "Bitola"


refresh_col1, refresh_col2, refresh_col3 = st.columns([1, 1, 3])

with refresh_col1:
    refresh_now = st.button("Refresh data")

with refresh_col2:
    auto_refresh = st.toggle("Auto refresh", value=False)

with refresh_col3:
    refresh_seconds = st.number_input(
        "Refresh interval seconds",
        min_value=5,
        max_value=300,
        value=30,
        step=5,
        disabled=not auto_refresh,
    )

if refresh_now:
    st.cache_data.clear()
    st.rerun()


def table_exists(conn, table_name):
    result = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return result is not None


def online_model_label(model_version):
    if pd.isna(model_version):
        return "Unknown"
    if "fine_tuned" in model_version:
        return "Fine-tuned"
    if "zero_shot" in model_version:
        return "Zero-shot"
    return str(model_version)


@st.cache_data(ttl=60)
def load_historical_metadata(db_path):
    db_file = Path(db_path)
    if not db_file.exists():
        return pd.DataFrame()

    frames = []
    with sqlite3.connect(db_file) as conn:
        if table_exists(conn, "offline_test_results"):
            frames.append(
                pd.read_sql_query(
                    """
                    SELECT
                        'offline_test' AS phase,
                        pollutant,
                        sensor_id,
                        MIN(timestamp) AS min_timestamp,
                        MAX(timestamp) AS max_timestamp,
                        COUNT(*) AS row_count
                    FROM offline_test_results
                    WHERE city = ?
                    GROUP BY pollutant, sensor_id
                    """,
                    conn,
                    params=(CITY,),
                )
            )

        if table_exists(conn, "online_forecasts"):
            frames.append(
                pd.read_sql_query(
                    """
                    SELECT
                        'online_forecast' AS phase,
                        pollutant,
                        sensor_id,
                        MIN(target_at) AS min_timestamp,
                        MAX(target_at) AS max_timestamp,
                        COUNT(*) AS row_count
                    FROM online_forecasts
                    WHERE city = ?
                    GROUP BY pollutant, sensor_id
                    """,
                    conn,
                    params=(CITY,),
                )
            )

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


@st.cache_data(ttl=30)
def load_online_issue_hour_options(db_path, start_at, end_at, pollutants, sensor_ids):
    db_file = Path(db_path)
    if not db_file.exists():
        return []

    pollutants = set(pollutants)
    sensor_ids = set(sensor_ids)

    with sqlite3.connect(db_file) as conn:
        if not table_exists(conn, "online_forecasts"):
            return []

        issue_hour_df = pd.read_sql_query(
            """
            SELECT DISTINCT strftime('%H', issued_at) AS issue_hour, pollutant, sensor_id
            FROM online_forecasts
            WHERE city = ?
              AND issued_at BETWEEN ? AND ?
            ORDER BY issue_hour
            """,
            conn,
            params=(CITY, start_at, end_at),
        )

    if issue_hour_df.empty:
        return []

    issue_hour_df = issue_hour_df[
        issue_hour_df["pollutant"].isin(pollutants)
        & issue_hour_df["sensor_id"].astype(str).isin(sensor_ids)
    ].copy()

    hours = issue_hour_df["issue_hour"].dropna().astype(int).unique()
    return [f"{hour:02d}:00" for hour in sorted(hours)]


@st.cache_data(ttl=30)
def load_historical_rows(
    db_path,
    start_at,
    end_at,
    pollutants,
    sensor_ids,
    online_issue_hour=None,
    online_models=None,
    shown_series=None,
):
    db_file = Path(db_path)
    if not db_file.exists():
        return pd.DataFrame()

    pollutants = set(pollutants)
    sensor_ids = set(sensor_ids)
    online_models = set(online_models or [])
    shown_series = set(shown_series or [])
    frames = []

    with sqlite3.connect(db_file) as conn:
        if table_exists(conn, "offline_test_results") and (
            "Actual" in shown_series or "Offline test prediction" in shown_series
        ):
            offline_df = pd.read_sql_query(
                """
                SELECT timestamp, sensor_id, pollutant, actual_value, predicted_value
                FROM offline_test_results
                WHERE city = ?
                  AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp, sensor_id, pollutant
                """,
                conn,
                params=(CITY, start_at, end_at),
            )
            if not offline_df.empty:
                offline_df = offline_df[
                    offline_df["pollutant"].isin(pollutants)
                    & offline_df["sensor_id"].astype(str).isin(sensor_ids)
                ].copy()

                if "Actual" in shown_series:
                    actual_df = offline_df[["timestamp", "sensor_id", "pollutant", "actual_value"]].rename(
                        columns={"actual_value": "value"}
                    )
                    actual_df["series"] = "Actual"
                    frames.append(actual_df)

                if "Offline test prediction" in shown_series:
                    predicted_df = offline_df[["timestamp", "sensor_id", "pollutant", "predicted_value"]].rename(
                        columns={"predicted_value": "value"}
                    )
                    predicted_df["series"] = "Offline test prediction"
                    frames.append(predicted_df)

        if (
            "Online forecast" in shown_series
            and online_issue_hour is not None
            and online_models
            and table_exists(conn, "online_forecasts")
        ):
            online_df = pd.read_sql_query(
                """
                SELECT target_at AS timestamp, sensor_id, pollutant, predicted_value, model_version
                FROM online_forecasts
                WHERE city = ?
                  AND issued_at BETWEEN ? AND ?
                  AND strftime('%H', issued_at) = ?
                  AND target_at BETWEEN ? AND ?
                ORDER BY target_at, sensor_id, pollutant, model_version
                """,
                conn,
                params=(CITY, start_at, end_at, online_issue_hour[:2], start_at, end_at),
            )
            if not online_df.empty:
                online_df["model_label"] = online_df["model_version"].map(online_model_label)
                online_df = online_df[
                    online_df["pollutant"].isin(pollutants)
                    & online_df["sensor_id"].astype(str).isin(sensor_ids)
                    & online_df["model_label"].isin(online_models)
                ].copy()
                online_df = online_df.rename(columns={"predicted_value": "value"})
                online_df["series"] = "Online forecast - " + online_df["model_label"]
                frames.append(online_df[["timestamp", "sensor_id", "pollutant", "value", "series"]])

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["timestamp"] = pd.to_datetime(result["timestamp"], errors="coerce")
    result["sensor_id"] = result["sensor_id"].astype(str)
    result["value"] = pd.to_numeric(result["value"], errors="coerce")
    return result.dropna(subset=["timestamp", "value"]).sort_values("timestamp")


def render_historical_results():
    st.subheader("Historical Results")

    metadata = load_historical_metadata(str(DB_PATH))
    if metadata.empty:
        st.info(f"No historical SQLite data found at {DB_PATH}.")
        return

    metadata["min_timestamp"] = pd.to_datetime(metadata["min_timestamp"], errors="coerce")
    metadata["max_timestamp"] = pd.to_datetime(metadata["max_timestamp"], errors="coerce")
    metadata = metadata.dropna(subset=["min_timestamp", "max_timestamp"])

    if metadata.empty:
        st.info("Historical data exists, but no valid timestamps were found.")
        return

    available_pollutants = sorted(metadata["pollutant"].dropna().unique())
    metric_options = available_pollutants + (["both"] if len(available_pollutants) > 1 else [])

    hcol1, hcol2, hcol3 = st.columns([1, 2, 2])

    with hcol1:
        selected_historical_metric = st.selectbox(
            "Historical metric",
            metric_options,
            index=metric_options.index("pm10") if "pm10" in metric_options else 0,
        )

    selected_pollutants = (
        available_pollutants
        if selected_historical_metric == "both"
        else [selected_historical_metric]
    )

    sensor_options = sorted(
        metadata[metadata["pollutant"].isin(selected_pollutants)]["sensor_id"].astype(str).unique()
    )

    with hcol2:
        selected_historical_sensors = st.multiselect(
            "Historical sensors",
            options=sensor_options,
            default=sensor_options[:3],
        )

    min_date = metadata["min_timestamp"].min().date()
    max_date = metadata["max_timestamp"].max().date()

    with hcol3:
        selected_date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    if not selected_historical_sensors:
        st.warning("Select at least one sensor to show historical results.")
        return

    if not isinstance(selected_date_range, (tuple, list)) or len(selected_date_range) != 2:
        st.warning("Select a start and end date.")
        return

    start_date, end_date = selected_date_range
    start_at = datetime.combine(start_date, time.min).strftime("%Y-%m-%d %H:%M:%S")
    end_at = datetime.combine(end_date, time.max).strftime("%Y-%m-%d %H:%M:%S")

    series_options = ["Actual", "Offline test prediction", "Online forecast"]
    selected_series = st.multiselect(
        "Show series",
        options=series_options,
        default=["Actual", "Offline test prediction", "Online forecast"],
    )

    issue_hour_options = load_online_issue_hour_options(
        str(DB_PATH),
        start_at,
        end_at,
        tuple(selected_pollutants),
        tuple(selected_historical_sensors),
    )
    selected_online_issue_hour = None
    selected_online_models = []

    if "Online forecast" in selected_series:
        if issue_hour_options:
            default_issue_hour_index = issue_hour_options.index("12:00") if "12:00" in issue_hour_options else 0
            online_col1, online_col2 = st.columns([1, 1])
            with online_col1:
                selected_online_issue_hour = st.selectbox(
                    "Online forecast issue time",
                    options=issue_hour_options,
                    index=default_issue_hour_index,
                    help="Shows 24-hour forecast cycles issued at this hour for each day in the selected range.",
                )
            with online_col2:
                selected_online_models = st.multiselect(
                    "Online forecast model",
                    options=["Fine-tuned", "Zero-shot"],
                    default=["Fine-tuned"],
                )
        else:
            st.caption("No online forecast issue times found for this selection. Showing offline rows only.")

    if not selected_series:
        st.warning("Select at least one series to plot.")
        return

    effective_series = list(selected_series)
    if "Online forecast" in effective_series and (selected_online_issue_hour is None or not selected_online_models):
        effective_series = [
            series for series in effective_series if series != "Online forecast"
        ]

        if not effective_series:
            st.info("No online forecasts match this selection.")
            return

    if selected_online_issue_hour is not None:
        st.caption(
            f"Online forecast line shows daily 24-hour forecast cycles issued at {selected_online_issue_hour}."
        )

    historical_df = load_historical_rows(
        str(DB_PATH),
        start_at,
        end_at,
        tuple(selected_pollutants),
        tuple(selected_historical_sensors),
        selected_online_issue_hour,
        tuple(selected_online_models),
        tuple(effective_series),
    )

    if historical_df.empty:
        st.info("No offline test results or online forecasts match this date range.")
        return

    historical_df["label"] = (
        historical_df["sensor_id"]
        + " - "
        + historical_df["pollutant"].str.upper()
        + " - "
        + historical_df["series"]
    )

    fig = px.line(
        historical_df,
        x="timestamp",
        y="value",
        color="label",
        line_dash="series",
        markers=True,
        title="Historical actual values and model predictions",
    )
    st.plotly_chart(fig, width="stretch")

    with st.expander("Historical rows"):
        st.dataframe(
            historical_df[["timestamp", "sensor_id", "pollutant", "series", "value"]].tail(200),
            width="stretch",
        )


render_historical_results()

if auto_refresh:
    time_module.sleep(refresh_seconds)
    st.cache_data.clear()
    st.rerun()
