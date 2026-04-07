import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh


st.set_page_config(page_title="Crypto Volatility Dashboard", layout="wide")
st_autorefresh(interval=5000, key="dashboard_refresh")

PREDICTIONS_PATH = "data/predictions"
METRICS_PATH = "data/metrics"


@st.cache_data(ttl=5)
def load_predictions():
    try:
        frame = pd.read_parquet(PREDICTIONS_PATH)
    except Exception:
        return pd.DataFrame()

    if not frame.empty:
        if "prediction_timestamp" in frame.columns:
            frame["prediction_timestamp"] = pd.to_datetime(frame["prediction_timestamp"], errors="coerce")
        if "market_timestamp" in frame.columns:
            frame["market_timestamp"] = pd.to_datetime(frame["market_timestamp"], errors="coerce")
    return frame


@st.cache_data(ttl=5)
def load_metrics():
    try:
        frame = pd.read_parquet(METRICS_PATH)
    except Exception:
        return pd.DataFrame()

    if not frame.empty and "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    return frame


st.title("Real-Time Crypto Volatility Predictions")

pred_df = load_predictions()
metrics_df = load_metrics()

if pred_df.empty:
    st.warning(
        "No prediction data available yet. Run coinbase_to_kafka.py and "
        "spark_prediction_pipeline.py first."
    )
    st.stop()

pred_df = pred_df.sort_values("prediction_timestamp")
latest_by_symbol = pred_df.groupby("symbol", as_index=False).tail(1)

btc_row = latest_by_symbol[latest_by_symbol["symbol"] == "BTC-USD"]
eth_row = latest_by_symbol[latest_by_symbol["symbol"] == "ETH-USD"]

avg_latency = pred_df["latency_seconds"].dropna().mean() if "latency_seconds" in pred_df.columns else float("nan")
total_predictions = int(pred_df["predicted_volatility_60min"].notna().sum())
latest_risk = latest_by_symbol[["symbol", "risk_signal"]].fillna("N/A")

col1, col2, col3, col4 = st.columns(4)
col1.metric(
    "BTC Predicted Volatility",
    f"{btc_row['predicted_volatility_60min'].iloc[0]:.5f}" if not btc_row.empty else "N/A",
)
col2.metric(
    "ETH Predicted Volatility",
    f"{eth_row['predicted_volatility_60min'].iloc[0]:.5f}" if not eth_row.empty else "N/A",
)
col3.metric("Avg Latency (s)", f"{avg_latency:.2f}" if pd.notna(avg_latency) else "N/A")
col4.metric("Total Predictions", total_predictions)

st.subheader("Latest Risk Signals")
st.dataframe(latest_risk, use_container_width=True, hide_index=True)

st.subheader("Predicted Volatility Over Time")
volatility_df = pred_df.dropna(subset=["prediction_timestamp", "predicted_volatility_60min"])
if not volatility_df.empty:
    pivot_df = volatility_df.pivot_table(
        index="prediction_timestamp",
        columns="symbol",
        values="predicted_volatility_60min",
        aggfunc="last",
    )
    st.line_chart(pivot_df)
else:
    st.info("No non-null predictions available yet.")

chart_left, chart_right = st.columns(2)

with chart_left:
    st.subheader("Current vs Predicted Volatility")
    compare_df = pred_df.dropna(subset=["prediction_timestamp"]).copy()
    compare_df = compare_df.set_index("prediction_timestamp")[
        ["current_volatility", "predicted_volatility_60min"]
    ]
    st.line_chart(compare_df)

with chart_right:
    st.subheader("Latency Over Time")
    if "latency_seconds" in pred_df.columns:
        latency_df = pred_df.dropna(subset=["prediction_timestamp", "latency_seconds"]).set_index(
            "prediction_timestamp"
        )[["latency_seconds"]]
        st.line_chart(latency_df)
    else:
        st.info("Latency data not available.")

if not metrics_df.empty:
    st.subheader("System Metrics")
    available_cols = [
        column
        for column in ["throughput_msg_per_sec", "avg_processing_time_ms", "p95_latency_seconds"]
        if column in metrics_df.columns
    ]
    if "timestamp" in metrics_df.columns and available_cols:
        system_df = metrics_df.dropna(subset=["timestamp"]).set_index("timestamp")[available_cols]
        st.line_chart(system_df)

st.subheader("Latest Predictions")
st.dataframe(
    pred_df.sort_values("prediction_timestamp", ascending=False).head(20),
    use_container_width=True,
)

if not metrics_df.empty:
    st.subheader("Latest Batch Metrics")
    sort_column = "timestamp" if "timestamp" in metrics_df.columns else metrics_df.columns[0]
    st.dataframe(
        metrics_df.sort_values(sort_column, ascending=False).head(20),
        use_container_width=True,
    )
