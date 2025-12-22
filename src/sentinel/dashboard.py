import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Sentinel NIDS Dashboard",
    layout="wide"
)

st.title("Sentinel Monitoring Dashboard")

def load_data():
    columns = ["timestamp", "packet_id", "prediction", "score"]
    try: 
        df = pd.read_csv("data/predictions.csv")
        
        if list(df.columns) != columns:
            df.columns = columns
            
        return df.tail(100)
    except Exception:
        return pd.DataFrame(columns=columns)

# This function will rerun every 1s automatically
@st.fragment(run_every=1)
def run_realtime_dashboard():
    df = load_data()

    if df.empty:
        st.spinner("Waiting for live predictions...")
        return 

    # KPIs
    total_packets = len(df)
    anomaly_count = (df["prediction"] == "Anomaly").sum()
    normal_count = total_packets - anomaly_count
    
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total Packets Scanned", total_packets)
    kpi2.metric("Normal Traffic", normal_count)
    kpi3.metric("Threats Detected", int(anomaly_count), delta=int(anomaly_count), delta_color="inverse")

    # Chart prep
    df["status_val"] = (df["prediction"] == "Anomaly").astype(int)

    fig = px.area(
        df,
        x="timestamp",
        y="status_val",
        title="Real Time Threat Level (1 = Anomaly, 0 = Normal)",
        color="prediction",
        color_discrete_map={
            "Normal": "green",
            "Anomaly": "red"
        },
        height=350
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key="threat_chart" 
    )

    st.subheader("Recent Traffic Logs")
    st.dataframe(
        df.sort_values("timestamp", ascending=False).head(10),
        use_container_width=True,
        hide_index=True
    )

run_realtime_dashboard()