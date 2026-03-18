"""
Telecom Analytics Dashboard
============================
Streamlit dashboard connecting to Snowflake Gold layer
and displaying:
  1. KPI summary cards
  2. Churn risk distribution
  3. Revenue by region
  4. Tower performance map
  5. Top at-risk customers

Usage:
    pip install streamlit plotly snowflake-connector-python python-dotenv
    streamlit run dashboard/app.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Telecom Analytics Dashboard",
    page_icon="📡",
    layout="wide"
)

# ── Snowflake Connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account="mshkneu-vq44359",
        user="ADVITHI",
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="TELECOM_WH",
        database="TELECOM_DB",
        schema="SILVER_GOLD"
    )

@st.cache_data(ttl=300)
def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    return df

# ── Load Data ─────────────────────────────────────────────────────────────────
customers_df = run_query("SELECT * FROM SILVER_GOLD.CUSTOMER_MONTHLY_SUMMARY")
churn_df     = run_query("SELECT * FROM SILVER_GOLD.CHURN_RISK_SCORE")
tower_df     = run_query("SELECT * FROM SILVER_GOLD.TOWER_PERFORMANCE")

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📡 Telecom Analytics Dashboard")
st.markdown("Real-time insights from the UK Telecom Data Pipeline")
st.divider()

# ── Row 1: KPI Cards ──────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

total_customers  = len(customers_df)
total_revenue    = customers_df["TOTAL_REVENUE"].sum()
churned          = customers_df["IS_CHURNED"].sum()
churn_rate       = (churned / total_customers) * 100
critical_risk    = len(churn_df[churn_df["CHURN_RISK_LABEL"] == "Critical"])
poor_towers      = len(tower_df[tower_df["TOWER_HEALTH_RATING"] == "Poor"])

col1.metric("Total Customers",  f"{total_customers:,}")
col2.metric("Total Revenue",    f"£{total_revenue:,.0f}")
col3.metric("Churn Rate",       f"{churn_rate:.1f}%")
col4.metric("Critical Risk",    f"{critical_risk:,} customers")
col5.metric("Poor Towers",      f"{poor_towers:,} towers")

st.divider()

# ── Row 2: Churn Risk + Revenue by Region ────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Churn Risk Distribution")
    churn_counts = churn_df["CHURN_RISK_LABEL"].value_counts().reset_index()
    churn_counts.columns = ["Risk Label", "Count"]
    color_map = {
        "Critical": "#E24B4A",
        "High":     "#EF9F27",
        "Medium":   "#378ADD",
        "Low":      "#1D9E75"
    }
    fig_churn = px.pie(
        churn_counts,
        values="Count",
        names="Risk Label",
        color="Risk Label",
        color_discrete_map=color_map,
        hole=0.4
    )
    fig_churn.update_layout(margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig_churn, use_container_width=True)

with col2:
    st.subheader("Revenue by Region")
    revenue_region = customers_df.groupby("REGION")["TOTAL_REVENUE"] \
        .sum().reset_index().sort_values("TOTAL_REVENUE", ascending=True)
    fig_revenue = px.bar(
        revenue_region,
        x="TOTAL_REVENUE",
        y="REGION",
        orientation="h",
        color="TOTAL_REVENUE",
        color_continuous_scale="teal",
        labels={"TOTAL_REVENUE": "Total Revenue (£)", "REGION": "Region"}
    )
    fig_revenue.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_revenue, use_container_width=True)

st.divider()

# ── Row 3: Tower Performance + Plan Distribution ──────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Tower Health by Region")
    tower_health = tower_df.groupby(["REGION", "TOWER_HEALTH_RATING"]) \
        .size().reset_index(name="Count")
    fig_tower = px.bar(
        tower_health,
        x="REGION",
        y="Count",
        color="TOWER_HEALTH_RATING",
        color_discrete_map={
            "Excellent": "#1D9E75",
            "Good":      "#378ADD",
            "Fair":      "#EF9F27",
            "Poor":      "#E24B4A"
        },
        labels={"Count": "Number of Towers", "REGION": "Region"}
    )
    fig_tower.update_layout(margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig_tower, use_container_width=True)

with col2:
    st.subheader("Customers by Plan")
    plan_counts = customers_df.groupby("PLAN")["CUSTOMER_ID"] \
        .count().reset_index(name="Count")
    fig_plan = px.bar(
        plan_counts,
        x="PLAN",
        y="Count",
        color="PLAN",
        color_discrete_sequence=["#378ADD", "#1D9E75", "#EF9F27", "#E24B4A"],
        labels={"Count": "Number of Customers", "PLAN": "Plan"}
    )
    fig_plan.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        showlegend=False
    )
    st.plotly_chart(fig_plan, use_container_width=True)

st.divider()

# ── Row 4: Top At-Risk Customers ──────────────────────────────────────────────
st.subheader("Top 10 At-Risk Customers")

top_risk = churn_df[churn_df["CHURN_RISK_LABEL"].isin(["Critical", "High"])] \
    .sort_values("CHURN_RISK_SCORE", ascending=False) \
    .head(10)[["CUSTOMER_ID", "FULL_NAME", "REGION", "PLAN",
               "TENURE_MONTHS", "CHURN_RISK_SCORE", "CHURN_RISK_LABEL",
               "NUM_COMPLAINTS", "TOTAL_LATE_PAYMENTS"]]

# Color code risk label
def color_risk(val):
    colors = {
        "Critical": "background-color: #FCEBEB; color: #A32D2D",
        "High":     "background-color: #FAEEDA; color: #633806",
    }
    return colors.get(val, "")

st.dataframe(
    top_risk.style.applymap(color_risk, subset=["CHURN_RISK_LABEL"]),
    use_container_width=True,
    hide_index=True
)

st.divider()

# ── Row 5: Network Performance ────────────────────────────────────────────────
st.subheader("Network Performance by Type")

network_perf = tower_df.groupby("NETWORK_TYPE").agg(
    avg_download=("AVG_DOWNLOAD_SPEED", "mean"),
    avg_upload=("AVG_UPLOAD_SPEED", "mean"),
    avg_latency=("AVG_LATENCY", "mean"),
    avg_drop_rate=("AVG_DROPPED_CALL_RATE", "mean")
).reset_index().round(2)

col1, col2 = st.columns(2)

with col1:
    fig_speed = px.bar(
        network_perf,
        x="NETWORK_TYPE",
        y=["avg_download", "avg_upload"],
        barmode="group",
        labels={"value": "Speed (Mbps)", "NETWORK_TYPE": "Network Type"},
        color_discrete_sequence=["#378ADD", "#1D9E75"]
    )
    fig_speed.update_layout(margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig_speed, use_container_width=True)

with col2:
    fig_drop = px.bar(
        network_perf,
        x="NETWORK_TYPE",
        y="avg_drop_rate",
        color="NETWORK_TYPE",
        color_discrete_sequence=["#E24B4A", "#EF9F27", "#378ADD"],
        labels={"avg_drop_rate": "Avg Drop Rate (%)", "NETWORK_TYPE": "Network Type"}
    )
    fig_drop.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        showlegend=False
    )
    st.plotly_chart(fig_drop, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    "Built with Python · Snowflake · dbt · Azure Data Factory · Apache Airflow",
)