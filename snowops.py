import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

st.set_page_config(
    page_title="Snowflake FinOps Dashboard",
    page_icon="❄️",
    layout="wide",
)


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

@st.cache_resource
def get_connection():

    conn = snowflake.connector.connect(
       account="RMCNBXS-PNB03755",
               user="SNOWPRO_PY",
               private_key_file=r"C:\Users\Narottam\rsa_key.p8",
               warehouse="FINOPS_WH",
               database="SNOWFLAKE",
               schema="ACCOUNT_USAGE",
               role="FINOPS_ROLE",
    )

    return conn







# ============================================================
# GENERIC QUERY FUNCTION
# ============================================================

#@st.cache_data(ttl=300)
@st.cache_resource
def run_query(query):

    conn = get_connection()

    try:
        df = pd.read_sql(query, conn)
        return df

    except Exception as e:
        st.error(f"Snowflake query failed: {e}")
        return pd.DataFrame()


# ============================================================
# FORMATTERS
# ============================================================

def bytes_to_tb(value):

    if value is None:
        return 0

    return value / (1024 ** 4)


def bytes_to_gb(value):

    if value is None:
        return 0

    return value / (1024 ** 3)


def format_number(value):

    if pd.isna(value):
        return "0"

    return f"{value:,.2f}"


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("❄️ Snowflake FinOps")

days = st.sidebar.slider(
    "Analysis period",
    min_value=7,
    max_value=90,
    value=30,
)

st.sidebar.markdown("---")

st.sidebar.caption(
    "Data source: SNOWFLAKE.ACCOUNT_USAGE"
)

if st.sidebar.button("🔄 Refresh data"):

    st.cache_data.clear()
    st.rerun()


# ============================================================
# HEADER
# ============================================================

st.title("❄️ Snowflake FinOps Dashboard")

st.caption(
    "Compute • Serverless • Storage • Data Services • Cost Drivers"
)

st.markdown(
    f"""
    **Analysis window:** Last {days} days  
    **Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    """
)


# ============================================================
# EXECUTIVE SUMMARY
# ============================================================

st.header("Executive Summary")


summary_query = f"""
SELECT
    SUM(CREDITS_USED) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
"""

summary_df = run_query(summary_query)

total_credits = (
    summary_df.iloc[0]["TOTAL_CREDITS"]
    if not summary_df.empty
    else 0
)


# Warehouse credits

warehouse_summary_query = f"""
SELECT
    SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICE_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
"""

warehouse_summary = run_query(warehouse_summary_query)

compute_credits = (
    warehouse_summary.iloc[0]["COMPUTE_CREDITS"]
    if not warehouse_summary.empty
    else 0
)

cloud_service_credits = (
    warehouse_summary.iloc[0]["CLOUD_SERVICE_CREDITS"]
    if not warehouse_summary.empty
    else 0
)


# Storage

storage_query = """
SELECT
    STORAGE_BYTES,
    STAGE_BYTES,
    FAILSAFE_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE USAGE_DATE = CURRENT_DATE() - 1
"""

storage_df = run_query(storage_query)

if not storage_df.empty:

    storage_bytes = storage_df.iloc[0]["STORAGE_BYTES"]
    stage_bytes = storage_df.iloc[0]["STAGE_BYTES"]
    failsafe_bytes = storage_df.iloc[0]["FAILSAFE_BYTES"]

else:

    storage_bytes = 0
    stage_bytes = 0
    failsafe_bytes = 0


# KPI cards

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric(
    "Total Credits",
    format_number(total_credits),
)

c2.metric(
    "Warehouse Compute",
    format_number(compute_credits),
)

c3.metric(
    "Cloud Services",
    format_number(cloud_service_credits),
)

c4.metric(
    "Table Storage",
    f"{bytes_to_tb(storage_bytes):,.2f} TB",
)

c5.metric(
    "Fail-safe",
    f"{bytes_to_tb(failsafe_bytes):,.2f} TB",
)


# ============================================================
# 1. WAREHOUSE USAGE TREND
# ============================================================

st.header("1. Warehouse Usage Trend")

warehouse_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICE_CREDITS,
    SUM(CREDITS_USED) AS TOTAL_CREDITS,
    SUM(CREDITS_ATTRIBUTED_COMPUTE_QUERIES)
        AS QUERY_ATTRIBUTED_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    WAREHOUSE_NAME
ORDER BY
    USAGE_DATE
"""

warehouse_df = run_query(warehouse_query)

if not warehouse_df.empty:

    fig = px.line(
        warehouse_df,
        x="USAGE_DATE",
        y="TOTAL_CREDITS",
        color="WAREHOUSE_NAME",
        markers=True,
        title="Warehouse Credits - Daily Trend",
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Credits",
        legend_title="Warehouse",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

else:

    st.info("No warehouse usage data found.")


# ============================================================
# 2. WAREHOUSE CAPACITY & QUEUE
# ============================================================

st.header("2. Warehouse Capacity & Queue")


conn = get_connection()


# ------------------------------------------------------------
# Current warehouse queue
# ------------------------------------------------------------
def get_current_warehouse_queue(conn):

    cursor = conn.cursor()

    try:
        cursor.execute("""
            SHOW WAREHOUSES
        """)

        rows = cursor.fetchall()

        columns = [c[0] for c in cursor.description]

        return pd.DataFrame(rows, columns=columns)

    finally:
        cursor.close()
st.subheader("🔴 Current Warehouse Queue")


current_wh_df = get_current_warehouse_queue(conn)


if not current_wh_df.empty:

    queue_df = current_wh_df[
        [
            "name",
            "state",
            "size",
            "started_clusters",
            "running",
            "queued",
            "min_cluster_count",
            "max_cluster_count",
            "scaling_policy",
        ]
    ].copy()

    queue_df.columns = [
        "Warehouse",
        "State",
        "Size",
        "Active Clusters",
        "Running SQL",
        "Queued SQL",
        "Min Clusters",
        "Max Clusters",
        "Scaling Policy",
    ]


    # --------------------------------------------------------
    # Highlight warehouses with queued SQL
    # --------------------------------------------------------

    def highlight_queue(row):

        if row["Queued SQL"] >= 10:
            return [
                "background-color: #ffcccc"
            ] * len(row)

        elif row["Queued SQL"] > 0:
            return [
                "background-color: #fff3cd"
            ] * len(row)

        return [""] * len(row)


    st.dataframe(
        queue_df.style.apply(
            highlight_queue,
            axis=1
        ),
        use_container_width=True,
    )


else:

    st.info("No warehouse information available.")


# ------------------------------------------------------------
# KPI summary
# ------------------------------------------------------------

if not queue_df.empty:

    total_running = queue_df["Running SQL"].sum()

    total_queued = queue_df["Queued SQL"].sum()

    warehouses_with_queue = (
        queue_df["Queued SQL"] > 0
    ).sum()

    max_queue = queue_df["Queued SQL"].max()


    c1, c2, c3, c4 = st.columns(4)


    c1.metric(
        "Running SQL",
        int(total_running)
    )

    c2.metric(
        "Queued SQL",
        int(total_queued)
    )

    c3.metric(
        "Warehouses Queuing",
        int(warehouses_with_queue)
    )

    c4.metric(
        "Peak Current Queue",
        int(max_queue)
    )


# ============================================================
# 7-DAY HISTORICAL QUEUE TREND
# ============================================================

st.subheader("📈 Warehouse Queue Trend — Last 7 Days")


queue_history_query = """
SELECT
    DATE_TRUNC('hour', start_time) AS usage_hour,
    warehouse_name,

    AVG(avg_running) AS avg_running,

    AVG(avg_queued_load) AS avg_queued_load,

    AVG(avg_queued_provisioning)
        AS avg_queued_provisioning,

    AVG(avg_blocked)
        AS avg_blocked

FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY

WHERE start_time >= DATEADD(
    day,
    -7,
    CURRENT_TIMESTAMP()
)

GROUP BY
    DATE_TRUNC('hour', start_time),
    warehouse_name

ORDER BY
    usage_hour,
    warehouse_name
"""


queue_history_df = run_query(
    queue_history_query
)


if not queue_history_df.empty:

    selected_warehouse = st.selectbox(
        "Select warehouse",
        sorted(
            queue_history_df[
                "WAREHOUSE_NAME"
            ].unique()
        )
    )


    selected_df = queue_history_df[
        queue_history_df["WAREHOUSE_NAME"]
        == selected_warehouse
    ]


    fig = px.line(
        selected_df,
        x="USAGE_HOUR",
        y="AVG_QUEUED_LOAD",
        title=f"{selected_warehouse} - Queue Load",
        markers=True,
    )


    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Average Queued Load",
    )


    st.plotly_chart(
        fig,
        use_container_width=True,
    )


else:

    st.info(
        "No historical warehouse queue data available."
    )




# ============================================================
# QUERY QUEUE ANALYSIS
# ============================================================

st.subheader("🔎 SQL Queue Analysis — Last 7 Days")


query_queue_query = """
SELECT
    query_id,
    warehouse_name,
    user_name,
    role_name,

    start_time,

    total_elapsed_time / 1000
        AS elapsed_seconds,

    queued_overload_time / 1000
        AS queued_overload_seconds,

    queued_provisioning_time / 1000
        AS queued_provisioning_seconds,

    execution_time / 1000
        AS execution_seconds,

    bytes_scanned,

    rows_produced,

    query_text

FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY

WHERE start_time >= DATEADD(
    day,
    -7,
    CURRENT_TIMESTAMP()
)

AND queued_overload_time > 0

ORDER BY queued_overload_time DESC

LIMIT 100
"""


query_queue_df = run_query(
    query_queue_query
)


if not query_queue_df.empty:

    st.dataframe(
        query_queue_df,
        use_container_width=True,
    )

else:

    st.success(
        "No queries experienced overload queueing "
        "during the last 7 days."
    )
# ============================================================
# WAREHOUSE COST BREAKDOWN
# ============================================================

st.subheader("Warehouse Cost Breakdown")

if not warehouse_df.empty:

    warehouse_total = (
        warehouse_df
        .groupby("WAREHOUSE_NAME")
        .agg(
            TOTAL_CREDITS=("TOTAL_CREDITS", "sum"),
            COMPUTE_CREDITS=("COMPUTE_CREDITS", "sum"),
            CLOUD_SERVICE_CREDITS=(
                "CLOUD_SERVICE_CREDITS",
                "sum",
            ),
            QUERY_ATTRIBUTED_CREDITS=(
                "QUERY_ATTRIBUTED_CREDITS",
                "sum",
            ),
        )
        .reset_index()
        .sort_values(
            "TOTAL_CREDITS",
            ascending=False,
        )
    )

    st.dataframe(
        warehouse_total,
        use_container_width=True,
    )

    fig = px.bar(
        warehouse_total.head(20),
        x="WAREHOUSE_NAME",
        y="TOTAL_CREDITS",
        color="TOTAL_CREDITS",
        title="Top Warehouses by Credit Consumption",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )


# ============================================================
# WAREHOUSE IDLE COST
# ============================================================

st.subheader("Warehouse Idle Compute")

idle_query = f"""
SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED_COMPUTE)
        AS TOTAL_COMPUTE_CREDITS,
    SUM(
        COALESCE(CREDITS_ATTRIBUTED_COMPUTE_QUERIES, 0)
    ) AS QUERY_CREDITS,
    SUM(CREDITS_USED_COMPUTE)
      -
    SUM(
        COALESCE(CREDITS_ATTRIBUTED_COMPUTE_QUERIES, 0)
    ) AS ESTIMATED_IDLE_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
ORDER BY ESTIMATED_IDLE_CREDITS DESC
"""

idle_df = run_query(idle_query)

if not idle_df.empty:

    st.dataframe(
        idle_df,
        use_container_width=True,
    )

    fig = px.bar(
        idle_df.head(20),
        x="WAREHOUSE_NAME",
        y="ESTIMATED_IDLE_CREDITS",
        title="Estimated Warehouse Idle Credits",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )


# ============================================================
# 2. SERVERLESS COMPUTE
# ============================================================

st.header("2. Serverless Compute Usage")


serverless_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    SERVICE_TYPE,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
  AND SERVICE_TYPE NOT IN ('WAREHOUSE_METERING')
GROUP BY
    DATE_TRUNC('day', START_TIME),
    SERVICE_TYPE
ORDER BY
    USAGE_DATE
"""

serverless_df = run_query(serverless_query)


if not serverless_df.empty:

    fig = px.area(
        serverless_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        color="SERVICE_TYPE",
        title="Non-Warehouse / Serverless Credit Usage",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    serverless_summary = (
        serverless_df
        .groupby("SERVICE_TYPE")
        .agg(
            CREDITS_USED=("CREDITS_USED", "sum")
        )
        .reset_index()
        .sort_values(
            "CREDITS_USED",
            ascending=False,
        )
    )

    st.dataframe(
        serverless_summary,
        use_container_width=True,
    )


# ============================================================
# 3. MATERIALIZED VIEW
# ============================================================

st.header("3. Materialized View Cost")


mv_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME AS MATERIALIZED_VIEW,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME
ORDER BY
    USAGE_DATE
"""

mv_df = run_query(mv_query)

if not mv_df.empty:

    fig = px.bar(
        mv_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        color="MATERIALIZED_VIEW",
        title="Materialized View Refresh Cost",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        mv_df,
        use_container_width=True,
    )

else:

    st.info(
        "No materialized view refresh usage found."
    )


# ============================================================
# 4. AUTOMATIC CLUSTERING
# ============================================================

st.header("4. Automatic Clustering")


clustering_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    SUM(CREDITS_USED) AS CREDITS_USED,
    SUM(NUM_BYTES_RECLUSTERED) AS BYTES_RECLUSTERED,
    SUM(NUM_ROWS_RECLUSTERED) AS ROWS_RECLUSTERED
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME)
ORDER BY
    USAGE_DATE
"""

clustering_df = run_query(clustering_query)

if not clustering_df.empty:

    fig = px.bar(
        clustering_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        title="Automatic Clustering Credits",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        clustering_df,
        use_container_width=True,
    )


# ============================================================
# 5. SEARCH OPTIMIZATION
# ============================================================

st.header("5. Search Optimization")


search_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    DATABASE_NAME,
    SCHEMA_NAME,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    DATABASE_NAME,
    SCHEMA_NAME
ORDER BY
    USAGE_DATE
"""

search_df = run_query(search_query)

if not search_df.empty:

    fig = px.bar(
        search_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        title="Search Optimization Credits",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        search_df,
        use_container_width=True,
    )


# ============================================================
# 6. STORAGE TREND
# ============================================================

st.header("6. Storage Consumption")


storage_trend_query = f"""
SELECT
    USAGE_DATE,
    STORAGE_BYTES,
    STAGE_BYTES,
    FAILSAFE_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE USAGE_DATE >= CURRENT_DATE() - {days}
ORDER BY USAGE_DATE
"""

storage_trend_df = run_query(storage_trend_query)


if not storage_trend_df.empty:

    storage_plot = storage_trend_df.copy()

    storage_plot["TABLE_STORAGE_TB"] = (
        storage_plot["STORAGE_BYTES"]
        / (1024 ** 4)
    )

    storage_plot["STAGE_STORAGE_TB"] = (
        storage_plot["STAGE_BYTES"]
        / (1024 ** 4)
    )

    storage_plot["FAILSAFE_TB"] = (
        storage_plot["FAILSAFE_BYTES"]
        / (1024 ** 4)
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=storage_plot["USAGE_DATE"],
            y=storage_plot["TABLE_STORAGE_TB"],
            mode="lines",
            name="Table Storage",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=storage_plot["USAGE_DATE"],
            y=storage_plot["STAGE_STORAGE_TB"],
            mode="lines",
            name="Stage Storage",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=storage_plot["USAGE_DATE"],
            y=storage_plot["FAILSAFE_TB"],
            mode="lines",
            name="Fail-safe",
        )
    )

    fig.update_layout(
        title="Storage Consumption Trend",
        xaxis_title="Date",
        yaxis_title="TB",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )


# ============================================================
# 7. TABLE STORAGE BREAKDOWN
# ============================================================

st.header("7. Table Storage Breakdown")

table_storage_query = f"""
SELECT
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME,
    TABLE_NAME,

    ACTIVE_BYTES,
    TIME_TRAVEL_BYTES,
    FAILSAFE_BYTES,
    RETAINED_FOR_CLONE_BYTES,

    (
        ACTIVE_BYTES
        + TIME_TRAVEL_BYTES
        + FAILSAFE_BYTES
        + RETAINED_FOR_CLONE_BYTES
    ) AS TOTAL_STORAGE_BYTES

FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS

WHERE
    TABLE_DROPPED IS NULL

ORDER BY
    TOTAL_STORAGE_BYTES DESC

LIMIT 100
"""

table_storage_df = run_query(table_storage_query)

if not table_storage_df.empty:

    for column in [
        "ACTIVE_BYTES",
        "TIME_TRAVEL_BYTES",
        "FAILSAFE_BYTES",
        "RETAINED_FOR_CLONE_BYTES",
        "TOTAL_STORAGE_BYTES",
    ]:

        table_storage_df[
            column + "_GB"
        ] = (
            table_storage_df[column]
            / (1024 ** 3)
        )

    st.dataframe(
        table_storage_df,
        use_container_width=True,
    )


# ============================================================
# 8. TIME TRAVEL / FAILSAFE / CLONE ANALYSIS
# ============================================================

st.header("8. Time Travel / Fail-safe / Clone Cost Drivers")


if not table_storage_df.empty:

    storage_cost_df = table_storage_df.copy()

    tt = storage_cost_df["TIME_TRAVEL_BYTES"].sum()
    fs = storage_cost_df["FAILSAFE_BYTES"].sum()
    clone = storage_cost_df["RETAINED_FOR_CLONE_BYTES"].sum()
    active = storage_cost_df["ACTIVE_BYTES"].sum()

    a, b, c, d = st.columns(4)

    a.metric(
        "Active Storage",
        f"{bytes_to_tb(active):,.2f} TB",
    )

    b.metric(
        "Time Travel",
        f"{bytes_to_tb(tt):,.2f} TB",
    )

    c.metric(
        "Fail-safe",
        f"{bytes_to_tb(fs):,.2f} TB",
    )

    d.metric(
        "Retained for Clone",
        f"{bytes_to_tb(clone):,.2f} TB",
    )


# ============================================================
# 9. SNOWPIPE
# ============================================================

st.header("9. Snowpipe Usage")


pipe_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    PIPE_NAME,
    SUM(CREDITS_USED) AS CREDITS_USED,
    SUM(BYTES_INSERTED) AS BYTES_INSERTED,
    SUM(FILES_INSERTED) AS FILES_INSERTED
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    PIPE_NAME
ORDER BY
    USAGE_DATE
"""

pipe_df = run_query(pipe_query)

if not pipe_df.empty:

    fig = px.bar(
        pipe_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        color="PIPE_NAME",
        title="Snowpipe Credit Usage",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        pipe_df,
        use_container_width=True,
    )


# ============================================================
# 10. SERVERLESS TASKS
# ============================================================

st.header("10. Serverless Task Usage")


task_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    DATABASE_NAME,
    SCHEMA_NAME,
    TASK_NAME,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    DATABASE_NAME,
    SCHEMA_NAME,
    TASK_NAME
ORDER BY
    USAGE_DATE
"""

task_df = run_query(task_query)

if not task_df.empty:

    fig = px.bar(
        task_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        color="TASK_NAME",
        title="Serverless Task Credits",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        task_df,
        use_container_width=True,
    )


# ============================================================
# 11. REPLICATION
# ============================================================

st.header("11. Replication Usage")


replication_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    DATABASE_NAME,
    SUM(CREDITS_USED) AS CREDITS_USED,
    SUM(BYTES_TRANSFERRED) AS BYTES_TRANSFERRED
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_REPLICATION_USAGE_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    DATABASE_NAME
ORDER BY
    USAGE_DATE
"""

replication_df = run_query(replication_query)

if not replication_df.empty:

    fig = px.bar(
        replication_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        color="DATABASE_NAME",
        title="Database Replication Credits",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        replication_df,
        use_container_width=True,
    )


# ============================================================
# 12. ALL SERVICE TYPES
# ============================================================

st.header("12. Snowflake Service Cost Breakdown")


service_query = f"""
SELECT
    SERVICE_TYPE,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY SERVICE_TYPE
ORDER BY CREDITS_USED DESC
"""

service_df = run_query(service_query)

if not service_df.empty:

    fig = px.pie(
        service_df,
        names="SERVICE_TYPE",
        values="CREDITS_USED",
        title="Credit Consumption by Snowflake Service",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.dataframe(
        service_df,
        use_container_width=True,
    )


# ============================================================
# 13. QUERY COST ANALYSIS
# ============================================================

st.header("13. Query Cost / Usage Analysis")


query_cost_query = f"""
SELECT
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME,
    COUNT(*) AS QUERY_COUNT,
    SUM(TOTAL_ELAPSED_TIME) / 1000 AS ELAPSED_SECONDS,
    SUM(BYTES_SCANNED) AS BYTES_SCANNED,
    SUM(BYTES_WRITTEN) AS BYTES_WRITTEN
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME
ORDER BY
    BYTES_SCANNED DESC
LIMIT 100
"""

query_cost_df = run_query(query_cost_query)

if not query_cost_df.empty:

    query_cost_df[
        "BYTES_SCANNED_TB"
    ] = (
        query_cost_df["BYTES_SCANNED"]
        / (1024 ** 4)
    )

    st.dataframe(
        query_cost_df,
        use_container_width=True,
    )


# ============================================================
# 14. WAREHOUSE LOAD
# ============================================================

st.header("14. Warehouse Utilization / Queuing")


load_query = f"""
SELECT
    DATE_TRUNC('day', START_TIME) AS USAGE_DATE,
    WAREHOUSE_NAME,
    AVG(AVG_RUNNING) AS AVG_RUNNING,
    AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
    AVG(AVG_QUEUED_PROVISIONING) AS AVG_QUEUED_PROVISIONING
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE START_TIME >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('day', START_TIME),
    WAREHOUSE_NAME
ORDER BY
    USAGE_DATE
"""

load_df = run_query(load_query)

if not load_df.empty:

    fig = px.line(
        load_df,
        x="USAGE_DATE",
        y="AVG_QUEUED_LOAD",
        color="WAREHOUSE_NAME",
        title="Warehouse Queued Load",
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )


# ============================================================
# 15. FINOPS RECOMMENDATIONS
# ============================================================

st.header("15. FinOps Recommendations")

recommendations = []

if not idle_df.empty:

    high_idle = idle_df[
        idle_df["ESTIMATED_IDLE_CREDITS"] > 0
    ]

    if not high_idle.empty:

        recommendations.append(
            "Review warehouses with significant idle compute. "
            "Consider aggressive AUTO_SUSPEND settings."
        )


if not table_storage_df.empty:

    total_tt = table_storage_df[
        "TIME_TRAVEL_BYTES"
    ].sum()

    total_fs = table_storage_df[
        "FAILSAFE_BYTES"
    ].sum()

    total_clone = table_storage_df[
        "RETAINED_FOR_CLONE_BYTES"
    ].sum()

    if total_tt > 0:

        recommendations.append(
            "Time Travel storage is contributing to storage consumption. "
            "Review retention policies for large/high-churn tables."
        )

    if total_fs > 0:

        recommendations.append(
            "Fail-safe storage is present. "
            "Identify high-churn or frequently replaced tables."
        )

    if total_clone > 0:

        recommendations.append(
            "Retained-for-clone storage is present. "
            "Review long-lived clones and development environments."
        )


if not clustering_df.empty:

    clustering_cost = clustering_df[
        "CREDITS_USED"
    ].sum()

    if clustering_cost > 0:

        recommendations.append(
            "Automatic Clustering is consuming credits. "
            "Review clustering keys and whether the query workload "
            "actually benefits from clustering."
        )


if not mv_df.empty:

    mv_cost = mv_df[
        "CREDITS_USED"
    ].sum()

    if mv_cost > 0:

        recommendations.append(
            "Materialized View maintenance is consuming credits. "
            "Compare refresh cost against query performance benefits."
        )


if recommendations:

    for recommendation in recommendations:

        st.warning(
            f"💡 {recommendation}"
        )

else:

    st.success(
        "No immediate FinOps recommendations detected."
    )


# ============================================================
# FOOTER
# ============================================================

st.markdown("---")

st.caption(
    "Snowflake FinOps Dashboard | "
    "Built with Python + Streamlit + Snowflake ACCOUNT_USAGE"
)
