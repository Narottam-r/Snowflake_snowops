"""
====================================================================
SnowDiag - A Snowflake Diagnosis Tool
====================================================================

Purpose
-------
Production Snowflake incident diagnostics.

SnowDiag connects to Snowflake using key-pair authentication and
executes a collection of diagnostic queries.

It evaluates current performance against a historical baseline and
flags abnormal metrics:

    GREEN  = Normal
    AMBER  = Above normal / investigate
    RED    = Significantly above normal / incident signal

The objective is NOT to replace Snowflake Support or Snowsight.

The objective is to provide an Incident Commander with a fast
"what is wrong right now?" view during a production incident.

Authentication
--------------
Snowflake Python Connector key-pair authentication.

Environment variables:

    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PRIVATE_KEY_PATH
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_ROLE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA

Example:

    export SNOWFLAKE_ACCOUNT="myorg-myaccount"
    export SNOWFLAKE_USER="SNOWDIAG_USER"
    export SNOWFLAKE_PRIVATE_KEY_PATH="/secure/rsa_key.p8"
    export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="..."
    export SNOWFLAKE_WAREHOUSE="SNOWDIAG_WH"
    export SNOWFLAKE_ROLE="SNOWDIAG_ROLE"

Run:

    streamlit run snowdiag.py

====================================================================
"""

import os
import time
import traceback
from datetime import datetime

import pandas as pd
import streamlit as st
import snowflake.connector


# ====================================================================
# APPLICATION CONFIGURATION
# ====================================================================

APP_NAME = "SnowDiag"
APP_SUBTITLE = "A Snowflake Diagnosis Tool"

CURRENT_WINDOW_MINUTES = 30
BASELINE_WINDOW_HOURS = 24

# Classification multipliers
#
# Current <= baseline * 1.5       GREEN
# Current <= baseline * 3.0       AMBER
# Current >  baseline * 3.0       RED

AMBER_MULTIPLIER = 1.5
RED_MULTIPLIER = 3.0

# Absolute thresholds for certain metrics
FAILURE_RATE_AMBER = 0.02
FAILURE_RATE_RED = 0.05

LOGIN_FAILURE_AMBER = 0.02
LOGIN_FAILURE_RED = 0.10

BLOCKING_AMBER_SECONDS = 5
BLOCKING_RED_SECONDS = 30

WAREHOUSE_LOAD_AMBER = 0.70
WAREHOUSE_LOAD_RED = 0.90

# How often Streamlit should refresh automatically
DEFAULT_REFRESH_SECONDS = 60


# ====================================================================
# PAGE CONFIG
# ====================================================================

st.set_page_config(
    page_title="SnowDiag - Snowflake Diagnosis Tool",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ====================================================================
# CUSTOM CSS
# ====================================================================

st.markdown(
    """
    <style>

    .snowdiag-title {
        font-size: 42px;
        font-weight: 800;
        color: #29B5E8;
        margin-bottom: 0px;
    }

    .snowdiag-subtitle {
        font-size: 18px;
        color: #777777;
        margin-bottom: 20px;
    }

    .metric-green {
        border-left: 8px solid #16A34A;
        background-color: #F0FDF4;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 10px;
    }

    .metric-amber {
        border-left: 8px solid #F59E0B;
        background-color: #FFFBEB;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 10px;
    }

    .metric-red {
        border-left: 8px solid #DC2626;
        background-color: #FEF2F2;
        padding: 18px;
        border-radius: 8px;
        margin-bottom: 12px;
    }

    .metric-name {
        font-size: 15px;
        font-weight: 700;
        color: #444444;
    }

    .metric-value-red {
        font-size: 36px;
        font-weight: 900;
        color: #DC2626;
    }

    .metric-value-amber {
        font-size: 30px;
        font-weight: 800;
        color: #D97706;
    }

    .metric-value-green {
        font-size: 28px;
        font-weight: 800;
        color: #15803D;
    }

    .status-red {
        color: #DC2626;
        font-weight: 900;
        font-size: 20px;
    }

    .status-amber {
        color: #D97706;
        font-weight: 900;
        font-size: 18px;
    }

    .status-green {
        color: #15803D;
        font-weight: 900;
        font-size: 18px;
    }

    .observation {
        font-size: 15px;
        color: #333333;
        margin-top: 5px;
    }

    .diagnostic-header {
        font-size: 23px;
        font-weight: 800;
        color: #333333;
        border-bottom: 2px solid #DDDDDD;
        padding-bottom: 5px;
        margin-top: 20px;
        margin-bottom: 15px;
    }

    .incident-box {
        background-color: #FEF2F2;
        border: 2px solid #DC2626;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
    }

    .healthy-box {
        background-color: #F0FDF4;
        border: 2px solid #16A34A;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ====================================================================
# ENVIRONMENT / CONFIG
# ====================================================================

def get_env(name, default=None):
    return os.getenv(name, default)


ACCOUNT = get_env("SNOWFLAKE_ACCOUNT")
USER = get_env("SNOWFLAKE_USER")
PRIVATE_KEY_PATH = get_env("SNOWFLAKE_PRIVATE_KEY_PATH")
PRIVATE_KEY_PASSPHRASE = get_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

WAREHOUSE = get_env("SNOWFLAKE_WAREHOUSE")
ROLE = get_env("SNOWFLAKE_ROLE")
DATABASE = get_env("SNOWFLAKE_DATABASE")
SCHEMA = get_env("SNOWFLAKE_SCHEMA", "PUBLIC")


# ====================================================================
# CONNECTION
# ====================================================================

@st.cache_resource(show_spinner=False)
def get_connection():

    if not ACCOUNT:
        raise RuntimeError("SNOWFLAKE_ACCOUNT is not configured.")

    if not USER:
        raise RuntimeError("SNOWFLAKE_USER is not configured.")

    if not PRIVATE_KEY_PATH:
        raise RuntimeError(
            "SNOWFLAKE_PRIVATE_KEY_PATH is not configured."
        )

    if not os.path.exists(PRIVATE_KEY_PATH):
        raise RuntimeError(
            f"Private key file does not exist: {PRIVATE_KEY_PATH}"
        )

    connection_params = {
        "account": ACCOUNT,
        "user": USER,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": PRIVATE_KEY_PATH,
        "warehouse": WAREHOUSE,
        "role": ROLE,
        "database": DATABASE,
        "schema": SCHEMA,

        # Connection timeout for incident diagnostics
        "login_timeout": 15,
        "network_timeout": 30,

        # Tag SnowDiag's own diagnostic queries
        "session_parameters": {
            "QUERY_TAG": "SnowDiag_Incident_Diagnostics"
        },
    }

    if PRIVATE_KEY_PASSPHRASE:
        connection_params[
            "private_key_file_pwd"
        ] = PRIVATE_KEY_PASSPHRASE

    return snowflake.connector.connect(**connection_params)


# ====================================================================
# SQL EXECUTION HELPERS
# ====================================================================

def execute_query(sql):

    conn = get_connection()

    cursor = None

    try:

        cursor = conn.cursor()

        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]

        rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=columns)

    finally:

        if cursor:
            cursor.close()


def execute_scalar(sql):

    df = execute_query(sql)

    if df.empty:
        return None

    return df.iloc[0, 0]


# ====================================================================
# CONNECTIVITY TEST
# ====================================================================

def run_connectivity_test():

    sql = """
    SELECT
        CURRENT_TIMESTAMP() AS CHECK_TIME,
        CURRENT_ACCOUNT() AS ACCOUNT_NAME,
        CURRENT_REGION() AS REGION,
        CURRENT_USER() AS USER_NAME,
        CURRENT_ROLE() AS ROLE_NAME,
        CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
        CURRENT_VERSION() AS SNOWFLAKE_VERSION
    """

    return execute_query(sql)


# ====================================================================
# QUERY HISTORY
# ====================================================================

def get_recent_query_history():

    sql = f"""
    SELECT
        QUERY_ID,
        QUERY_TEXT,
        DATABASE_NAME,
        SCHEMA_NAME,
        USER_NAME,
        ROLE_NAME,
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        EXECUTION_STATUS,
        ERROR_CODE,
        ERROR_MESSAGE,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME,
        COMPILATION_TIME,
        EXECUTION_TIME,
        QUEUED_PROVISIONING_TIME,
        QUEUED_REPAIR_TIME,
        QUEUED_OVERLOAD_TIME,
        TRANSACTION_BLOCKED_TIME,
        BYTES_SCANNED,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        ROWS_PRODUCED,
        QUERY_TAG
    FROM TABLE(
        INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START =>
                DATEADD(
                    'minute',
                    -{CURRENT_WINDOW_MINUTES},
                    CURRENT_TIMESTAMP()
                ),
            END_TIME_RANGE_END =>
                CURRENT_TIMESTAMP(),
            RESULT_LIMIT => 10000
        )
    )
    """

    return execute_query(sql)


# ====================================================================
# QUERY HISTORY BASELINE
# ====================================================================

def get_baseline_query_history():

    sql = f"""
    SELECT
        QUERY_ID,
        START_TIME,
        TOTAL_ELAPSED_TIME,
        COMPILATION_TIME,
        EXECUTION_TIME,
        QUEUED_PROVISIONING_TIME,
        QUEUED_OVERLOAD_TIME,
        TRANSACTION_BLOCKED_TIME,
        BYTES_SCANNED,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        EXECUTION_STATUS,
        WAREHOUSE_NAME
    FROM TABLE(
        INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START =>
                DATEADD(
                    'hour',
                    -{BASELINE_WINDOW_HOURS},
                    CURRENT_TIMESTAMP()
                ),
            END_TIME_RANGE_END =>
                DATEADD(
                    'minute',
                    -{CURRENT_WINDOW_MINUTES},
                    CURRENT_TIMESTAMP()
                ),
            RESULT_LIMIT => 10000
        )
    )
    """

    return execute_query(sql)


# ====================================================================
# STATISTICAL HELPERS
# ====================================================================

def percentile(series, p):

    series = pd.to_numeric(
        series,
        errors="coerce"
    ).dropna()

    if len(series) == 0:
        return None

    return float(series.quantile(p))


def safe_mean(series):

    series = pd.to_numeric(
        series,
        errors="coerce"
    ).dropna()

    if len(series) == 0:
        return None

    return float(series.mean())


def safe_sum(series):

    series = pd.to_numeric(
        series,
        errors="coerce"
    ).dropna()

    if len(series) == 0:
        return 0

    return float(series.sum())


# ====================================================================
# METRIC CLASSIFICATION
# ====================================================================

def classify_ratio(
    current,
    baseline,
    name,
    unit="",
    red_multiplier=RED_MULTIPLIER,
    amber_multiplier=AMBER_MULTIPLIER,
):

    if current is None:

        return {
            "name": name,
            "value": None,
            "baseline": baseline,
            "ratio": None,
            "status": "UNKNOWN",
            "unit": unit,
            "message": "No data available",
            "observation": "Unable to evaluate this metric.",
        }

    if baseline is None or baseline <= 0:

        return {
            "name": name,
            "value": current,
            "baseline": baseline,
            "ratio": None,
            "status": "UNKNOWN",
            "unit": unit,
            "message": "Baseline unavailable",
            "observation": (
                "A historical baseline could not be established."
            ),
        }

    ratio = current / baseline

    if ratio >= red_multiplier:

        status = "RED"
        message = "HIGHER THAN USUAL"

        observation = (
            f"{name} is {ratio:.1f}x above the normal baseline."
        )

    elif ratio >= amber_multiplier:

        status = "AMBER"
        message = "ABOVE NORMAL"

        observation = (
            f"{name} is {ratio:.1f}x above the normal baseline."
        )

    else:

        status = "GREEN"
        message = "NORMAL"

        observation = (
            f"{name} is within the normal range."
        )

    return {
        "name": name,
        "value": current,
        "baseline": baseline,
        "ratio": ratio,
        "status": status,
        "unit": unit,
        "message": message,
        "observation": observation,
    }


# ====================================================================
# QUERY PERFORMANCE DIAGNOSTICS
# ====================================================================

def diagnose_query_performance(
    current_df,
    baseline_df
):

    diagnostics = []

    # --------------------------------------------------------------
    # P95 TOTAL RUNTIME
    # --------------------------------------------------------------

    current_p95 = percentile(
        current_df["TOTAL_ELAPSED_TIME"],
        0.95
    )

    baseline_p95 = percentile(
        baseline_df["TOTAL_ELAPSED_TIME"],
        0.95
    )

    diagnostics.append(
        classify_ratio(
            current_p95,
            baseline_p95,
            "P95 Query Runtime",
            "sec",
        )
    )

    # --------------------------------------------------------------
    # P95 COMPILATION
    # --------------------------------------------------------------

    current_p95 = percentile(
        current_df["COMPILATION_TIME"],
        0.95
    )

    baseline_p95 = percentile(
        baseline_df["COMPILATION_TIME"],
        0.95
    )

    diagnostics.append(
        classify_ratio(
            current_p95,
            baseline_p95,
            "P95 Compilation Time",
            "sec",
        )
    )

    # --------------------------------------------------------------
    # P95 EXECUTION
    # --------------------------------------------------------------

    current_p95 = percentile(
        current_df["EXECUTION_TIME"],
        0.95
    )

    baseline_p95 = percentile(
        baseline_df["EXECUTION_TIME"],
        0.95
    )

    diagnostics.append(
        classify_ratio(
            current_p95,
            baseline_p95,
            "P95 Execution Time",
            "sec",
        )
    )

    # --------------------------------------------------------------
    # OVERLOAD QUEUE
    # --------------------------------------------------------------

    current_avg = safe_mean(
        current_df["QUEUED_OVERLOAD_TIME"]
    )

    baseline_avg = safe_mean(
        baseline_df["QUEUED_OVERLOAD_TIME"]
    )

    diagnostics.append(
        classify_ratio(
            current_avg,
            baseline_avg,
            "Average Overload Queue",
            "sec/query",
        )
    )

    # --------------------------------------------------------------
    # PROVISIONING QUEUE
    # --------------------------------------------------------------

    current_avg = safe_mean(
        current_df["QUEUED_PROVISIONING_TIME"]
    )

    baseline_avg = safe_mean(
        baseline_df["QUEUED_PROVISIONING_TIME"]
    )

    diagnostics.append(
        classify_ratio(
            current_avg,
            baseline_avg,
            "Average Provisioning Queue",
            "sec/query",
        )
    )

    # --------------------------------------------------------------
    # TRANSACTION BLOCKING
    # --------------------------------------------------------------

    current_avg = safe_mean(
        current_df["TRANSACTION_BLOCKED_TIME"]
    )

    baseline_avg = safe_mean(
        baseline_df["TRANSACTION_BLOCKED_TIME"]
    )

    result = classify_ratio(
        current_avg,
        baseline_avg,
        "Transaction Blocking",
        "sec/query",
    )

    # Override with absolute thresholds
    if current_avg is not None:

        seconds = current_avg / 1000

        if seconds >= BLOCKING_RED_SECONDS:

            result["status"] = "RED"
            result["message"] = "HIGH BLOCKING"
            result["observation"] = (
                f"Average transaction blocking is "
                f"{seconds:.1f} seconds/query."
            )

        elif seconds >= BLOCKING_AMBER_SECONDS:

            result["status"] = "AMBER"
            result["message"] = "ABOVE NORMAL"
            result["observation"] = (
                f"Average transaction blocking is "
                f"{seconds:.1f} seconds/query."
            )

    diagnostics.append(result)

    # --------------------------------------------------------------
    # REMOTE SPILL
    # --------------------------------------------------------------

    current_spill = safe_sum(
        current_df["BYTES_SPILLED_TO_REMOTE_STORAGE"]
    )

    baseline_spill = safe_sum(
        baseline_df["BYTES_SPILLED_TO_REMOTE_STORAGE"]
    )

    diagnostics.append(
        classify_ratio(
            current_spill,
            baseline_spill,
            "Remote Spill",
            "bytes",
        )
    )

    # --------------------------------------------------------------
    # LOCAL SPILL
    # --------------------------------------------------------------

    current_spill = safe_sum(
        current_df["BYTES_SPILLED_TO_LOCAL_STORAGE"]
    )

    baseline_spill = safe_sum(
        baseline_df["BYTES_SPILLED_TO_LOCAL_STORAGE"]
    )

    diagnostics.append(
        classify_ratio(
            current_spill,
            baseline_spill,
            "Local Spill",
            "bytes",
        )
    )

    # --------------------------------------------------------------
    # BYTES SCANNED
    # --------------------------------------------------------------

    current_scan = safe_sum(
        current_df["BYTES_SCANNED"]
    )

    baseline_scan = safe_sum(
        baseline_df["BYTES_SCANNED"]
    )

    diagnostics.append(
        classify_ratio(
            current_scan,
            baseline_scan,
            "Data Scanned",
            "bytes",
        )
    )

    return diagnostics


# ====================================================================
# FAILURE RATE
# ====================================================================

def diagnose_failure_rate(
    current_df,
    baseline_df
):

    if len(current_df) == 0:

        return {
            "name": "Query Failure Rate",
            "value": None,
            "baseline": None,
            "ratio": None,
            "status": "UNKNOWN",
            "unit": "%",
            "message": "No data",
            "observation": "No recent queries were returned.",
        }

    current_failures = (
        current_df["EXECUTION_STATUS"]
        .astype(str)
        .str.upper()
        .ne("SUCCESS")
        .sum()
    )

    current_rate = (
        current_failures /
        len(current_df)
    )

    baseline_failures = (
        baseline_df["EXECUTION_STATUS"]
        .astype(str)
        .str.upper()
        .ne("SUCCESS")
        .sum()
    )

    baseline_rate = (
        baseline_failures /
        len(baseline_df)
        if len(baseline_df) > 0
        else None
    )

    if current_rate >= FAILURE_RATE_RED:

        status = "RED"
        message = "HIGH FAILURE RATE"

    elif current_rate >= FAILURE_RATE_AMBER:

        status = "AMBER"
        message = "ABOVE NORMAL"

    else:

        status = "GREEN"
        message = "NORMAL"

    if baseline_rate and current_rate > baseline_rate * 3:

        status = "RED"
        message = "HIGHER THAN USUAL"

    observation = (
        f"{current_rate * 100:.2f}% of recent queries are failing."
    )

    return {
        "name": "Query Failure Rate",
        "value": current_rate,
        "baseline": baseline_rate,
        "ratio": (
            current_rate / baseline_rate
            if baseline_rate and baseline_rate > 0
            else None
        ),
        "status": status,
        "unit": "%",
        "message": message,
        "observation": observation,
    }


# ====================================================================
# WAREHOUSE LOAD
# ====================================================================

def get_warehouse_load():

    sql = """
    SELECT
        WAREHOUSE_NAME,
        START_TIME,
        END_TIME,
        AVG_RUNNING,
        AVG_QUEUED_LOAD,
        AVG_QUEUED_PROVISIONING,
        AVG_BLOCKED
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD(
        'hour',
        -6,
        CURRENT_TIMESTAMP()
    )
    ORDER BY START_TIME DESC
    """

    return execute_query(sql)


# ====================================================================
# LOGIN HISTORY
# ====================================================================

def get_login_failures():

    sql = """
    SELECT
        EVENT_TIMESTAMP,
        EVENT_TYPE,
        USER_NAME,
        CLIENT_IP,
        REPORTED_CLIENT_TYPE,
        IS_SUCCESS,
        ERROR_CODE,
        ERROR_MESSAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
    WHERE EVENT_TIMESTAMP >= DATEADD(
        'hour',
        -2,
        CURRENT_TIMESTAMP()
    )
    ORDER BY EVENT_TIMESTAMP DESC
    """

    return execute_query(sql)


def diagnose_login_failures(login_df):

    if login_df.empty:

        return {
            "name": "Login Failure Rate",
            "value": None,
            "baseline": None,
            "ratio": None,
            "status": "UNKNOWN",
            "unit": "%",
            "message": "No data",
            "observation": "Login history did not return data.",
        }

    total = len(login_df)

    failed = (
        login_df["IS_SUCCESS"]
        .astype(str)
        .str.upper()
        .isin(["NO", "FALSE"])
        .sum()
    )

    rate = failed / total

    if rate >= LOGIN_FAILURE_RED:

        status = "RED"
        message = "HIGH LOGIN FAILURE RATE"

    elif rate >= LOGIN_FAILURE_AMBER:

        status = "AMBER"
        message = "ABOVE NORMAL"

    else:

        status = "GREEN"
        message = "NORMAL"

    return {
        "name": "Login Failure Rate",
        "value": rate,
        "baseline": None,
        "ratio": None,
        "status": status,
        "unit": "%",
        "message": message,
        "observation": (
            f"{failed:,} of {total:,} login attempts failed."
        ),
    }


# ====================================================================
# WAREHOUSE EVENTS
# ====================================================================

def get_warehouse_events():

    sql = """
    SELECT
        TIMESTAMP,
        WAREHOUSE_NAME,
        CLUSTER_NUMBER,
        EVENT_NAME,
        EVENT_REASON,
        EVENT_STATE,
        USER_NAME,
        ROLE_NAME,
        QUERY_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY
    WHERE TIMESTAMP >= DATEADD(
        'hour',
        -6,
        CURRENT_TIMESTAMP()
    )
    ORDER BY TIMESTAMP DESC
    """

    return execute_query(sql)


# ====================================================================
# DIAGNOSTIC ENGINE
# ====================================================================

def run_all_diagnostics():

    results = {}

    # --------------------------------------------------------------
    # Connectivity
    # --------------------------------------------------------------

    results["connectivity"] = run_connectivity_test()

    # --------------------------------------------------------------
    # Query history
    # --------------------------------------------------------------

    current_df = get_recent_query_history()

    baseline_df = get_baseline_query_history()

    results["query_history"] = current_df

    results["baseline"] = baseline_df

    # --------------------------------------------------------------
    # Query diagnostics
    # --------------------------------------------------------------

    diagnostics = diagnose_query_performance(
        current_df,
        baseline_df
    )

    diagnostics.append(
        diagnose_failure_rate(
            current_df,
            baseline_df
        )
    )

    results["diagnostics"] = diagnostics

    # --------------------------------------------------------------
    # Warehouse
    # --------------------------------------------------------------

    try:
        results["warehouse_load"] = get_warehouse_load()

    except Exception as exc:

        results["warehouse_load"] = pd.DataFrame()

        results["warehouse_error"] = str(exc)

    # --------------------------------------------------------------
    # Login
    # --------------------------------------------------------------

    try:

        login_df = get_login_failures()

        results["login_history"] = login_df

        results["login_diagnostic"] = (
            diagnose_login_failures(login_df)
        )

    except Exception as exc:

        results["login_history"] = pd.DataFrame()

        results["login_diagnostic"] = {
            "name": "Login Failure Rate",
            "value": None,
            "baseline": None,
            "ratio": None,
            "status": "UNKNOWN",
            "unit": "%",
            "message": "Unable to query LOGIN_HISTORY",
            "observation": str(exc),
        }

    # --------------------------------------------------------------
    # Warehouse events
    # --------------------------------------------------------------

    try:

        results["warehouse_events"] = (
            get_warehouse_events()
        )

    except Exception as exc:

        results["warehouse_events"] = pd.DataFrame()

        results["warehouse_events_error"] = str(exc)

    return results


# ====================================================================
# FORMATTING
# ====================================================================

def format_value(value, unit):

    if value is None:
        return "N/A"

    if unit == "sec":

        return f"{value / 1000:.2f} sec"

    if unit == "sec/query":

        return f"{value / 1000:.2f} sec/query"

    if unit == "bytes":

        value = float(value)

        if value >= 1024**4:
            return f"{value / 1024**4:.2f} TB"

        if value >= 1024**3:
            return f"{value / 1024**3:.2f} GB"

        if value >= 1024**2:
            return f"{value / 1024**2:.2f} MB"

        return f"{value:,.0f} bytes"

    if unit == "%":

        return f"{value * 100:.2f}%"

    return f"{value:,.2f}"


def format_baseline(value, unit):

    if value is None:
        return "N/A"

    return format_value(value, unit)


# ====================================================================
# METRIC CARD
# ====================================================================

def render_metric(metric):

    status = metric["status"]

    if status == "RED":

        css_class = "metric-red"
        value_class = "metric-value-red"
        status_class = "status-red"
        icon = "🔴"

    elif status == "AMBER":

        css_class = "metric-amber"
        value_class = "metric-value-amber"
        status_class = "status-amber"
        icon = "🟡"

    elif status == "GREEN":

        css_class = "metric-green"
        value_class = "metric-value-green"
        status_class = "status-green"
        icon = "🟢"

    else:

        css_class = "metric-amber"
        value_class = "metric-value-amber"
        status_class = "status-amber"
        icon = "⚪"

    current = format_value(
        metric["value"],
        metric["unit"]
    )

    baseline = format_baseline(
        metric["baseline"],
        metric["unit"]
    )

    ratio = metric.get("ratio")

    ratio_text = ""

    if ratio:

        ratio_text = f" | Deviation: {ratio:.1f}x"

    st.markdown(
        f"""
        <div class="{css_class}">

            <div class="metric-name">
                {icon} {metric["name"]}
            </div>

            <div class="{value_class}">
                {current}
            </div>

            <div class="{status_class}">
                {metric["message"]}
            </div>

            <div class="observation">
                <b>Normal baseline:</b> {baseline}
                {ratio_text}
            </div>

            <div class="observation">
                {metric["observation"]}
            </div>

        </div>
        """,
        unsafe_allow_html=True
    )


# ====================================================================
# OVERALL STATUS
# ====================================================================

def calculate_overall_status(diagnostics):

    statuses = [
        d["status"]
        for d in diagnostics
        if d["status"] != "UNKNOWN"
    ]

    if "RED" in statuses:
        return "RED"

    if "AMBER" in statuses:
        return "AMBER"

    return "GREEN"


def render_overall_status(diagnostics):

    status = calculate_overall_status(
        diagnostics
    )

    red = [
        d for d in diagnostics
        if d["status"] == "RED"
    ]

    amber = [
        d for d in diagnostics
        if d["status"] == "AMBER"
    ]

    if status == "RED":

        st.markdown(
            """
            <div class="incident-box">

            <div style="
                font-size:32px;
                font-weight:900;
                color:#DC2626;
            ">
            🔴 SNOWFLAKE PRODUCTION PERFORMANCE DEGRADATION
            </div>

            <p>
            One or more critical diagnostic metrics are
            significantly outside the normal baseline.
            </p>

            </div>
            """,
            unsafe_allow_html=True
        )

        st.markdown("### 🔴 Most Significant Observations")

        for d in red[:8]:

            st.markdown(
                f"**🔴 {d['name']}** — {d['observation']}"
            )

    elif status == "AMBER":

        st.markdown(
            """
            <div class="metric-amber">

            <div style="
                font-size:30px;
                font-weight:900;
                color:#D97706;
            ">
            🟡 SNOWFLAKE PERFORMANCE DEGRADATION
            </div>

            <p>
            Some metrics are above their normal baseline.
            </p>

            </div>
            """,
            unsafe_allow_html=True
        )

    else:

        st.markdown(
            """
            <div class="healthy-box">

            <div style="
                font-size:30px;
                font-weight:900;
                color:#15803D;
            ">
            🟢 SNOWFLAKE PERFORMANCE APPEARS NORMAL
            </div>

            <p>
            No major diagnostic deviations were detected.
            </p>

            </div>
            """,
            unsafe_allow_html=True
        )


# ====================================================================
# TOP FAILED QUERIES
# ====================================================================

def render_failed_queries(df):

    st.markdown(
        '<div class="diagnostic-header">Failed Queries</div>',
        unsafe_allow_html=True
    )

    if df.empty:

        st.success(
            "No failed queries detected in the current window."
        )

        return

    failed = df[
        df["EXECUTION_STATUS"]
        .astype(str)
        .str.upper()
        .ne("SUCCESS")
    ].copy()

    if failed.empty:

        st.success(
            "No failed queries detected in the current window."
        )

        return

    columns = [
        "QUERY_ID",
        "USER_NAME",
        "WAREHOUSE_NAME",
        "ERROR_CODE",
        "ERROR_MESSAGE",
        "START_TIME",
        "QUERY_TEXT",
    ]

    columns = [
        c for c in columns
        if c in failed.columns
    ]

    st.dataframe(
        failed[columns].head(50),
        use_container_width=True,
        hide_index=True,
    )


# ====================================================================
# LONG RUNNING QUERIES
# ====================================================================

def render_long_running_queries(df):

    st.markdown(
        '<div class="diagnostic-header">Longest Running Queries</div>',
        unsafe_allow_html=True
    )

    if df.empty:

        st.info("No query history available.")

        return

    display_df = df.copy()

    display_df["ELAPSED_SEC"] = (
        display_df["TOTAL_ELAPSED_TIME"] / 1000
    )

    display_df = display_df.sort_values(
        "ELAPSED_SEC",
        ascending=False
    )

    columns = [
        "QUERY_ID",
        "USER_NAME",
        "WAREHOUSE_NAME",
        "ELAPSED_SEC",
        "COMPILATION_TIME",
        "EXECUTION_TIME",
        "QUEUED_OVERLOAD_TIME",
        "QUEUED_PROVISIONING_TIME",
        "QUERY_TEXT",
    ]

    columns = [
        c for c in columns
        if c in display_df.columns
    ]

    st.dataframe(
        display_df[columns].head(25),
        use_container_width=True,
        hide_index=True,
    )


# ====================================================================
# WAREHOUSE LOAD DISPLAY
# ====================================================================

def render_warehouse_load(df):

    st.markdown(
        '<div class="diagnostic-header">Warehouse Load</div>',
        unsafe_allow_html=True
    )

    if df.empty:

        st.warning(
            "Warehouse load history is unavailable."
        )

        return

    latest = (
        df.sort_values("START_TIME")
        .groupby("WAREHOUSE_NAME")
        .tail(1)
    )

    for _, row in latest.iterrows():

        warehouse = row["WAREHOUSE_NAME"]

        running = row["AVG_RUNNING"] or 0
        queued = row["AVG_QUEUED_LOAD"] or 0
        provisioning = (
            row["AVG_QUEUED_PROVISIONING"] or 0
        )
        blocked = row["AVG_BLOCKED"] or 0

        if queued >= WAREHOUSE_LOAD_RED:

            status = "RED"

        elif queued >= WAREHOUSE_LOAD_AMBER:

            status = "AMBER"

        else:

            status = "GREEN"

        if status == "RED":

            st.error(
                f"🔴 {warehouse}: "
                f"High warehouse queue load "
                f"({queued:.1%})"
            )

        elif status == "AMBER":

            st.warning(
                f"🟡 {warehouse}: "
                f"Elevated warehouse queue load "
                f"({queued:.1%})"
            )

        else:

            st.success(
                f"🟢 {warehouse}: "
                f"Warehouse queue load normal "
                f"({queued:.1%})"
            )

        cols = st.columns(4)

        cols[0].metric(
            "Running",
            f"{running:.2f}"
        )

        cols[1].metric(
            "Queued Load",
            f"{queued:.2%}"
        )

        cols[2].metric(
            "Provisioning",
            f"{provisioning:.2%}"
        )

        cols[3].metric(
            "Blocked",
            f"{blocked:.2%}"
        )


# ====================================================================
# WAREHOUSE EVENTS DISPLAY
# ====================================================================

def render_warehouse_events(df):

    st.markdown(
        '<div class="diagnostic-header">Recent Warehouse Events</div>',
        unsafe_allow_html=True
    )

    if df.empty:

        st.info("No warehouse events returned.")

        return

    st.dataframe(
        df.head(100),
        use_container_width=True,
        hide_index=True,
    )


# ====================================================================
# LOGIN DISPLAY
# ====================================================================

def render_login_status(metric):

    st.markdown(
        '<div class="diagnostic-header">Authentication</div>',
        unsafe_allow_html=True
    )

    render_metric(metric)


# ====================================================================
# CONNECTION DETAILS
# ====================================================================

def render_connection_details(df):

    if df.empty:
        return

    row = df.iloc[0]

    st.markdown(
        '<div class="diagnostic-header">Connection Health</div>',
        unsafe_allow_html=True
    )

    cols = st.columns(6)

    cols[0].metric(
        "Account",
        str(row["ACCOUNT_NAME"])
    )

    cols[1].metric(
        "Region",
        str(row["REGION"])
    )

    cols[2].metric(
        "User",
        str(row["USER_NAME"])
    )

    cols[3].metric(
        "Role",
        str(row["ROLE_NAME"])
    )

    cols[4].metric(
        "Warehouse",
        str(row["WAREHOUSE_NAME"])
    )

    cols[5].metric(
        "Version",
        str(row["SNOWFLAKE_VERSION"])
    )


# ====================================================================
# SIDEBAR
# ====================================================================

def render_sidebar():

    st.sidebar.markdown(
        "## ❄️ SnowDiag"
    )

    st.sidebar.caption(
        "A Snowflake Diagnosis Tool"
    )

    st.sidebar.markdown("---")

    refresh = st.sidebar.slider(
        "Auto refresh (seconds)",
        min_value=30,
        max_value=600,
        value=DEFAULT_REFRESH_SECONDS,
        step=30,
    )

    st.sidebar.markdown("---")

    st.sidebar.markdown(
        f"""
        **Diagnostic window**

        Recent queries:
        `{CURRENT_WINDOW_MINUTES} minutes`

        Baseline:
        `{BASELINE_WINDOW_HOURS} hours`

        **Threshold model**

        GREEN: < 1.5x baseline

        AMBER: 1.5x–3x baseline

        RED: > 3x baseline
        """
    )

    st.sidebar.markdown("---")

    if st.sidebar.button(
        "🔄 Run Diagnostics Now",
        use_container_width=True,
    ):

        st.cache_resource.clear()
        st.rerun()

    return refresh


# ====================================================================
# MAIN
# ====================================================================

def main():

    refresh_seconds = render_sidebar()

    st.markdown(
        '<div class="snowdiag-title">'
        '❄️ SnowDiag'
        '</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="snowdiag-subtitle">'
        'A Snowflake Diagnosis Tool'
        '</div>',
        unsafe_allow_html=True
    )

    st.caption(
        f"Last diagnostic run: "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # --------------------------------------------------------------
    # Execute diagnostics
    # --------------------------------------------------------------

    try:

        with st.spinner(
            "Running Snowflake diagnostics..."
        ):

            results = run_all_diagnostics()

    except Exception as exc:

        st.error(
            "❌ SnowDiag could not connect to Snowflake."
        )

        st.exception(exc)

        st.stop()

    # --------------------------------------------------------------
    # Connection
    # --------------------------------------------------------------

    render_connection_details(
        results["connectivity"]
    )

    # --------------------------------------------------------------
    # Overall diagnosis
    # --------------------------------------------------------------

    diagnostics = results["diagnostics"]

    login_diagnostic = results.get(
        "login_diagnostic"
    )

    if login_diagnostic:

        diagnostics.append(
            login_diagnostic
        )

    render_overall_status(
        diagnostics
    )

    # --------------------------------------------------------------
    # Metrics
    # --------------------------------------------------------------

    st.markdown(
        '<div class="diagnostic-header">'
        '🚨 Incident Diagnostics'
        '</div>',
        unsafe_allow_html=True
    )

    # Split diagnostics into two columns
    left, right = st.columns(2)

    for index, metric in enumerate(diagnostics):

        target = (
            left
            if index % 2 == 0
            else right
        )

        with target:

            render_metric(metric)

    # --------------------------------------------------------------
    # Warehouse
    # --------------------------------------------------------------

    render_warehouse_load(
        results["warehouse_load"]
    )

    # --------------------------------------------------------------
    # Queries
    # --------------------------------------------------------------

    st.markdown("---")

    tabs = st.tabs(
        [
            "🔴 Failed Queries",
            "⏱ Long Running",
            "🏭 Warehouse Events",
            "📊 Raw Query History",
        ]
    )

    with tabs[0]:

        render_failed_queries(
            results["query_history"]
        )

    with tabs[1]:

        render_long_running_queries(
            results["query_history"]
        )

    with tabs[2]:

        render_warehouse_events(
            results["warehouse_events"]
        )

    with tabs[3]:

        st.dataframe(
            results["query_history"],
            use_container_width=True,
            hide_index=True,
        )

    # --------------------------------------------------------------
    # Footer
    # --------------------------------------------------------------

    st.markdown("---")

    st.caption(
        "SnowDiag is a diagnostic aid. "
        "Metrics should be correlated with Snowflake "
        "platform status, workload characteristics, "
        "and application behaviour before determining root cause."
    )

    # --------------------------------------------------------------
    # Auto refresh
    # --------------------------------------------------------------

    time.sleep(refresh_seconds)

    st.rerun()


# ====================================================================
# ENTRY POINT
# ====================================================================

if __name__ == "__main__":

    main()
