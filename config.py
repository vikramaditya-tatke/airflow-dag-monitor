#!/usr/bin/env python3
"""
Configuration file for Airflow DAG Task Monitor

Edit the variables below to customize the analysis parameters.
"""

# ============================================================================
# AIRFLOW CONFIGURATION
# ============================================================================

# DAG ID to monitor (REQUIRED)
DAG_ID = "mimecast_mta_to_datadog_etl_test"

# Airflow webserver URL
AIRFLOW_URL = "http://localhost:8080"

# ============================================================================
# ANALYSIS PARAMETERS
# ============================================================================

# Time period for analysis
# Options: 5m, 15m, 30m, 1h, 6h, 12h, 1d, 2d, 7d, 14d, 1mo
TIME_PERIOD = "1h"

# Task states to include in analysis
# Set to None to include all states, or specify a list like ["success", "failed"]
# Available states: success, failed, skipped, running, queued, up_for_retry, 
#                   up_for_reschedule, upstream_failed, deferred, removed
TASK_STATES = None

# Show only DAG runs with zero skipped tasks
SHOW_NO_SKIPPED_ONLY = True

# ============================================================================
# OUTPUT CONFIGURATION
# ============================================================================

# Save task data to CSV file
SAVE_DATA = False

# Save charts as HTML files
SAVE_CHARTS = True

# Output directory for saved files (charts and CSV)
OUTPUT_DIR = "/tmp"

# ============================================================================
# CHART CONFIGURATION
# ============================================================================

# Chart dimensions and styling
CHART_WIDTH = 800
CHART_HEIGHT = 400
SMALL_CHART_WIDTH = 500
SMALL_CHART_HEIGHT = 300

# Color scheme for task states
TASK_STATE_COLORS = {
    "success": "#2E7D32",        # Green
    "failed": "#D32F2F",         # Red
    "skipped": "#FF9800",        # Orange
    "running": "#1976D2",        # Blue
    "queued": "#757575",         # Gray
    "up_for_retry": "#9C27B0"    # Purple
}

# ============================================================================
# API CONFIGURATION
# ============================================================================

# Maximum number of DAG runs to fetch per API call
MAX_DAG_RUNS_LIMIT = 100

# Request timeout in seconds
REQUEST_TIMEOUT = 30

# Retry configuration for API calls
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds

# ============================================================================
# DISPLAY CONFIGURATION
# ============================================================================

# Console output formatting
CONSOLE_WIDTH = 60
ENABLE_EMOJI = True
VERBOSE_OUTPUT = True

# Date/time formatting for display
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"