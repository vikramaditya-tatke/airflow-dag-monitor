#!/usr/bin/env python3
"""
Core module for Airflow DAG Task Monitor

Contains all classes and utility functions for monitoring and analyzing
Airflow DAG task statuses.
"""

import sys
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Dict, Any, Optional
import requests
import polars as pl
import altair as alt


class TaskState(Enum):
    """Valid Airflow task states"""
    SUCCESS = "success"
    FAILED = "failed" 
    SKIPPED = "skipped"
    RUNNING = "running"
    QUEUED = "queued"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    DEFERRED = "deferred"
    REMOVED = "removed"


class TimePeriod(Enum):
    """Valid time periods for analysis"""
    LAST_5_MIN = ("5m", timedelta(minutes=5))
    LAST_15_MIN = ("15m", timedelta(minutes=15))  
    LAST_30_MIN = ("30m", timedelta(minutes=30))
    LAST_1_HOUR = ("1h", timedelta(hours=1))
    LAST_6_HOURS = ("6h", timedelta(hours=6))
    LAST_12_HOURS = ("12h", timedelta(hours=12))
    LAST_1_DAY = ("1d", timedelta(days=1))
    LAST_2_DAYS = ("2d", timedelta(days=2))
    LAST_7_DAYS = ("7d", timedelta(days=7))
    LAST_14_DAYS = ("14d", timedelta(days=14))
    LAST_1_MONTH = ("1mo", timedelta(days=30))
    
    def __init__(self, label: str, delta: timedelta):
        self.label = label
        self.delta = delta


class AirflowClient:
    """
    Client for interacting with Airflow REST API.
    Handles authentication and API calls to fetch DAG runs and task instances.
    """
    
    def __init__(self, base_url: str = "http://localhost:8080", timeout: int = 30):
        """
        Initialize the Airflow API client.
        
        Args:
            base_url: Base URL for the Airflow webserver
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"accept": "application/json"})
    
    def get_dag_runs(self, dag_id: str, start_date_gte: str, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch DAG runs for a specific DAG within a time range.
        
        Args:
            dag_id: The DAG identifier
            start_date_gte: Start date filter in ISO format
            limit: Maximum number of runs to fetch
            
        Returns:
            Dictionary containing DAG runs data
        """
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
        params = {
            "start_date_gte": start_date_gte,
            "limit": limit,
            "order_by": "-start_date"
        }
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Error fetching DAG runs: {e}")
            return {"dag_runs": []}
    
    def get_task_instances(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        Fetch task instances for a specific DAG run.
        
        Args:
            dag_id: The DAG identifier  
            dag_run_id: The DAG run identifier
            
        Returns:
            Dictionary containing task instances data
        """
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Error fetching task instances for {dag_run_id}: {e}")
            return {"task_instances": []}


class TaskDataFetcher:
    """
    Responsible for fetching and processing task data from Airflow API.
    Converts raw API responses into structured Polars DataFrames.
    """
    
    def __init__(self, client: AirflowClient):
        """
        Initialize with an Airflow client.
        
        Args:
            client: AirflowClient instance for API communication
        """
        self.client = client
    
    def fetch_task_data(
        self, 
        dag_id: str, 
        time_period: TimePeriod,
        task_states: List[TaskState]
    ) -> pl.DataFrame:
        """
        Fetch and process task data for analysis.
        
        Args:
            dag_id: The DAG to analyze
            time_period: Time period for data collection
            task_states: List of task states to filter by
            
        Returns:
            Polars DataFrame with task data
        """
        # Calculate time range
        end_time = datetime.utcnow()
        start_time = end_time - time_period.delta
        start_date_str = start_time.isoformat() + "Z"
        
        print(f"📊 Fetching DAG runs from {start_date_str} to {end_time.isoformat()}Z")
        
        # Get DAG runs
        dag_runs_data = self.client.get_dag_runs(dag_id, start_date_str)
        dag_runs = dag_runs_data.get("dag_runs", [])
        
        if not dag_runs:
            print("ℹ️  No DAG runs found in the specified time period")
            return pl.DataFrame()
        
        print(f"✅ Found {len(dag_runs)} DAG runs")
        
        # Collect all task instances
        all_tasks = []
        state_filter = {state.value for state in task_states} if task_states else set()
        
        for dag_run in dag_runs:
            dag_run_id = dag_run["dag_run_id"]
            run_type = dag_run["run_type"]
            logical_date = dag_run["logical_date"]
            dag_start_date = dag_run["start_date"]
            dag_end_date = dag_run["end_date"]
            dag_state = dag_run["state"]
            
            # Get task instances for this DAG run
            task_data = self.client.get_task_instances(dag_id, dag_run_id)
            task_instances = task_data.get("task_instances", [])
            
            # Filter and collect tasks
            for task in task_instances:
                if not task_states or task["state"] in state_filter:
                    all_tasks.append({
                        "dag_id": dag_id,
                        "dag_run_id": dag_run_id,
                        "run_type": run_type,
                        "logical_date": logical_date,
                        "dag_start_date": dag_start_date,
                        "dag_end_date": dag_end_date,
                        "dag_state": dag_state,
                        "task_id": task["task_id"],
                        "task_state": task["state"],
                        "task_start_date": task.get("start_date"),
                        "task_end_date": task.get("end_date"),
                        "duration": task.get("duration", 0.0),
                        "try_number": task.get("try_number", 0),
                        "max_tries": task.get("max_tries", 0),
                        "operator": task.get("operator", ""),
                        "priority_weight": task.get("priority_weight", 0)
                    })
        
        if not all_tasks:
            print("ℹ️  No matching tasks found")
            return pl.DataFrame()
        
        # Convert to Polars DataFrame
        df = pl.DataFrame(all_tasks)
        
        # Convert datetime columns
        datetime_cols = ["logical_date", "dag_start_date", "dag_end_date", "task_start_date", "task_end_date"]
        for col in datetime_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False))
        
        print(f"✅ Collected {len(df)} task instances")
        return df


class TaskAnalyzer:
    """
    Analyzer for processing task data and generating insights.
    Provides methods to analyze task patterns and identify issues.
    """
    
    @staticmethod
    def get_basic_statistics(df: pl.DataFrame) -> Dict[str, Any]:
        """
        Calculate basic statistics from task data.
        
        Args:
            df: DataFrame with task data
            
        Returns:
            Dictionary with basic statistics
        """
        if df.is_empty():
            return {}
        
        total_tasks = len(df)
        unique_dag_runs = df["dag_run_id"].n_unique()
        unique_task_types = df["task_id"].n_unique()
        
        state_summary = df.group_by("task_state").agg(pl.count().alias("count")).sort("count", descending=True)
        
        return {
            "total_tasks": total_tasks,
            "unique_dag_runs": unique_dag_runs,
            "unique_task_types": unique_task_types,
            "state_breakdown": state_summary.to_dicts()
        }
    
    @staticmethod
    def find_runs_without_skipped_tasks(df: pl.DataFrame) -> pl.DataFrame:
        """
        Find DAG runs that have zero skipped tasks.
        
        Args:
            df: DataFrame with task data
            
        Returns:
            DataFrame with DAG runs that have no skipped tasks
        """
        if df.is_empty():
            return pl.DataFrame()
        
        # Group by DAG run and count task states
        run_stats = df.group_by("dag_run_id").agg([
            pl.col("task_state").filter(pl.col("task_state") == "skipped").count().alias("skipped_count"),
            pl.col("task_state").filter(pl.col("task_state") == "success").count().alias("success_count"),
            pl.col("task_state").filter(pl.col("task_state") == "failed").count().alias("failed_count"),
            pl.col("task_state").count().alias("total_tasks"),
            pl.col("logical_date").first().alias("logical_date"),
            pl.col("run_type").first().alias("run_type"),
            pl.col("dag_state").first().alias("dag_state"),
            pl.col("duration").sum().alias("total_duration")
        ])
        
        # Filter runs with zero skipped tasks
        no_skipped_runs = run_stats.filter(pl.col("skipped_count") == 0)
        
        return no_skipped_runs.sort("logical_date", descending=True)
    
    @staticmethod
    def create_dag_run_summary(df: pl.DataFrame) -> pl.DataFrame:
        """
        Create a comprehensive summary of DAG runs.
        
        Args:
            df: DataFrame with task data
            
        Returns:
            DataFrame with DAG run summaries
        """
        if df.is_empty():
            return pl.DataFrame()
            
        summary = df.group_by(["dag_run_id", "run_type", "logical_date", "dag_state"]).agg([
            pl.col("task_state").filter(pl.col("task_state") == "success").count().alias("success_tasks"),
            pl.col("task_state").filter(pl.col("task_state") == "failed").count().alias("failed_tasks"),
            pl.col("task_state").filter(pl.col("task_state") == "skipped").count().alias("skipped_tasks"),
            pl.col("task_state").count().alias("total_tasks"),
            pl.col("duration").sum().alias("total_duration")
        ]).sort("logical_date", descending=True)
        
        return summary


class ChartGenerator:
    """
    Generates visualizations using Altair for task data analysis.
    Creates interactive charts for different aspects of task execution.
    """
    
    def __init__(self, color_scheme: Dict[str, str] = None, width: int = 800, height: int = 400):
        """
        Initialize chart generator with optional customization.
        
        Args:
            color_scheme: Dictionary mapping task states to colors
            width: Default chart width
            height: Default chart height
        """
        self.color_scheme = color_scheme or {
            "success": "#2E7D32",
            "failed": "#D32F2F", 
            "skipped": "#FF9800",
            "running": "#1976D2",
            "queued": "#757575",
            "up_for_retry": "#9C27B0"
        }
        self.width = width
        self.height = height
    
    def create_task_state_distribution_chart(self, df: pl.DataFrame, width: int = None, height: int = None) -> alt.Chart:
        """
        Create a bar chart showing the distribution of task states.
        
        Args:
            df: DataFrame with task data
            width: Chart width (uses default if None)
            height: Chart height (uses default if None)
            
        Returns:
            Altair chart object
        """
        if df.is_empty():
            return alt.Chart().mark_text(text="No data available", size=20)
        
        # Use provided dimensions or defaults
        chart_width = width or 500
        chart_height = height or 300
        
        # Aggregate task states
        state_counts = df.group_by("task_state").agg(pl.count().alias("count"))
        
        # Extract colors and domains from color scheme
        domains = list(self.color_scheme.keys())
        ranges = list(self.color_scheme.values())
        
        chart = alt.Chart(state_counts.to_pandas()).mark_bar().encode(
            x=alt.X("task_state:N", title="Task State", sort="-y"),
            y=alt.Y("count:Q", title="Number of Tasks"),
            color=alt.Color(
                "task_state:N",
                scale=alt.Scale(domain=domains, range=ranges),
                legend=alt.Legend(title="Task State")
            ),
            tooltip=["task_state:N", "count:Q"]
        ).properties(
            width=chart_width,
            height=chart_height,
            title="Task State Distribution"
        )
        
        return chart
    
    def create_dag_runs_timeline_chart(self, df: pl.DataFrame, width: int = None, height: int = None) -> alt.Chart:
        """
        Create a timeline chart showing DAG runs and their task states over time.
        
        Args:
            df: DataFrame with task data
            width: Chart width (uses default if None)
            height: Chart height (uses default if None)
            
        Returns:
            Altair chart object
        """
        if df.is_empty():
            return alt.Chart().mark_text(text="No data available", size=20)
        
        # Use provided dimensions or defaults
        chart_width = width or self.width
        chart_height = height or self.height
        
        # Convert to pandas for Altair
        chart_data = df.select([
            "dag_run_id", "logical_date", "task_id", "task_state", "duration", "run_type"
        ]).to_pandas()
        
        # Extract colors and domains from color scheme
        domains = list(self.color_scheme.keys())
        ranges = list(self.color_scheme.values())
        
        chart = alt.Chart(chart_data).mark_circle(size=100).encode(
            x=alt.X("logical_date:T", title="Logical Date"),
            y=alt.Y("task_id:N", title="Task ID"),
            color=alt.Color(
                "task_state:N",
                scale=alt.Scale(domain=domains, range=ranges),
                legend=alt.Legend(title="Task State")
            ),
            size=alt.Size("duration:Q", title="Duration (s)", scale=alt.Scale(range=[50, 400])),
            tooltip=["dag_run_id:N", "task_id:N", "task_state:N", "duration:Q", "run_type:N"]
        ).properties(
            width=chart_width,
            height=chart_height,
            title="DAG Runs Timeline - Task Execution Over Time"
        )
        
        return chart
    
    def save_charts_as_html(self, charts: Dict[str, alt.Chart], output_dir: str = "/tmp") -> List[str]:
        """
        Save charts as HTML files.
        
        Args:
            charts: Dictionary mapping chart names to Altair chart objects
            output_dir: Directory to save HTML files
            
        Returns:
            List of file paths where charts were saved
        """
        saved_files = []
        
        for chart_name, chart in charts.items():
            try:
                filename = f"{output_dir}/airflow_{chart_name}.html"
                chart.save(filename)
                saved_files.append(filename)
                print(f"📊 Chart saved: {filename}")
            except Exception as e:
                print(f"❌ Error saving chart {chart_name}: {e}")
        
        return saved_files


def display_statistics(stats: Dict[str, Any]) -> None:
    """
    Display basic statistics in a formatted way.
    
    Args:
        stats: Statistics dictionary from TaskAnalyzer
    """
    if not stats:
        print("ℹ️  No statistics available")
        return
    
    print("\n" + "="*60)
    print("📈 TASK EXECUTION STATISTICS")
    print("="*60)
    print(f"Total Task Instances: {stats['total_tasks']:,}")
    print(f"Unique DAG Runs: {stats['unique_dag_runs']:,}")
    print(f"Unique Task Types: {stats['unique_task_types']:,}")
    
    print(f"\n📊 Task State Breakdown:")
    for state_info in stats['state_breakdown']:
        percentage = (state_info['count'] / stats['total_tasks']) * 100
        print(f"  • {state_info['task_state'].upper()}: {state_info['count']:,} ({percentage:.1f}%)")


def display_no_skipped_analysis(no_skipped_df: pl.DataFrame) -> None:
    """
    Display analysis of DAG runs with zero skipped tasks.
    
    Args:
        no_skipped_df: DataFrame with DAG runs that have no skipped tasks
    """
    print("\n" + "="*60)
    print("🎯 DAG RUNS WITH ZERO SKIPPED TASKS")
    print("="*60)
    
    if no_skipped_df.is_empty():
        print("❌ No DAG runs found with zero skipped tasks in the specified time period")
        return
    
    print(f"✅ Found {len(no_skipped_df)} DAG runs with no skipped tasks:\n")
    
    for row in no_skipped_df.iter_rows(named=True):
        success_rate = (row['success_count'] / row['total_tasks']) * 100 if row['total_tasks'] > 0 else 0
        
        print(f"🔹 {row['dag_run_id']}")
        print(f"   Success: {row['success_count']}/{row['total_tasks']} tasks ({success_rate:.1f}%)")
        print(f"   Failed: {row['failed_count']} tasks")
        print(f"   Total Duration: {row['total_duration']:.2f}s")
        print(f"   Run Type: {row['run_type']}")
        print(f"   Logical Date: {row['logical_date']}")
        print()


def validate_configuration(dag_id: str, time_period_str: str, task_states_list: List[str] = None) -> tuple:
    """
    Validate configuration parameters and return parsed values.
    
    Args:
        dag_id: DAG ID to validate
        time_period_str: Time period string to validate
        task_states_list: List of task state strings to validate
    
    Returns:
        Tuple of (time_period, task_states) objects
        
    Raises:
        ValueError: If any configuration parameter is invalid
    """
    if not dag_id:
        raise ValueError("DAG_ID is required and cannot be empty")
    
    # Validate TIME_PERIOD
    time_period = None
    for period in TimePeriod:
        if period.label == time_period_str:
            time_period = period
            break
    
    if time_period is None:
        valid_periods = [period.label for period in TimePeriod]
        raise ValueError(f"Invalid TIME_PERIOD '{time_period_str}'. Valid options: {valid_periods}")
    
    # Validate TASK_STATES
    task_states = []
    if task_states_list is not None:
        valid_states = [state.value for state in TaskState]
        for state in task_states_list:
            if state not in valid_states:
                raise ValueError(f"Invalid task state '{state}'. Valid options: {valid_states}")
            task_states.append(TaskState(state))
    else:
        task_states = list(TaskState)  # All states
    
    return time_period, task_states