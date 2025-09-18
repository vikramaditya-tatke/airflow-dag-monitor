import marimo
import polars as pl
import altair as alt

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    from dag_monitor_core import (
        validate_configuration,
        AirflowClient,
        TaskDataFetcher,
        TaskAnalyzer,
    )
    from config import DAG_ID, AIRFLOW_URL, TIME_PERIOD, TASK_STATES, SHOW_NO_SKIPPED_ONLY

    return (
        AIRFLOW_URL,
        DAG_ID,
        SHOW_NO_SKIPPED_ONLY,
        AirflowClient,
        TaskAnalyzer,
        TaskDataFetcher,
        TIME_PERIOD,
        TASK_STATES,
        validate_configuration,
    )


@app.cell
def _():
    # Color scheme for charts - Catppuccin Mocha theme
    COLOR_SCHEME = {
        "success": "#a6e3a1",  # Green
        "failed": "#f38ba8",  # Red
        "skipped": "#f9e2af",  # Yellow
        "running": "#89b4fa",  # Blue
        "queued": "#bac2de",  # Subtext1
        "up_for_retry": "#cba6f7",  # Purple
    }

    def create_task_state_distribution_chart(df, width=500, height=300):
        """Create a bar chart showing the distribution of task states."""
        if df.is_empty():
            return alt.Chart().mark_text(text="No data available", size=20)

        # Aggregate task states
        state_counts = df.group_by("task_state").agg(pl.len().alias("count"))

        # Extract colors and domains from color scheme
        domains = list(COLOR_SCHEME.keys())
        ranges = list(COLOR_SCHEME.values())

        chart = (
            alt.Chart(state_counts.to_pandas())
            .mark_bar()
            .encode(
                x=alt.X("task_state:N", title="Task State", sort="-y"),
                y=alt.Y("count:Q", title="Number of Tasks"),
                color=alt.Color(
                    "task_state:N",
                    scale=alt.Scale(domain=domains, range=ranges),
                    legend=alt.Legend(title="Task State"),
                ),
                tooltip=["task_state:N", "count:Q"],
            )
            .properties(width=width, height=height, title="Task State Distribution")
        )

        return chart

    def create_dag_runs_timeline_chart(df, width=800, height=400):
        """Create a timeline chart showing DAG runs and their task states over time."""
        if df.is_empty():
            return alt.Chart().mark_text(text="No data available", size=20)

        # Convert to pandas for Altair
        chart_data = df.select(
            [
                "dag_run_id",
                "logical_date",
                "task_id",
                "task_state",
                "duration",
                "run_type",
            ]
        ).to_pandas()

        # Extract colors and domains from color scheme
        domains = list(COLOR_SCHEME.keys())
        ranges = list(COLOR_SCHEME.values())

        chart = (
            alt.Chart(chart_data)
            .mark_circle(size=100)
            .encode(
                x=alt.X("logical_date:T", title="Logical Date"),
                y=alt.Y("task_id:N", title="Task ID"),
                color=alt.Color(
                    "task_state:N",
                    scale=alt.Scale(domain=domains, range=ranges),
                    legend=alt.Legend(title="Task State"),
                ),
                size=alt.Size(
                    "duration:Q", title="Duration (s)", scale=alt.Scale(range=[50, 400])
                ),
                tooltip=[
                    "dag_run_id:N",
                    "task_id:N",
                    "task_state:N",
                    "duration:Q",
                    "run_type:N",
                ],
            )
            .properties(
                width=width,
                height=height,
                title="DAG Runs Timeline - Task Execution Over Time",
            )
        )

        return chart

    def display_statistics(stats):
        """Display basic statistics in a formatted way."""
        if not stats:
            return None

        return {
            "total_tasks": stats["total_tasks"],
            "unique_dag_runs": stats["unique_dag_runs"],
            "unique_task_types": stats["unique_task_types"],
            "state_breakdown": stats["state_breakdown"],
        }

    def display_no_skipped_analysis(no_skipped_df):
        """Display analysis of DAG runs with zero skipped tasks."""
        if no_skipped_df.is_empty():
            return None

        return no_skipped_df

    return (
        COLOR_SCHEME,
        create_dag_runs_timeline_chart,
        create_task_state_distribution_chart,
        display_no_skipped_analysis,
        display_statistics,
    )


@app.cell
def _(
    AIRFLOW_URL,
    AirflowClient,
    DAG_ID,
    SHOW_NO_SKIPPED_ONLY,
    TaskAnalyzer,
    TaskDataFetcher,
    TIME_PERIOD,
    TASK_STATES,
    create_dag_runs_timeline_chart,
    create_task_state_distribution_chart,
    display_no_skipped_analysis,
    display_statistics,
    sys,
    validate_configuration,
):
    # Initialize variables outside try block
    time_period = None
    task_states = None

    try:
        time_period, task_states = validate_configuration(
            DAG_ID, TIME_PERIOD, TASK_STATES
        )
    except ValueError as e:
        sys.exit(1)

    # Initialize components
    client = AirflowClient(AIRFLOW_URL)
    fetcher = TaskDataFetcher(client)

    # Fetch task data
    task_df = fetcher.fetch_task_data(DAG_ID, time_period, task_states)

    if task_df.is_empty():
        sys.exit(1)

    # Analyze data
    analyzer = TaskAnalyzer()
    stats = analyzer.get_basic_statistics(task_df)

    # Display basic statistics
    stats_result = display_statistics(stats)

    # Analyze runs without skipped tasks if requested
    no_skipped_result = None
    if SHOW_NO_SKIPPED_ONLY:
        no_skipped_df = analyzer.find_runs_without_skipped_tasks(task_df)
        no_skipped_result = display_no_skipped_analysis(no_skipped_df)

    # Generate charts
    charts = {
        "task_state_distribution": create_task_state_distribution_chart(task_df),
        "dag_runs_timeline": create_dag_runs_timeline_chart(task_df),
    }

    return {
        "task_df": task_df,
        "stats": stats_result,
        "no_skipped_result": no_skipped_result,
        "charts": charts,
    }


if __name__ == "__main__":
    app.run()
