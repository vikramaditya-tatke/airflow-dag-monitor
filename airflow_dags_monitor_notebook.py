import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    from airflow_dags_monitor_simple import validate_configuration, AirflowClient, TaskDataFetcher, TaskAnalyzer, ChartGenerator
    return (
        AirflowClient,
        ChartGenerator,
        TaskAnalyzer,
        TaskDataFetcher,
        validate_configuration,
    )


@app.cell
def _(
    AIRFLOW_URL,
    AirflowClient,
    ChartGenerator,
    DAG_ID,
    SAVE_CHARTS,
    SHOW_NO_SKIPPED_ONLY,
    TaskAnalyzer,
    TaskDataFetcher,
    display_no_skipped_analysis,
    display_statistics,
    sys,
    validate_configuration,
):
    try:
        time_period, task_states = validate_configuration()
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        sys.exit(1)

    # Display configuration
    print("🔍 AIRFLOW DAG TASK MONITOR (SIMPLIFIED)")
    print("="*60)
    print(f"DAG ID: {DAG_ID}")
    print(f"Time Period: {time_period.label}")
    print(f"Task States: {[state.value for state in task_states]}")
    print(f"Airflow URL: {AIRFLOW_URL}")
    print(f"Show No-Skipped Only: {SHOW_NO_SKIPPED_ONLY}")
    print(f"Save Charts: {SAVE_CHARTS}")

    # Initialize components
    client = AirflowClient(AIRFLOW_URL)
    fetcher = TaskDataFetcher(client)

    # Fetch task data
    task_df = fetcher.fetch_task_data(DAG_ID, time_period, task_states)

    if task_df.is_empty():
        print("\n❌ No data found for the specified criteria")
        print("Please check:")
        print("  • DAG_ID exists and is spelled correctly")
        print("  • AIRFLOW_URL is accessible")
        print("  • TIME_PERIOD contains DAG runs")
        print("  • Selected TASK_STATES exist in the data")
        sys.exit(1)

    # Analyze data
    analyzer = TaskAnalyzer()
    stats = analyzer.get_basic_statistics(task_df)

    # Display basic statistics
    display_statistics(stats)

    # Analyze runs without skipped tasks if requested
    if SHOW_NO_SKIPPED_ONLY:
        no_skipped_df = analyzer.find_runs_without_skipped_tasks(task_df)
        display_no_skipped_analysis(no_skipped_df)

    # Generate and save charts if requested
    if SAVE_CHARTS:
        print("\n" + "="*60)
        print("📊 GENERATING CHARTS")
        print("="*60)

        chart_generator = ChartGenerator()

        charts = {
            "task_state_distribution": chart_generator.create_task_state_distribution_chart(task_df),
            "dag_runs_timeline": chart_generator.create_dag_runs_timeline_chart(task_df)
        }

        saved_files = chart_generator.save_charts_as_html(charts)

        if saved_files:
            print(f"✅ {len(saved_files)} charts saved successfully")
        else:
            print("❌ No charts were saved")

    # Save data to CSV if requested
    save_data = globals().get('SAVE_DATA', False)
    if save_data:
        output_file = f"/tmp/airflow_tasks_{DAG_ID}_{time_period.label}_{len(task_df)}_records.csv"
        task_df.write_csv(output_file)
        print(f"\n💾 Task data saved to: {output_file}")

    print("\n✅ Analysis complete!")
    return


if __name__ == "__main__":
    app.run()
