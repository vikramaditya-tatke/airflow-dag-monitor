#!/usr/bin/env python3
"""
Main script for Airflow DAG Task Monitor

This script orchestrates the entire DAG monitoring workflow by creating
objects and coordinating their interactions. Configuration is loaded from
config.py and all functionality is imported from dag_monitor_core.py.

Usage: python main.py
"""

import sys

# Import configuration
try:
    import config
except ImportError as e:
    print(f"❌ Error importing configuration: {e}")
    print("Make sure config.py exists in the same directory")
    sys.exit(1)

# Import core functionality
try:
    from dag_monitor_core import (
        AirflowClient,
        TaskDataFetcher,
        TaskAnalyzer,
        ChartGenerator,
        display_statistics,
        display_no_skipped_analysis,
        validate_configuration
    )
except ImportError as e:
    print(f"❌ Error importing core modules: {e}")
    print("Make sure dag_monitor_core.py exists in the same directory")
    sys.exit(1)


def main():
    """
    Main execution function that orchestrates the entire analysis process.
    Creates objects and coordinates the workflow using configuration settings.
    """
    # Display configuration
    print("🔍 AIRFLOW DAG TASK MONITOR")
    print("="*60)
    print(f"DAG ID: {config.DAG_ID}")
    print(f"Time Period: {config.TIME_PERIOD}")
    print(f"Task States: {config.TASK_STATES or 'All states'}")
    print(f"Airflow URL: {config.AIRFLOW_URL}")
    print(f"Show No-Skipped Only: {config.SHOW_NO_SKIPPED_ONLY}")
    print(f"Save Charts: {config.SAVE_CHARTS}")
    print(f"Save Data: {config.SAVE_DATA}")
    
    # Validate configuration
    try:
        time_period, task_states = validate_configuration(
            config.DAG_ID,
            config.TIME_PERIOD,
            config.TASK_STATES
        )
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        sys.exit(1)
    
    # Create core objects
    print("\n🔧 Initializing components...")
    
    # Create Airflow client
    client = AirflowClient(
        base_url=config.AIRFLOW_URL,
        timeout=getattr(config, 'REQUEST_TIMEOUT', 30)
    )
    
    # Create data fetcher
    fetcher = TaskDataFetcher(client)
    
    # Create analyzer
    analyzer = TaskAnalyzer()
    
    # Create chart generator with custom colors if available
    color_scheme = getattr(config, 'TASK_STATE_COLORS', None)
    chart_width = getattr(config, 'CHART_WIDTH', 800)
    chart_height = getattr(config, 'CHART_HEIGHT', 400)
    
    chart_generator = ChartGenerator(
        color_scheme=color_scheme,
        width=chart_width,
        height=chart_height
    )
    
    # Fetch task data
    print("\n📊 Fetching and processing data...")
    task_df = fetcher.fetch_task_data(config.DAG_ID, time_period, task_states)
    
    # Check if data was retrieved
    if task_df.is_empty():
        print("\n❌ No data found for the specified criteria")
        print("Please check:")
        print("  • DAG_ID exists and is spelled correctly")
        print("  • AIRFLOW_URL is accessible")
        print("  • TIME_PERIOD contains DAG runs")
        print("  • Selected TASK_STATES exist in the data")
        sys.exit(1)
    
    # Analyze data
    print("\n📈 Analyzing task data...")
    stats = analyzer.get_basic_statistics(task_df)
    
    # Display basic statistics
    display_statistics(stats)
    
    # Analyze runs without skipped tasks if requested
    if config.SHOW_NO_SKIPPED_ONLY:
        print("\n🔍 Analyzing DAG runs with zero skipped tasks...")
        no_skipped_df = analyzer.find_runs_without_skipped_tasks(task_df)
        display_no_skipped_analysis(no_skipped_df)
    
    # Generate and save charts if requested
    if config.SAVE_CHARTS:
        print("\n📊 Generating charts...")
        print("="*60)
        
        # Create charts
        charts = {
            "task_state_distribution": chart_generator.create_task_state_distribution_chart(
                task_df,
                width=getattr(config, 'SMALL_CHART_WIDTH', 500),
                height=getattr(config, 'SMALL_CHART_HEIGHT', 300)
            ),
            "dag_runs_timeline": chart_generator.create_dag_runs_timeline_chart(
                task_df,
                width=chart_width,
                height=chart_height
            )
        }
        
        # Save charts
        output_dir = getattr(config, 'OUTPUT_DIR', '/tmp')
        saved_files = chart_generator.save_charts_as_html(charts, output_dir)
        
        if saved_files:
            print(f"✅ {len(saved_files)} charts saved successfully")
            for file_path in saved_files:
                print(f"   📄 {file_path}")
        else:
            print("❌ No charts were saved")
    
    # Save data to CSV if requested
    if config.SAVE_DATA:
        print("\n💾 Saving data to CSV...")
        output_dir = getattr(config, 'OUTPUT_DIR', '/tmp')
        output_file = f"{output_dir}/airflow_tasks_{config.DAG_ID}_{time_period.label}_{len(task_df)}_records.csv"
        
        try:
            task_df.write_csv(output_file)
            print(f"✅ Task data saved to: {output_file}")
        except Exception as e:
            print(f"❌ Error saving data: {e}")
    
    # Final summary
    print("\n" + "="*60)
    print("✅ Analysis complete!")
    print(f"📊 Processed {len(task_df)} task instances from {stats.get('unique_dag_runs', 0)} DAG runs")
    
    if config.SAVE_CHARTS:
        print(f"📈 Charts saved to: {getattr(config, 'OUTPUT_DIR', '/tmp')}")
    
    if config.SAVE_DATA:
        print(f"💾 Data saved to: {output_file}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("Please check your configuration and try again")
        sys.exit(1)