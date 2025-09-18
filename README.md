# Airflow DAG Task Monitor

Monitor and analyze Airflow DAG task statuses with interactive charts and statistics. This project provides a marimo notebook and Python scripts to fetch, analyze, and visualize DAG/task data from your Airflow instance.

## Features
- Fetches DAG/task data from Airflow webserver
- Analyzes task states and run statistics
- Interactive charts (Altair)
- CSV export and sample data support
- Highly configurable via `config.py`

## Prerequisites
- Python 3.13+
- Airflow webserver running and accessible
- (Optional) ClickHouse for advanced data export

## Installation
Clone the repo and install dependencies:

```bash
git clone https://github.com/yourusername/airflow-dag-monitor.git
cd airflow-dag-monitor
uv init
uv sync
```

## Configuration
Edit `config.py` to set:
- `DAG_ID`: The DAG to monitor
- `AIRFLOW_URL`: Your Airflow webserver URL
- `TIME_PERIOD`, `TASK_STATES`, and other analysis options

## Usage

### Run the main script
```bash
python main.py
```

### Launch the marimo notebook (for interactive analysis)
```bash
marimo run airflow_dags_monitor_notebook.py
```

### Export ClickHouse data (optional)
```bash
python export_clickHouse_data_to_csvs.py
```

## Sample Data
Sample CSVs are provided in `sampledata/` for testing and demo purposes.

## Troubleshooting
- Ensure Airflow is running and accessible at the configured URL
- Check `config.py` for correct DAG ID and parameters
- Review error messages for missing dependencies or misconfiguration

## Dependencies
Key packages (see `pyproject.toml`):
- altair
- clickhouse-connect
- httpx
- marimo
- pandas
- polars
- pyarrow
- requests

## License
MIT