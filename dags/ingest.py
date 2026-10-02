"""Ingest DAG — runs the three Day 1 loaders in order.

Replaces 'make bronze' with something scheduled and monitored.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/workspaces/ite-data-pipelines-3"   # adjust if your folder differs

with DAG(
    dag_id="ingest",
    description="Load attendance CSVs, student DB and grades API into Bronze.",
    start_date=datetime(2026, 9, 29, tz="Asia/Singapore),          # tz="Asia/Singapore
    schedule="0 2 * * *",                             # change to "0 2 * * *" for 02:00 daily
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "owner": "ITE Data Team",
    },
    tags=["workshop", "ingest"],
) as dag:

    # Task 1: load attendance CSVs into bronze.file_attendance
    ingest_files = BashOperator(
        task_id="ingest_files",
        bash_command=f"cd {REPO} && python lab1/solution/ingest_files.py",
    )

    # Task 2: incrementally load students from the SQLite DB into bronze.db_students
    ingest_database = BashOperator(
        task_id="ingest_database",
        bash_command=f"cd {REPO} && python lab1/solution/ingest_database.py",
    )

    # Task 3: incrementally load grades from the API into bronze.api_grades
    ingest_api = BashOperator(
        task_id="ingest_api",
        bash_command=f"cd {REPO} && python lab2/solution/ingest_api.py",
    )

    # Chain them: files first, then database, then API
    ingest_files >> ingest_database >> ingest_api
