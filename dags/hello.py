"""A hello-world DAG. Three tasks that run one after the other."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def say_hello():
    """A Python function Airflow will call as a task."""
    print("Hello from Python!")


# A DAG is just a Python object. Everything indented under this `with`
# block becomes part of the DAG.
with DAG(
    dag_id="hello",
    description="Our first DAG.",
    start_date=datetime(2026, 9, 29),
    schedule=None,                 # None = run only when triggered manually
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["workshop"],
) as dag:

    # Task 1 — a shell command
    task_shell = BashOperator(
        task_id="say_hello_shell",
        bash_command="echo 'Hello from Bash!'",
    )

    # Task 2 — a Python function
    task_python = PythonOperator(
        task_id="say_hello_python",
        python_callable=say_hello,
    )

    # Task 3 — another shell command
    task_done = BashOperator(
        task_id="all_done",
        bash_command="echo 'DAG finished.'",
    )

    # The arrow decides the order
    task_shell >> task_python >> task_done