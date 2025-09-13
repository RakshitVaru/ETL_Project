from datetime import datetime
import os, sys
from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {"owner": "airflow","depends_on_past": False}

def run_etl():
    repo = "/opt/airflow/dags/repo"
    sys.path.insert(0, repo)
    from etl.run import run
    cfg = os.path.join(repo, "config", "pipeline.yaml")
    run(cfg)

with DAG(
    dag_id="risk_etl_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["risk","etl","duckdb"],
) as dag:
    run = PythonOperator(task_id="run_etl", python_callable=run_etl)
