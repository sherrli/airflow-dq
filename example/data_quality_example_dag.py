"""Example DAG usago of DataQualityYAMLCheckOperator and directory of config files"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import glob

from airflow import DAG
from airflow.operators.data_quality_yaml_check_operator import DataQualityYAMLCheckOperator
from airflow.operators.dummy_operator import DummyOperator

YAML_DIR = Path(__file__).parents[1] / "tests" / "configs"

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2019, 11, 1),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

dag = DAG(
    "data_quality_check",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)
tasks = []

for yaml_path in glob.glob(os.path.join(str(YAML_DIR), "*.yaml")):
    task_name = yaml_path.split("/")[-1][:-5]
    task = DataQualityYAMLCheckOperator(
        task_id=task_name,
        yaml_path=yaml_path,
        dag=dag
    )
    tasks.append(task)

task_before_dq = DummyOperator(
    task_id="task_before_data_quality_checks",
    dag=dag
)

task_after_dq = DummyOperator(
    task_id="task_after_data_quality_checks",
    dag=dag
)

task_before_dq.set_downstream(tasks)
task_after_dq.set_upstream(tasks)
