import os

from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig


default_dbt_root_path = Path(__file__).parent / "dbt"
dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))

profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="prd",
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/profiles.yml')
)

raw_acidentes_brasil_dataset = Dataset("bigquery://acidentes.raw_acidentes_brasil")

default_args = {
    "owner": "Matheus Ferreira Costa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="consumer_bigquery_dbt_datasets",
    start_date=days_ago(1),
    schedule=[raw_acidentes_brasil_dataset],
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/mferreiracosta/"},
    tags=["landing", "bronze", "acidentes_brasil", "gcp"],
)
def dbt_bigquery_project():

    # Task que indica o início do pipeline
    start = EmptyOperator(task_id="start_task")

    models = DbtTaskGroup(
        group_id="models",
        project_config=ProjectConfig(
            dbt_root_path.as_posix()
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True
        }
    )

    # Task que indica o término do pipeline
    end = EmptyOperator(task_id="end_task")

    start >> models >> end


dbt_bigquery_project()
