from datetime import timedelta

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago


default_args = {
    "owner": "Matheus Ferreira Costa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ingestion_local_pipeline",
    start_date=days_ago(1),
    schedule=None,
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["bronze", "silver", "acidentes_brasil", "local"],
)
def pipeline():

    @task
    def start() -> str:
        """Task que indica o início do pipeline"""
        return "start_task"

    file_sensor = FileSensor(
        task_id ="wait_for_file",
        filepath="acidentes_brasil.csv",
        fs_conn_id="file_conn",
        poke_interval=5,
        timeout=60,
    )

    @task.external_python(python="/usr/local/airflow/dask_venv/bin/python")
    def convert_csv_to_parquet_bronze():
        from include.etl.to_bronze import convert_csv_to_parquet_local_bronze

        filename = "acidentes_brasil"
        input_path = "include/datasets"
        output_path = "include/datalake/bronze"

        convert_csv_to_parquet_local_bronze(
            filename=filename,
            input_path=input_path,
            output_path=output_path
        )

    @task
    def end() -> str:
        """Task que indica o término do pipeline"""
        return "end_task"

    start() >> file_sensor >> convert_csv_to_parquet_bronze() >> end()


pipeline()