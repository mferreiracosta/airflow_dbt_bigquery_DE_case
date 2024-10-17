from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago


@dag(
    dag_id="Ingestion_acidentes_brasil_pipeline",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "acidentes_brasil"],
)
def pipeline():

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline"""
        return "start_task"
    
    @task
    def end_task() -> str:
        """Task que indica o término do pipeline"""
        return "end_task"

    file_sensor = FileSensor(
        task_id ="wait_for_file",
        filepath="acidentes_brasil.csv",
        fs_conn_id="file_conn",
        poke_interval=5,
        timeout=60,
    )

    @task.external_python(python="/usr/local/airflow/dask_venv/bin/python")
    def to_bronze_task():
        from include.etl.to_bronze import convert_csv_to_bronze_parquet

        table_name = "acidentes_brasil"
        input_path = "include/datasets"
        output_path = "include/datalake/bronze"

        convert_csv_to_bronze_parquet(table_name, input_path, output_path)

    start_task() >> file_sensor >> to_bronze_task() >> end_task()


pipeline()