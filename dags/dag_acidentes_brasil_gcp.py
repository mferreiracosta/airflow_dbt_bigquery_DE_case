from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

raw_acidentes_brasil_dataset = Dataset("bigquery://cobli.raw_acidentes_brasil")


@dag(
    dag_id="Ingestion_acidentes_brasil_gcp_pipeline",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["landing", "bronze", "bigquery", "raw_acidentes_brasil"],
)
def pipeline():

    start = EmptyOperator(task_id="start_task")

    file_sensor = FileSensor(
        task_id ="wait_for_file",
        filepath="acidentes_brasil.csv",
        fs_conn_id="file_conn",
        poke_interval=5,
        timeout=60,
    )

    local_files_to_landing = LocalFilesystemToGCSOperator(
        task_id="local_files_to_landing",
        src="include/datasets/acidentes_brasil.csv",
        dst="acidentes_brasil/acidentes_brasil.csv",
        bucket="plataforma-dados-cobli-landing",
        gcp_conn_id="google_cloud_default",
        mime_type="text/csv",
    )

    @task.external_python(python="/usr/local/airflow/dask_venv/bin/python")
    def convert_csv_to_parquet_bronze():
        from include.etl.to_bronze import convert_csv_to_parquet_gcs_bronze

        filename = "acidentes_brasil"
        src_bucket_name = "gs://plataforma-dados-cobli-landing"
        dst_bucket_name = "gs://plataforma-dados-cobli-bronze"

        convert_csv_to_parquet_gcs_bronze(
            filename=filename,
            src_bucket_name=src_bucket_name,
            dst_bucket_name=dst_bucket_name
        )
    
    # # Create bigquery dataset
    # create_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id='create_dataset',
    #     dataset_id='cobli',
    #     gcp_conn_id='google_cloud_default',
    # )

    # Load parquet file from bronze bucket on GCS to BigQuery raw table
    raw_acidentes_brazil_bigquery = aql.load_file(
        task_id='raw_acidentes_brazil_bigquery',
        input_file=File(
            'gs://plataforma-dados-cobli-bronze/acidentes_brasil/*.parquet',
            conn_id='google_cloud_default',
            filetype=FileType.PARQUET,
        ),
        output_table=Table(
            name='raw_acidentes_brasil',
            conn_id='google_cloud_default',
            metadata=Metadata(schema='cobli')
        ),
        if_exists="replace",
        use_native_support=True,
        outlets=[raw_acidentes_brasil_dataset]
    )

    end = EmptyOperator(task_id="end_task")

    start >> file_sensor >> local_files_to_landing >> convert_csv_to_parquet_bronze() >> raw_acidentes_brazil_bigquery >> end


pipeline()