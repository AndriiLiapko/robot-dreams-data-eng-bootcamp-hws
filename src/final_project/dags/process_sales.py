from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 9, 1),
    'end_date': datetime(2022, 9, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'process_sales',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True,
) as dag:
    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bronze',
        bucket='de-final-project-data',
        source_objects=[
            "sales/{{ execution_date.strftime('%Y-%m-') }}{{ execution_date.day }}/{{ execution_date.strftime('%Y-%m-') }}{{ execution_date.day }}__sales.csv"
        ],
        destination_project_dataset_table='bronze.sales',
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

