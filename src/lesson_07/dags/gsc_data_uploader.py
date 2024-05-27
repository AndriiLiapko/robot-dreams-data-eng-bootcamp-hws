import datetime

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

BUCKET_NAME = 'de-07-robot-dreams-bootcamp-gcp-bucket'

with DAG(
    dag_id='gsc_data_uploader_v2',
    params={
        "date_to_process": Param(
            default=f"{datetime.date.today()}",
            type="string",
            format="date"
        ),

        "output_prefix": Param(
            default="src1/sales/v1/"
        )
    }
):

    upload_local_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        src="/opt/airflow/data/{{ params.date_to_process }}.csv",
        dst=("{{ params.output_prefix }}"
             + "{{ params.date_to_process.replace('-', '/') }}/"
             + "{{ params.date_to_process }}.csv"),
        bucket=BUCKET_NAME,
    )

    upload_local_file_to_gcs
