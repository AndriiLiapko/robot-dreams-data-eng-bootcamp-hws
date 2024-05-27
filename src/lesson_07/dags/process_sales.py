import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator


def build_target_path(**kwargs):
    BASE_DIR = os.environ.get("BASE_DIR")
    execution_date = str(kwargs['execution_date']).split(" ")[0]
    print(execution_date)

    raw_dir = os.path.join(BASE_DIR, "raw", "sales", execution_date)
    stg_dir = os.path.join(BASE_DIR, "stg", "sales", execution_date)

    kwargs['ti'].xcom_push(key='raw_dir', value=raw_dir)
    kwargs['ti'].xcom_push(key='stg_dir', value=stg_dir)


with DAG(
    dag_id='process_sales',
    default_args={},
    description="DAG to schedule the fetch and processing of sales data",
    schedule_interval="0 1 * * *",
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    catchup=True,
    tags=["process_sales", "robot_dreams", "data_engineering", "home_work"],
    max_active_runs=1
) as dag:

    build_paths = PythonOperator(
        task_id='build_paths',
        python_callable=build_target_path,
        provide_context=True,
        dag=dag
    )

    extract_data_from_api = SimpleHttpOperator(
        task_id='extract_data_from_api',
        method='POST',
        http_conn_id='flask_job_1_fetch',
        endpoint='/',
        data='''{
             "date": "{{ execution_date }}",
             "raw_dir": "{{ ti.xcom_pull(task_ids=\'build_paths\', key=\'raw_dir\') }}"
             }''',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: response.status_code == 201,
    )

    convert_to_avro = SimpleHttpOperator(
        task_id='convert_to_avro',
        method='POST',
        http_conn_id='flask_job_2_convert_to_avro',
        data='''{
            "raw_dir": "{{ ti.xcom_pull(task_ids=\'build_paths\', key=\'raw_dir\') }}",
            "stg_dir": "{{ ti.xcom_pull(task_ids=\'build_paths\', key=\'stg_dir\') }}"
        }''',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: response.status_code == 201,
    )

    build_paths >> extract_data_from_api >> convert_to_avro



