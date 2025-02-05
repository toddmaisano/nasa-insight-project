from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import json
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Set DAG
dag = DAG(
    'insight_api_data',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False)

# Mars Insight Rover API endpoint and parameters
api_endpoint = "https://api.nasa.gov/insight_weather/"
api_params = {
        "feedtype": "json",
        "ver": "1.0",
        "api_key": Variable.get("key")
    }


def extract_insight_data(**kwargs):
    '''Extracts mars rover 'insight' data from nasa api

    Parameters: 
        ti: Task Instance

    Returns:
        None - passes data to create bucket'''

    # Make imports
    import requests

    print("Extracting started ")
    ti = kwargs['ti']
    response = requests.get(api_endpoint, params=api_params)
    insight_api_data = response.json()
    ti.xcom_push(key='insight_api_data', value=insight_api_data)


extract_api_data = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_insight_data,
    provide_context=True,
    dag=dag,
    )

upload_to_s3 = S3CreateObjectOperator(
        task_id="upload_to_S3",
        aws_conn_id='AWS_CONN',
        s3_bucket='insight-api-output',
        s3_key='raw/insight_data.json',
        data="{{ti.xcom_pull(key='insight_api_data')}}",
        dag=dag,
    )

# Set task dependencies
extract_api_data >> upload_to_s3

