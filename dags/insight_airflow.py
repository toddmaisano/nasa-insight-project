
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from datetime import datetime, timedelta
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


def transform_insight_data(**kwargs):
    import json
    import pandas as pd

    ti = kwargs['ti']
    data = ti.xcom_pull(key='insight_api_data')

    sol_keys = data.get("sol_keys", [])

    weather_data = []

    # Process each sol
    for sol in sol_keys:
        sol_data = data.get(sol, {})

        row = {
            "Sol": sol,
            "First_UTC": sol_data.get("First_UTC", "N/A"),
            "Last_UTC": sol_data.get("Last_UTC", "N/A"),
            "Avg_Temp": sol_data.get("AT", {}).get("av", "N/A"),
            "Min_Temp": sol_data.get("AT", {}).get("mn", "N/A"),
            "Max_Temp": sol_data.get("AT", {}).get("mx", "N/A"),
            "Avg_Wind_Speed": sol_data.get("HWS", {}).get("av", "N/A"),
            "Max_Wind_Speed": sol_data.get("HWS", {}).get("mx", "N/A"),
            "Avg_Pressure": sol_data.get("PRE", {}).get("av", "N/A"),
            "Max_Pressure": sol_data.get("PRE", {}).get("mx", "N/A"),
            "Season": sol_data.get("Season", "N/A"),
            "Northern_Season": sol_data.get("Northern_season", "N/A"),
            "Southern_Season": sol_data.get("Southern_season", "N/A"),
        }

        weather_data.append(row)

    # Convert to DataFrame
    df = pd.DataFrame(weather_data)
    ti.xcom_push(key='insight_api_data_csv', value=df.to_csv(index=False))


extract_api_data = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_insight_data,
    provide_context=True,
    dag=dag,
    )

transform_api_data = PythonOperator(
    task_id='transform_api_data',
    python_callable=transform_insight_data,
    provide_context=True,
    dag=dag,
    )

upload_to_s3 = S3CreateObjectOperator(
        task_id="upload_to_S3",
        aws_conn_id='AWS_CONN',
        s3_bucket='insight-api-output',
        s3_key='raw/insight_data.csv',
        data="{{ti.xcom_pull(key='insight_api_data_csv')}}",
        dag=dag,
    )

# Set task dependencies
extract_api_data >> transform_api_data >> upload_to_s3
