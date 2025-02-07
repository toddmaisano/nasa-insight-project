import requests
from io import StringIO
import boto3
import csv
import os
import json
import datetime
import pandas as pd
from botocore.exceptions import ClientError


def get_secret(secret_name):

    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret


def add_timestamp_to_filename(original_filename):
    # Get the current date and time
    now = datetime.datetime.now()

    # Format the timestamp
    timestamp = now.strftime('%Y%m%d%H%M')

    # Split the original filename into name and extension
    name, extension = os.path.splitext(original_filename)

    # Create a new filename with the timestamp appended before the file extension
    new_filename = f"{name}_{timestamp}{extension}"

    return new_filename


def get_insight_data(api_key):
    base_url = "https://api.nasa.gov/insight_weather/?feedtype=json&ver=1.0&"
    complete_url = f"{base_url}api_key=5WCDPNiNnkoIfqeYQbfc9I6toJbnMRlPsjSgQzqJ"
    response = requests.get(complete_url)

    if response.status_code == 200:
        # Ensure response content is treated as a string
        return response.json()
    else:
        print(f"Failed to retrieve data, status code: {response.status_code}")
        return None


def lambda_handler(event, context):

    # Get data from nasa API
    api_key = get_secret('nasa_api_key')
    insight_data = get_insight_data(api_key)

        # Extract all sols
    sol_keys = insight_data.get("sol_keys", [])

    # Initialize a list to store processed data
    weather_data = []

    for sol in sol_keys:
        sol_data = insight_data.get(sol, {})
        
        # Extract Atmospheric Temperature (AT), Horizontal Wind Speed (HWS), and Pressure (PRE)
        at_data = sol_data.get("AT", {})
        hws_data = sol_data.get("HWS", {})
        pre_data = sol_data.get("PRE", {})
        
        # Extract Wind Directions (WD)
        wind_directions = sol_data.get("WD", {})
        
        for direction, values in wind_directions.items():
            if direction != "most_common":  # Exclude the summary "most_common" entry
                row = {
                    "Sol": sol,
                    "First_UTC": sol_data.get("First_UTC", "N/A"),
                    "Last_UTC": sol_data.get("Last_UTC", "N/A"),
                    "Month_Ordinal": sol_data.get("Month_ordinal", "N/A"),
                    "Season": sol_data.get("Season", "N/A"),
                    "Northern_Season": sol_data.get("Northern_season", "N/A"),
                    "Southern_Season": sol_data.get("Southern_season", "N/A"),
                    "Avg_Temp": at_data.get("av", "N/A"),
                    "Min_Temp": at_data.get("mn", "N/A"),
                    "Max_Temp": at_data.get("mx", "N/A"),
                    "Avg_Wind_Speed": hws_data.get("av", "N/A"),
                    "Min_Wind_Speed": hws_data.get("mn", "N/A"),
                    "Max_Wind_Speed": hws_data.get("mx", "N/A"),
                    "Avg_Pressure": pre_data.get("av", "N/A"),
                    "Min_Pressure": pre_data.get("mn", "N/A"),
                    "Max_Pressure": pre_data.get("mx", "N/A"),
                    "Wind_Direction": values.get("compass_point", "N/A"),
                    "Wind_Degrees": values.get("compass_degrees", "N/A"),
                    "Wind_Ct": values.get("ct", "N/A"),
                }
                combined_data.append(row)

    # Convert to DataFrame
    df = pd.DataFrame(weather_data)

    # Specify your S3 bucket and object name
    bucket_name = 'nasa-insight-data'
    object_name = 'insight_data.csv'
    file_name = add_timestamp_to_filename(object_name)

    # Initialize the S3 client
    s3_client = boto3.client('s3')
    # Upload the CSV to S3
    s3_client.put_object(Bucket=bucket_name, Key=f'raw/{file_name}', Body=df.to_csv(index=False))

    return {
        'statusCode': 200,
        'body': f"Successfully wrote weather data to {bucket_name}"
    }
