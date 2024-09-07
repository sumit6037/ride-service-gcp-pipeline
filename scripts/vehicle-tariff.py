# EXTRACTING LAT AND LNG DATA FROM THE DRIVER TRACKING BQ TABLE HIT THE VEHICLE AND TARIFF API ENDPOINTS, INGEST THE DATA IN BQ TABLE.

import time
import requests
from pytz import timezone
from datetime import datetime
from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client()

def fetch_data_from_url(url):
    headers = {
        "Authorization": "XXXXX",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_date=response.json()
        # Convert current datetime to West African Time (WAT)
        nigerian_tz = timezone('Africa/Lagos')
        current_date = datetime.now(nigerian_tz).date().isoformat()
        response_date['data_date'] = current_date
        return response_date
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
        return None

def fetch_vehicle_data(lat, lng):
    api_url = "https://api.aggregator.app/dispatch/v1/vehicle"
    url = f"{api_url}?origin={lat},{lng}"
    return fetch_data_from_url(url)

def fetch_tariff_data(lat, lng):
    api_url = "https://api.aggregator.app/dispatch/v1/tariff"
    url = f"{api_url}?origin={lat},{lng}"
    return fetch_data_from_url(url)

# Define your BigQuery table and dataset information
project_id = 'XXXX-1330b'
dataset_id = 'XXXX_test'
table_id = 'XXXX_raw_order_data_test'

# Construct the SQL query to get distinct lat and lng values
query = f"""
    SELECT DISTINCT driverLocation.lat, driverLocation.lng
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE DATE(data_date) = CURRENT_DATE();
"""

# Execute the query
query_job = client.query(query)
results = query_job.result()

# Uncomment and indent this block to fetch and process data
for row in results:
    lat, lng = row.lat, row.lng
    vehicle_data = fetch_vehicle_data(lat, lng)
    tariff_data = fetch_tariff_data(lat, lng)

    # # Add rate limiting to avoid overloading the API
    # time.sleep(1)  # Sleep for 1 second between API calls (adjust as needed)

    # Define your BigQuery dataset ID
    dataset_id = 'XXXX_test'

    # Define the BigQuery table names
    vehicle_table_name = "vehicle"
    tariff_table_name = "tariff"

    # Create or get the BigQuery dataset
    dataset_ref = client.dataset(dataset_id)

    vehicle_table_ref = dataset_ref.table(vehicle_table_name)
    tariff_table_ref = dataset_ref.table(tariff_table_name)

    try:
  
        client.insert_rows_json(vehicle_table_ref, [vehicle_data])
        client.insert_rows_json(tariff_table_ref, [tariff_data])
    except Exception as e:
        print(f"Error inserting data: {e}")