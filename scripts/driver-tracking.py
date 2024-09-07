# TAKING PREACCUMULATED FILE AS INPUT INGEST THE COLUMN "driverLocation" DATA IN A SEPARARTE BQ TABLE. THE DATA IS FOR DRIVER TRACKING.

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from pandas_gbq import to_gbq
import ast
from pytz import timezone
from datetime import datetime

# Replace these with your GCS bucket and file information
bucket_name = "XXXX_websocket_stage"
file_name = "pre_accumulated_data.csv"

# Replace with your GCP project ID
project_id = 'XXXX-1330b'

# Replace with your BigQuery dataset ID and table name
dataset_id = 'XXXX_test'
table_id = 'XXXX_raw_order_data_test'

# Initialize a GCS client
client = storage.Client()

# Get a reference to the GCS bucket
bucket = client.bucket(bucket_name)

# Define the GCS file blob
blob = bucket.blob(file_name)
file_content = blob.download_as_text()
df = pd.read_csv(io.StringIO(file_content))

# Convert the 'driverLocation' column from string to dictionary
df['driverLocation'] = df['driverLocation'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

# Create a function to handle missing keys and float values in driverLocation
def extract_driver_location(driver_location):
    if isinstance(driver_location, dict):
        try:
            return {
                'accuracy': driver_location['accuracy'],
                'time': driver_location['time'],
                'bearing': driver_location['bearing'],
                'speed': driver_location['speed'],
                'lat': driver_location['lat'],
                'lng': driver_location['lng']
            }
        except KeyError:
            return {
                'accuracy': None,
                'time': None,
                'bearing': None,
                'speed': None,
                'lat': None,
                'lng': None
            }
    else:
        return {
            'accuracy': None,
            'time': None,
            'bearing': None,
            'speed': None,
            'lat': None,
            'lng': None
        }

# Create a nested structure for the 'driverLocation' column
nested_driver_location = df['driverLocation'].apply(extract_driver_location)

# Add the nested 'driverLocation' column to the original DataFrame
df['driverLocation'] = nested_driver_location
# Convert current datetime to West African Time (WAT)
nigerian_tz = timezone('Africa/Lagos')
current_datetime = datetime.now(nigerian_tz)

# Add the 'data_date' column to the DataFrame with the Nigerian timestamp
df['data_date'] = current_datetime


# Replace with your service account JSON key file path
credentials_path = 'XXXX-1330b-56f6c52a17c5.json'

# Create a BigQuery client with your credentials
credentials = service_account.Credentials.from_service_account_file(
    credentials_path, scopes=["https://www.googleapis.com/auth/bigquery"]
)
client = bigquery.Client(project=project_id, credentials=credentials)

# Define the schema for the BigQuery table
schema = [
    bigquery.SchemaField('driverId', 'STRING'),
    bigquery.SchemaField('orderId', 'STRING'),
    bigquery.SchemaField('status', 'STRING'),
    bigquery.SchemaField('data_date', 'TIMESTAMP'),
    bigquery.SchemaField('driverLocation', 'RECORD', mode='NULLABLE', fields=[
        bigquery.SchemaField('accuracy', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('time', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('bearing', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('speed', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('lat', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('lng', 'FLOAT', mode='NULLABLE')
    ])
]

# Define the destination table in the format 'project_id.dataset_id.table_id'
destination_table = f'{project_id}.{dataset_id}.{table_id}'
try:
    # Ingest the DataFrame into BigQuery without specifying the schema here
    to_gbq(df, destination_table=destination_table, project_id=project_id, if_exists='replace')
    print("Ingestion Successful")
except Exception as e:
    print(f"An error occurred: {str(e)}")