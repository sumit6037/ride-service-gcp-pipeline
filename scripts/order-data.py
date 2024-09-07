# HIT THE 4 ORDER API AND INGEST THE RESPONSE DATA TO 4 DIFFERENT TABLES IN BQ

import pandas as pd
from google.cloud import storage
import csv
import io
import requests
from datetime import date, datetime, timedelta
from pytz import timezone
from google.cloud import bigquery
import logging  # Import the logging module

# Define your BigQuery project ID
project_id = "XXXX-1330b"

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_id)

# Subfunction to fetch data from a specific URL based on order ID
def fetch_data_from_url(order_id, url):
    base_url = url.format(ORDER_ID=order_id)
    headers = {
        "Authorization": "XXXXX",
        "Content-Type": "application/json"
    }
    response = requests.get(base_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Subfunction to fetch data from the summary URL
def fetch_summary_data(ORDER_ID):
    url = 'https://api.aggregator.app/dispatch/v1/order/{ORDER_ID}/summary'
    return fetch_data_from_url(ORDER_ID, url)

# Subfunction to fetch data from the offer URL
def fetch_offer_data(ORDER_ID):
    url = 'https://api.aggregator.app/dispatch/v1/order/{ORDER_ID}/offer'
    return fetch_data_from_url(ORDER_ID, url)

# Subfunction to fetch data from the request URL
def fetch_request_data(ORDER_ID):
    url = 'https://api.aggregator.app/dispatch/v1/order/{ORDER_ID}/request'
    return fetch_data_from_url(ORDER_ID, url)

# Subfunction to fetch data from another URL (if needed)
def fetch_update_data(ORDER_ID):
    url = 'https://api.aggregator.app/dispatch/v1/order/{ORDER_ID}/update'
    return fetch_data_from_url(ORDER_ID, url)

def write_to_bq(extracted_order_ids):
    summary_data = fetch_summary_data(extracted_order_ids)
    offer_data = fetch_offer_data(extracted_order_ids)
    request_data = fetch_request_data(extracted_order_ids)
    update_data = fetch_update_data(extracted_order_ids)
    # Define your BigQuery dataset ID
    dataset_id = "XXXX_dataset"
    # Define the BigQuery table names
    summary_table_name = "summary_table"
    offer_table_name = "offer_table"
    request_table_name = "request_table"
    update_table_name = "update_table"
    # Define the 'orderId' schema field
    #order_id_field = bigquery.SchemaField("orderId", "STRING")
    # Create or get the BigQuery dataset
    dataset_ref = bq_client.dataset(dataset_id)
    #dataset = bigquery.Dataset(dataset_ref)
    #bq_client.create_dataset(dataset, exists_ok=True)
    summary_table_ref = dataset_ref.table(summary_table_name)
    offer_table_ref = dataset_ref.table(offer_table_name)
    request_table_ref = dataset_ref.table(request_table_name)
    update_table_ref = dataset_ref.table(update_table_name)
    try:
        # Insert data into the BigQuery tables
        summary_data['orderId'] = extracted_order_ids
        offer_data['orderId'] = extracted_order_ids
        request_data['orderId'] = extracted_order_ids
        update_data['orderId'] = extracted_order_ids
        # Add 'data_date' to the data before insertion
        nigerian_tz = timezone('Africa/Lagos')
        current_date = datetime.now(nigerian_tz).date().isoformat()
        summary_data['data_date'] = current_date
        offer_data['data_date'] = current_date
        request_data['data_date'] = current_date
        update_data['data_date'] = current_date
        if 'payments' not in summary_data:
            summary_data['payments'] = [{'amount': None, 'time': None}]
        # Attempt to insert data into BigQuery
        bq_client.insert_rows_json(summary_table_ref, [summary_data])
        bq_client.insert_rows_json(offer_table_ref, [offer_data])
        bq_client.insert_rows_json(request_table_ref, [request_data])
        bq_client.insert_rows_json(update_table_ref, [update_data])
    except Exception as e:
        # If an exception occurs, log the error message
        print(f"Error inserting data for orderId {extracted_order_ids}: {e}")
      
# TO RUN THE CODE IN CLOUD FUNCTION REOLACE  "process_data()" WITH "process_data(event, context)" 
def process_data():
    current_date = datetime.now().strftime("%Y-%m-%d")
    bucket_name = "XXXX_websocket_stage"
    file_name = f"Pre_accumulated_data.csv"
    output_bucket_name = "XXXX_websocket_data_filtered"
    current_output_file_name = f"Filtered-data_{current_date}.csv"
    previous_date = (datetime.now() - timedelta(days=1)).date().strftime("%Y-%m-%d")
    previous_output_file_name = f"filtered-data_{previous_date}.csv"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    # Download the CSV file and create a DataFrame
    csv_text = blob.download_as_text()
    # pre_accumulated_df = pd.DataFrame([x.split(',') for x in csv_text.split('\n')[1:]],
    #                   columns=[x for x in csv_text.split('\n')[0].split(',')])
    pre_accumulated_df = pd.read_csv(io.StringIO(csv_text), delimiter=',')

    # Check if the current_filtered_file exists, and if not, create an empty file
    output_bucket = storage_client.get_bucket(output_bucket_name)
    output_blob = output_bucket.blob(current_output_file_name)
    if not output_blob.exists():
        empty_df = pd.DataFrame(columns=['driverId', 'orderId', 'status','driverLocation'])
        empty_csv = empty_df.to_csv(index=False)
        output_blob.upload_from_string(empty_csv)

    current_filtered_blob = output_bucket.blob(current_output_file_name)
    current_filtered_csv_text = current_filtered_blob.download_as_text()
    current_filtered_df = pd.read_csv(io.StringIO(current_filtered_csv_text))


    previous_filtered_blob = output_bucket.blob(previous_output_file_name)
    previous_filtered_csv_text = previous_filtered_blob.download_as_text()
    previous_filtered_df = pd.read_csv(io.StringIO(previous_filtered_csv_text))

    # Clean column names by stripping leading and trailing whitespace
    # pre_accumulated_df.columns = pre_accumulated_df.columns.str.strip()

    for index, row in pre_accumulated_df.iterrows():
        order_id = row['orderId']
        status = row['status']
        driver_id = row['driverId']
        driver_location=row['driverLocation']

        # Filter based on status
        if status in ['FINISHED_PAID', 'FINISHED_UNPAID','CANCELLED_BY_DISPATCH','CANCELLED_BY_DRIVER','CANCELLED_DECIDED_NOT_TO_GO','CANCELLED_DRIVER_OFFLINE','CANCELLED_NO_TAXI','CANCELLED_SEARCH_EXCEEDED']:
            # Check if the order_id exists in the current_filtered_df or previous_filtered_df
            if (order_id in current_filtered_df['orderId'].values) or (order_id in previous_filtered_df['orderId'].values):
                print(f"Order ID {order_id} already exists in the current_filtered_df and previous_filtered_df.")
            else:
                # Append the data to the current_filtered_df
                new_data = pd.DataFrame([[driver_id, order_id, status,driver_location]], columns=['driverId', 'orderId', 'status','driverLocation'])
                # pd.concat([existing_df, filtered_df], ignore_index=True)
                current_filtered_df = pd.concat([new_data, current_filtered_df], ignore_index=True)
                extracted_order_id = current_filtered_df['orderId'].to_list()[0]
                # Call write_to_bq function
                write_to_bq(extracted_order_id)
            # Save the updated current_filtered_df back to the storage
            updated_csv = current_filtered_df.to_csv(index=False)
            current_filtered_blob.upload_from_string(updated_csv)

        else:
            print(f"No data matching the filter {status}.")
