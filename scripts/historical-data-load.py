# FETCHING THE HISTORICAL DATA FOR ALL 4 ORDER API ENPOINTS AND INGEST THE DATA IN BQ

import aiohttp
import asyncio
import pandas as pd
from datetime import date, datetime
import io
import nest_asyncio
from google.cloud import storage, bigquery
from pytz import timezone
from google.oauth2 import service_account

nest_asyncio.apply()

async def fetch_data(session, id):
    base_url = 'https://api.aggregator.app/dispatch/v1/order/{ORDER_ID}/offer'
    headers = {'Authorization': 'XXXXXX','Content-Type': 'application/json'}

    api_url  = base_url.format(ORDER_ID=id)

    try:
        async with session.get(api_url, headers=headers) as response:
            response_data = await response.json()
            return response_data
    except Exception as e:
        print(f"Failed to fetch data for ID {id}: {str(e)}")
        return None

async def main():
    input_bucket_name = 'XXXXX_generated_test'

# Initialize GCS client
    storage_client = storage.Client()

    # Step 1: Read the CSV File from Input GCS Bucket
    input_blob = storage_client.bucket(input_bucket_name).blob('order_ids_20k.csv')
    input_csv_data = input_blob.download_as_text()
    df = pd.read_csv(io.StringIO(input_csv_data))

    # Select the first 100 IDs (you can adjust this based on your needs)
    ids_to_fetch = df["orderid"].tolist()#[:100]

    # Add the orderId column to the DataFrame
    # df['orderId'] = ids_to_fetch

    batch_size = 10  # You can adjust the batch size based on your API's rate limits
    responses = []

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, id) for id in ids_to_fetch]

        for i in range(0, len(tasks), batch_size):
            batch_responses = await asyncio.gather(*tasks[i:i+batch_size])
            responses.extend(batch_responses)

    # Remove None values from the responses (failed requests)
    responses = [response for response in responses if response is not None]

    for response, order_id in zip(responses, ids_to_fetch):
        response['orderId'] = order_id
    # print(responses)

    bq_project_id = 'XXXX-1330b'
    bq_dataset_id = 'XXXX_history_test'
    bq_table_id = 'offer_copy'
     # Authenticate with BigQuery
    keyfile_path = 'XXXX-1330b-56f6c52a17c5.json'
    creds = service_account.Credentials.from_service_account_file(keyfile_path)
    bq_client = bigquery.Client(project=bq_project_id, credentials=creds)

    # Define the BigQuery dataset and table
    dataset_ref = bq_client.dataset(bq_dataset_id)
    table_ref = dataset_ref.table(bq_table_id)

    # Write data to BigQuery
    job_config = bigquery.LoadJobConfig(
        #autodetect=True  # Detect schema automatically
        #source_format=bigquery.SourceFormat.CSV,
        #skip_leading_rows=1
    )
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    #return df.to_csv(index=False)

    job = bq_client.load_table_from_json(
        responses, table_ref, job_config=job_config
    )
    job.result()  # Wait for the job to complete

    print(f'Data loaded to BigQuery {bq_project_id}.{bq_dataset_id}.{bq_table_id} successfully!')
    

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())