# HITTING THE WEBSOCKET AND SAVING THE REPONSES IN GCS BUCKET IN TWO DIFFERENT FILES AS PRE_ACCUMULATE FILE AND ACCUMULATED FILE
import nest_asyncio
nest_asyncio.apply()

import asyncio
import datetime
import json
import websockets
from websockets.exceptions import ConnectionClosedError
import pandas as pd
from google.cloud import storage
import io
import os

ACCUMULATED_DATA_BUCKET_NAME = "XXXX_websocket_data"
PRE_ACCUMULATED_DATA_BUCKET_NAME = "XXXX_websocket_stage"
PRE_ACCUMULATED_FILENAME = "pre_accumulated_data.csv"
ACCUMULATED_FILENAME = "Accumulated_data.csv"

accumulated_data = []

async def keep_alive(websocket):
    while True:
        await asyncio.sleep(60)
        try:
            await websocket.send('{"type": "keepAlive"}')
            print("Sent keep-alive")
        except ConnectionClosedError as e:
            print(f"Connection closed while sending keep-alive: {e}")
            break

async def connect_to_websocket():
    while True:
        try:
            async with websockets.connect("wss://api.aggregator.app/dispatch/v1/notification/") as websocket:
                await websocket.send('{"tracking": true}')
                asyncio.create_task(keep_alive(websocket))
                
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")  # Use a clean timestamp
                        order_id = data.get('orderId', 'N/A')
                        print(f"{timestamp} - Received data: {data}")
                        
                        # Accumulate the data in the list
                        accumulated_data.append(data)
                        
                    except ConnectionClosedError as e:
                        print(f"Connection closed while receiving message: {e}")
                        break
                
                # When the WebSocket connection is closed, write the accumulated data to CSV
                if accumulated_data:
                    write_data_to_csv(accumulated_data, timestamp)
                    accumulated_data.clear()
                        
        except Exception as e:
            print(f"An error occurred: {e}. Retrying...")

def write_data_to_csv(data, timestamp):
    # Create a DataFrame from the accumulated data
    df = pd.DataFrame(data)

    # Write the DataFrame to a CSV file, overwriting it
    # pre_local_filename = f"pre_accumulated_data_{timestamp}.csv" 
    pre_local_filename = PRE_ACCUMULATED_FILENAME
    df.to_csv(pre_local_filename, mode='w', index=False)
    
    # Determine the file path for the accumulated CSV file
    # local_filename = ACCUMULATED_FILENAME
    local_filename = f"Accumulated_data_{timestamp}.csv"
    if not os.path.exists(local_filename):
        # If the accumulated CSV file doesn't exist, create it and append data from pre_accumulated_data.csv
        df.to_csv(local_filename,index=False)
        print(f"Creating accumulated data file and writing data: {local_filename}")
    else:
        # If the accumulated CSV file exists, append data from pre_accumulated_data.csv
        existing_df = pd.read_csv(local_filename)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_csv(local_filename, mode='w', index=False)
        print(f"Appending data to accumulated data file: {local_filename}")
    
    # Initialise Google Cloud Storage Client
    client = storage.Client()
    
    accumulated_bucket = client.get_bucket(ACCUMULATED_DATA_BUCKET_NAME)
    pre_accumulated_bucket = client.get_bucket(PRE_ACCUMULATED_DATA_BUCKET_NAME)
    
    # Upload the accumulated.csv file to Google Cloud Storage
    accumulated_blob = accumulated_bucket.blob(local_filename)
    accumulated_blob.upload_from_filename(local_filename)
    print(f"Accumulated data file {local_filename} has been uploaded to cloud storage {ACCUMULATED_DATA_BUCKET_NAME} ")

    # Upload "pre_accumulated.csv" file to Google Cloud Storage
    pre_accumulated_blob = pre_accumulated_bucket.blob(PRE_ACCUMULATED_FILENAME)
    pre_accumulated_blob.upload_from_filename(pre_local_filename)
    print(f"Pre-accumulated data file {PRE_ACCUMULATED_FILENAME} has been uploaded to cloud storage {PRE_ACCUMULATED_DATA_BUCKET_NAME} ")

async def main():
    await asyncio.gather(connect_to_websocket())

asyncio.run(main())