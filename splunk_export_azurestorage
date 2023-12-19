import requests
import json
import requests.sessions
import time
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, StandardBlobTier
import os

# Define Azure Storage connection parameters
AZURE_STORAGE_CONNECTION_STRING = 'ABC'  # Replace with your Azure Storage connection string
CONTAINER_NAME = 'ABC'  # Replace with the name of your Azure Blob Storage container

# Define Splunk connection parameters
HOST = 'ENVIRONMENT '
PORT = 8089
TOKEN = 'SPLUNK TOKEN'

# Define the base URL for the Splunk REST API
BASE_URL = f'https://{HOST}:{PORT}'

# Define the search endpoint to initiate a search job
SEARCH_JOB_ENDPOINT = '/services/search/jobs'

# Define headers (including authentication token)
HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'Content-Type' : 'application/x-www-form-urlencoded'
}

# Define the time range (from 1st Aug 2023 to 1st Sept 2023)
start_time = datetime(2023, 8, 1)
end_time = datetime(2023, 9, 1)

# Define the time interval (5 minutes)
interval = timedelta(minutes=5)

# Check if there is a progress file indicating where to resume
try:
    with open('progress.txt', 'r') as progress_file:
        progress = int(progress_file.read().strip())
except FileNotFoundError:
    progress = 0

# Generate search queries with time range and interval
search_queries = []
current_time = start_time
i=0

while current_time < end_time:
    i += 1
    if i <= progress:
        current_time += interval
        continue

    next_time = current_time + interval
    search_query = f'search index="tt_azure" earliest="{current_time.strftime("%m/%d/%Y:%H:%M:%S")}" latest="{next_time.strftime("%m/%d/%Y:%H:%M:%S")}"'
    search_queries.append((search_query, current_time, next_time))
    current_time = next_time

# Create a BlobServiceClient instance using the connection string
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

for i, (search_query, start_time, end_time) in enumerate(search_queries, start=1):
    try:
        # Define request payload (search query)
        payload = {
            'search': search_query,
            'output_mode': 'json'
        }
        # Make the POST request to initiate the search job
        response = requests.post(
            f'{BASE_URL}/services/search/jobs',
            headers=HEADERS,
            data=payload,
            verify=False  # Disabling SSL verification for simplicity
        )

        # Check if the request was successful (201 status code)
        if response.status_code == 201:
            # Extract the job SID from the response
            job_sid = response.json()['sid']

            # Define the search results endpoint using the job SID
            SEARCH_RESULTS_ENDPOINT = f'/services/search/jobs/{job_sid}/results'

            search_results_response = None
            while search_results_response is None or search_results_response.status_code != 200:
                # Make a GET request to retrieve search results
                search_results_response = requests.get(
                    f'{BASE_URL}{SEARCH_RESULTS_ENDPOINT}',
                    headers=HEADERS,
                    data={'output_mode': 'json'},
                    verify=False  # Disabling SSL verification for simplicity
                )
                if search_results_response.status_code != 200:
                    if search_results_response.status_code == 404:
                        # Save progress
                        with open('progress_azurefeb2.txt', 'w') as progress_file:
                            progress_file.write(str(i))
                        break
                    print(f'Waiting for search results (status code: {search_results_response.status_code})...')
                    time.sleep(10)  # Wait for 10 seconds before retrying

                # Define the output file name with query, start time, and end time
                query_name = search_query.split('search index=')[1].split(' earliest=')[0]
                start_time_str = start_time.strftime('%Y%m%d%H%M%S')
                end_time_str = end_time.strftime('%Y%m%d%H%M%S')
                output_file_name = f'output_{query_name}_{start_time_str}_to_{end_time_str}.json'

                # Create a BlobClient instance for the output file
                blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=output_file_name)

                # Upload the search results to Azure Blob Storage
                blob_client.upload_blob(search_results_response.content, overwrite=True)
                
                                
                print(f'Search {i} results uploaded to Azure Blob Storage: {output_file_name}')

        else:
            print(f'Error initiating search job. Status code: {response.status_code}')
            print(f'Response content: {response.content}')
    
    
    except Exception as e:
        print(f'Error: {e}')

# Cleanup progress file once all searches are completed
try:
    os.remove('progress_azurefeb2.txt')
except FileNotFoundError:
    pass
