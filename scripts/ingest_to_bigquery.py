from google.cloud import bigquery
import os
import glob
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account

# Configuration
DATA_DIR = "/app/data"  # Directory with your CSV files
PROJECT_ID = os.getenv("PROJECT_ID")  # Your GCP project ID
DATASET_ID = os.getenv("DATASET_ID") # BigQuery dataset ID
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # Path to your service account JSON file

# Function to upload CSV to BigQuery
def upload_csv_to_bigquery(file_path):
    table_id = os.path.basename(file_path).replace('.csv', '')  # Use the CSV file name as table name
    
    # Load CSV file into DataFrame
    df = pd.read_csv(file_path)
    df['ingestion_date'] = datetime.now()  # Add ingestion date to the DataFrame
    
    # Authenticate and initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    # Define the BigQuery table
    table_ref = client.dataset(DATASET_ID).table(table_id)
    
    # Upload the DataFrame to BigQuery
    df.to_gbq(destination_table=f"{DATASET_ID}.{table_id}", project_id=PROJECT_ID, if_exists='append', credentials=credentials)
    print(f"Loaded {file_path} to BigQuery table {DATASET_ID}.{table_id}")

# Function to process multiple CSV files
def ingest_csv_files():
    # Find all CSV files in the specified directory
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not csv_files:
        print("No CSV files found in the directory.")
        return
    
    # Upload each CSV file to BigQuery
    for file_path in csv_files:
        upload_csv_to_bigquery(file_path)

# Main script
if __name__ == "__main__":
    ingest_csv_files()
