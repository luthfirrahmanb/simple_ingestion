from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Directory containing SQL files
SQL_DIR = "/app/sql_files/staging"  # Change this to your actual SQL files directory
PROJECT_ID = os.getenv("PROJECT_ID")  # Your GCP project ID
DATASET_ID = os.getenv("DATASET_ID") # BigQuery dataset ID
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # Path to your service account JSON file

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def execute_sql_file(file_path):
    """
    Reads and executes a SQL file in BigQuery.
    """
    try:
        # Read the SQL file
        with open(file_path, 'r') as file:
            sql_query = file.read()

        # Run the SQL query
        print(f"Executing SQL from {file_path}")
        query_job = client.query(sql_query)  # Make an API request to run the query
        query_job.result()  # Wait for the job to complete

        print(f"Successfully executed {file_path}")
    except Exception as e:
        print(f"Error executing {file_path}: {e}")

def execute_all_sql_files():
    """
    Executes all SQL files in the specified directory.
    """
    # Get a list of all .sql files in the directory
    sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith('.sql')]
    
    if not sql_files:
        print("No SQL files found in the directory.")
        return

    # Execute each SQL file
    for sql_file in sql_files:
        file_path = os.path.join(SQL_DIR, sql_file)
        execute_sql_file(file_path)

if __name__ == "__main__":
    execute_all_sql_files()