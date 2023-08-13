from google.cloud import bigquery
from google.cloud import storage

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/nguyentuanduong7/airflow/src')))
from plugin.Sql_Queries import Query_Bigquery_DataWarehouse_Tiki, Query_Bigquery_DataWarehouse_Newegg
import json

bigquery_client = bigquery.Client()
storage_client = storage.Client()
DATASET_BQ = 'Data_db'
BUCKET_NAME = "project6-backup"
FILE_GCS_prefix = "/project6-backup/"
credentials_path = '/home/nguyentuanduong7/airflow/credentials.json'
bq_client = bigquery.Client.from_service_account_json(credentials_path)

def GCS_to_Bigquery_Staging():
    blobs = storage_client.list_blobs(BUCKET_NAME)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,  # Enable schema auto-detection
        max_bad_records=50000,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Specify the format of the error rows to be recorded
    )
    for blob in blobs:
        if blob.name.endswith(".json"):
            TABLE_BQ = blob.name.replace(".json", "")
            dataset_ref = bq_client.dataset(DATASET_BQ)
            table_ref = dataset_ref.table(TABLE_BQ)
            gcs_uri = f'gs://{BUCKET_NAME}/{blob.name}'
            load_job = bq_client.load_table_from_uri(
                gcs_uri, location="US", destination=table_ref, job_config=job_config
            )
            load_job.result()
            if load_job.errors:
                for error in load_job.errors:
                    if "json" in error:
                        print("Error Row:")
                        print(
                            json.dumps(error["json"], indent=2)
                        )  # Print the JSON data of the error row
                        print("-" * 50)  # Separator between error rows
                    else:
                        print("Error Row does not contain JSON data.")
            else:
                print("Data loaded to BigQuery table successfully!")

def Bigquery_DataWarehouse_Tiki():
    Table_Tiki_Staging = "Tiki_Staging"
    Table_Tiki_Warehouse = "Tiki_Warehouse"
    PROJECT_BQ = "project6-airflow"
    Tiki_Warehouse = f"{PROJECT_BQ}.{DATASET_BQ}.{Table_Tiki_Warehouse}"
    Tiki_Staging = f"{PROJECT_BQ}.{DATASET_BQ}.{Table_Tiki_Staging}"
    query = Query_Bigquery_DataWarehouse_Tiki(Tiki_Warehouse, Tiki_Staging)
    query_job = bq_client.query(query)
    query_job.result()

def Bigquery_DataWarehouse_Newegg():
    Table_Newegg = "Newegg_Staging"
    query = Query_Bigquery_DataWarehouse_Newegg(Table_Newegg)
    query_job = bq_client.query(query)
    query_job.result()  # Wait for the job to complete

