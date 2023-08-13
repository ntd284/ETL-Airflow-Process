from google.cloud import bigquery
from google.cloud import storage
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/nguyentuanduong7/ETL-Airflow-Process/src')))

from plugin.Sql_Queries import (
    Query_Update_Bigquery_Datamart_Tiki,
    Query_Create_Bigquery_Datamart_Tiki,
    Query_Update_Bigquery_Datamart_Newegg,
    Bigquery_Create_Datamart_Newegg,
)
import json

PROEJECT_BQ = 'project6-airflow'
DATASET_BQ = 'Data_db'
credentials_path = '/home/nguyentuanduong7/ETL-Airflow-Process/credentials.json'
bq_client = bigquery.Client.from_service_account_json(credentials_path)

def Bigquery_Datamart_Tiki():
    Table_Tiki_Warehouse = "Tiki_Warehouse"
    Table_Tiki_Datamart = "Tiki_Datamart"
    Dataset = bq_client.dataset(DATASET_BQ)

    Table_Warehouse = Dataset.table(Table_Tiki_Warehouse)
    Table_Datamart = Dataset.table(Table_Tiki_Datamart)

    Table_Warehouse_Dic = bq_client.get_table(Table_Warehouse)

    Table_Tiki_Warehouse_sample = f"{PROEJECT_BQ}.{DATASET_BQ}.{Table_Tiki_Warehouse}"
    Table_Tiki_Datamart_sample = f"{PROEJECT_BQ}.{DATASET_BQ}.{Table_Tiki_Datamart}"

    try:
        Table_Datamart_Dic = bq_client.get_table(Table_Datamart)
        if Table_Tiki_Datamart_sample in str(Table_Datamart_Dic):
            query = Query_Update_Bigquery_Datamart_Tiki(Table_Datamart_Dic, Table_Warehouse_Dic)
    except:
        query = Query_Create_Bigquery_Datamart_Tiki(Table_Tiki_Datamart_sample, Table_Warehouse_Dic)
    query_job = bq_client.query(query)
    query_job.result()  # Wait for the job to complete

def Bigquery_Datamart_Newegg():
    Table_Newegg_Warehouse = "Newegg_Warehouse"
    Table_Newegg_Datamart = "Newegg_Datamart"
    Dataset = bq_client.dataset(DATASET_BQ)

    Table_Warehouse = Dataset.table(Table_Newegg_Warehouse)
    Table_Datamart = Dataset.table(Table_Newegg_Datamart)

    Table_Warehouse_Dic = bq_client.get_table(Table_Warehouse)

    Table_Newegg_Warehouse_sample = f"{PROEJECT_BQ}.{DATASET_BQ}.{Table_Newegg_Warehouse}"
    Table_Newegg_Datamart_sample = f"{PROEJECT_BQ}.{DATASET_BQ}.{Table_Newegg_Datamart}"

    try:
        Table_Datamart_Dic = bq_client.get_table(Table_Datamart)
        if Table_Newegg_Datamart_sample in str(Table_Datamart_Dic):
            query = Query_Update_Bigquery_Datamart_Newegg(Table_Datamart_Dic, Table_Warehouse_Dic)
    except:
        query = Bigquery_Create_Datamart_Newegg(Table_Newegg_Datamart_sample, Table_Warehouse_Dic)
    query_job = bq_client.query(query)
    query_job.result()  # Wait for the job to complete
