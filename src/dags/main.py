
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/nguyentuanduong7/ETL-Airflow-Process/src/')))

from plugin.Extract_Data import Fetch_data_From_Mysql, Fetch_data_From_Mongodb, Local_data_to_GCS
from plugin.Transform_Data import (
    GCS_to_Bigquery_Staging,
    Bigquery_DataWarehouse_Tiki,
    Bigquery_DataWarehouse_Newegg
)
from plugin.Load_Data import Bigquery_Datamart_Tiki, Bigquery_Datamart_Newegg

default_args = {
    'owner': 'Duong',
    'depends_on_past': False,
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["nguyentuanduong555@gmail.com"],
    "start_date": datetime(2023, 8, 11),
    'retries': 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="Project6-airflow",
    schedule_interval="0 7 * * *"
) as dag:
    MySqL_Fetch_data = PythonOperator(
        task_id="Fetch_data_From_Mysql",
        python_callable=Fetch_data_From_Mysql,
    )
    Mongodb_Fetch_data = PythonOperator(
        task_id="Fetch_data_From_Mongodb",
        python_callable=Fetch_data_From_Mongodb,
    )
    Local_data_to_GCS = PythonOperator(
        task_id="Local_data_to_GCS",
        python_callable=Local_data_to_GCS,
    )
    GCS_to_Bigquery_Staging = PythonOperator(
        task_id="GCS_to_Bigquery_Staging",
        python_callable=GCS_to_Bigquery_Staging,
    )
    Tiki_Bigquery_DataWarehouse = PythonOperator(
        task_id="Tiki_Bigquery_DataWarehouse",
        python_callable=Bigquery_DataWarehouse_Tiki,
    )
    Newegg_Bigquery_DataWarehouse = PythonOperator(
        task_id="Newegg_Bigquery_DataWarehouse",
        python_callable=Bigquery_DataWarehouse_Newegg,
    )
    Tiki_Bigquery_Datamart = PythonOperator(
        task_id="Tiki_Bigquery_Datamart",
        python_callable=Bigquery_Datamart_Tiki,
    )
    Newegg_Bigquery_Datamart = PythonOperator(
        task_id="Newegg_Bigquery_Datamart",
        python_callable=Bigquery_Datamart_Newegg,
    )

    Successful_project = EmailOperator(
        task_id="Successful_project",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>test email notification</h1>"""
    )

    # test_function = BashOperator(
    #     task_id="test_function",
    #     bash_command="cd nonexist_folder"
    # )

MySqL_Fetch_data >> Mongodb_Fetch_data >> Local_data_to_GCS >> GCS_to_Bigquery_Staging

GCS_to_Bigquery_Staging >> Tiki_Bigquery_DataWarehouse >> Tiki_Bigquery_Datamart >> Successful_Alert_Project

Tiki_Bigquery_DataWarehouse >> Newegg_Bigquery_DataWarehouse >> Newegg_Bigquery_Datamart >> Successful_Alert_Project
