from sqlalchemy import create_engine, Column, Integer, String, JSON, Float, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import json
import subprocess
import os
import glob
USERNAME = "nguyentuanduong7"
PASSWORD = "882489"
HOST = "localhost"
DATABASE = "Newegg_db"
TABLE = "Prod_Info"
PORT = "3306"

PROJECT_GCS = "project6-backup"
FILE_GCS = "Newegg_db.json"

DATASET_BQ = "Data_db"
TABLE_BQ = "Newegg_table"

client = MongoClient("mongodb://localhost:27017/")
DB_Name = "Tiki_db"
Col_Name = "Tiki_col"

def Fetch_data_From_Mysql():
    File = "/home/nguyentuanduong7/ETL-Airflow-Process/data/Newegg_Staging.json"
    f = open(File, "w")
    engine = create_engine(
        f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )
    Data = text(f"SELECT * FROM {TABLE}")
    connection = engine.connect()
    result = connection.execute(Data)
    count = 0
    for row in result.mappings():
        with open(File, "a", encoding="utf-8") as f:
            row_dict = dict(row)
            row_dict["Specs_data"] = str(row_dict["Specs_data"]).replace('"', "")
            f.write(json.dumps(row_dict, ensure_ascii=False) + "\n")
            count += 1
    return "DONE"

def Fetch_data_From_Mongodb():
    File = "/home/nguyentuanduong7/ETL-Airflow-Process/data/Tiki_Staging.json"
    f = open(File, "w")
    Database = client[DB_Name]
    collection = Database[Col_Name]
    cursor = collection.find()
    Data = []
    count = 0
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        # ... (other field modifications)
        with open(File, "a", encoding="utf-8") as f:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count += 1
            if count == 1000:
                return "Done"

def Local_data_to_GCS():
    folder_path = "/home/nguyentuanduong7/ETL-Airflow-Process/airflow/data"
    extension = "*.json"
    file_list = glob.glob(os.path.join(folder_path, extension))
    for filename in file_list:
        bucket_name = "project6-backup"
        gcs_name = filename.replace("/home/nguyentuanduong7/ETL-Airflow-Process/airflow/data/", "")
        gsutil_command = [
            "gsutil",
            "-o",
            "GSUtil:parallel_composite_upload_threshold=150M",
            "-m",
            "cp",
            filename,
            f"gs://{bucket_name}/{gcs_name}",
        ]
        subprocess.run(gsutil_command, check=True)

