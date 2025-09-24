from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from fetchers.landsat import fetch_landsat_data
from fetchers.noaa import fetch_noaa_data
from fetchers.mtbs import fetch_mtbs_data
from fetchers.nifc import fetch_nifc_data

from db.mongo import store_mongo_data
from db.postgres import store_postgres_data, store_metadata
from storage.gcp_bucket import upload_to_bucket

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='weekly_fire_data_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 4, 1),
    schedule_interval='@weekly',
    catchup=False
)

# Configs — can eventually be templated from Variable or Param
user_id = "system"
location = "California"
date_range = {"start": "2024-01-01", "end": "2024-12-31"}

def run_landsat():
    data = fetch_landsat_data(location, date_range)
    store_mongo_data(data)
    store_metadata(user_id, "LANDSAT", location, date_range, "MISS")
    upload_to_bucket(os.getenv("GCP_BUCKET_RAW"), "/tmp/landsat_data.json")

def run_noaa():
    data = fetch_noaa_data(location, date_range)
    store_postgres_data(data, "NOAA")
    store_metadata(user_id, "NOAA", location, date_range, "MISS")
    upload_to_bucket(os.getenv("GCP_BUCKET_RAW"), "/tmp/noaa_data.json")

def run_mtbs():
    data = fetch_mtbs_data(location, date_range)
    store_postgres_data(data, "MTBS")
    store_metadata(user_id, "MTBS", location, date_range, "MISS")
    upload_to_bucket(os.getenv("GCP_BUCKET_RAW"), "/tmp/mtbs_data.json")

def run_nifc():
    data = fetch_nifc_data(location, date_range)
    store_postgres_data(data, "NIFC")
    store_metadata(user_id, "NIFC", location, date_range, "MISS")
    upload_to_bucket(os.getenv("GCP_BUCKET_RAW"), "/tmp/nifc_data.json")

# Tasks
landsat_task = PythonOperator(task_id="landsat_ingestion", python_callable=run_landsat, dag=dag)
noaa_task = PythonOperator(task_id="noaa_ingestion", python_callable=run_noaa, dag=dag)
mtbs_task = PythonOperator(task_id="mtbs_ingestion", python_callable=run_mtbs, dag=dag)
nifc_task = PythonOperator(task_id="nifc_ingestion", python_callable=run_nifc, dag=dag)

landsat_task >> [noaa_task, mtbs_task, nifc_task]
