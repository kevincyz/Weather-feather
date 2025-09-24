
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import psycopg2
from datetime import datetime, timezone, timedelta

#
PROJECT_ID = "prime-agency-456202-b7"
PROCESSED_BUCKET_NAME = "ece-590-group2-processed"
DB_CONFIG = {
    'dbname': "noaa",
    'user': "postgres",
    'password': "final-project",
    'host': "35.202.11.58",
    'connect_timeout': 10
}
AGE_THRESHOLD_DAYS = 30  # Any value you want

def cleanup_old_files():
    now = datetime.now(timezone.utc)
    cutoff_time = now - timedelta(days=AGE_THRESHOLD_DAYS)

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    blobs = gcs_hook.list(bucket_name=PROCESSED_BUCKET_NAME)

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for blob_name in blobs:
            blob_metadata = gcs_hook.get_blob(bucket_name=PROCESSED_BUCKET_NAME, object_name=blob_name)
            if blob_metadata.updated < cutoff_time:
                print(f"Deleting old file: {blob_name}")

                parts = blob_name.split('/')
                filename = parts[-1]
                name_parts = filename.replace(".json", "").split("_")
                
                if len(name_parts) < 3:
                    print(f"Skipping malformed filename: {filename}")
                    continue

                user_id, station_id, *date_parts = name_parts
                date_range = "_".join(date_parts).replace("_to_", " to ")

                cursor.execute(
                    """
                    DELETE FROM data_metadata
                    WHERE station_id = %s AND user_accessed = %s AND date_range = %s
                    """,
                    (station_id, user_id, date_range)
                )

                # Delete GCS file
                gcs_hook.delete(bucket_name=PROCESSED_BUCKET_NAME, object_name=blob_name)
                print(f"Deleted {blob_name} and corresponding metadata.")

        conn.commit()

    except Exception as e:
        print(f"Cleanup error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="cleanup_old_files",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval="0 0 * * *",  # Every midnight
    catchup=False,
    tags=["maintenance"]
) as dag:

    clean_task = PythonOperator(
        task_id="cleanup_old_files_task",
        python_callable=cleanup_old_files
    )
