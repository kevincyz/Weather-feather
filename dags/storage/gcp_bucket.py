from google.cloud import storage
import os
from dotenv import load_dotenv

load_dotenv()

def upload_to_bucket(bucket_name, file_path):
    client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(file_path))
    blob.upload_from_filename(file_path)
    return blob.public_url
