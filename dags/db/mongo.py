from pymongo import MongoClient
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["landsat_data"]
collection = db["images"]

def store_mongo_data(data):
    data["stored_at"] = datetime.utcnow()
    collection.insert_one(data)
