from pymongo import MongoClient
from typing import List, Dict, Any
from src import env

def insert_transactions_to_db(transactions: List[Dict[str, Any]]):
    client = MongoClient(env.MONGODB_URI)  # Update with your MongoDB URI
    db = client[env.DB_NAME]  # Database name
    collection = db[get_effective_collection_name()]  # Collection name
    if transactions:
        collection.insert_many(transactions)  # Insert all transactions
    client.close()

def get_effective_collection_name() -> str:
    base = env.COLLECTION_NAME
    if (env.ENV or "").strip().lower() == "dev":
        return f"{base}_dev"
    return base