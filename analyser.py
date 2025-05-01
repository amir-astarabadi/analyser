from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def get_dataframe_from_mongo(query={}):
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME")
    collection_name = os.getenv("COLLECTION_NAME")
    client = MongoClient(mongo_uri) 
    db = client[db_name]
    collection = db[collection_name]

    cursor = collection.find(query)
    data = list(cursor)
    records = [doc['data'] for doc in data if 'data' in doc]

    return pd.DataFrame(records)

def extract(dataset):
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 
    metadata = []

    for col in df.columns:
        col_data = df[col]
        dtype = pd.api.types.infer_dtype(col_data, skipna=False)
        if pd.api.types.is_numeric_dtype(col_data):
            if col_data.nunique() <= 5:
                if col == "exercise_frequency_3":
                    top_values = col_data.value_counts().to_dict()
                top_values = col_data.value_counts().to_dict()
                summary = {
                    "column": col,
                    "type": "categorical (numeric labels)",
                    "top_values": top_values,
                    "missing": int(col_data.isna().sum()),
                    "unique_values": int(col_data.nunique())
                }
            else:
                summary = {
                    "column": col,
                    "type": "numeric",
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "mean": float(col_data.mean()),
                    "median": float(col_data.median()),
                    "missing": int(col_data.isna().sum()),
                    "unique_values": int(col_data.nunique())
                }
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            top_values = col_data.value_counts().head(10).to_dict()
            summary = {
                "column": col,
                "type": "date",
                "top_values": top_values,
                "missing": int(col_data.isna().sum()),
                "unique_values": int(col_data.nunique())
            }
        elif pd.api.types.is_string_dtype(col_data):
            top_values = col_data.value_counts().head(10).to_dict()
            summary = {
                "column": col,
                "type": "categorical",
                "top_values": top_values,
                "missing": int(col_data.isna().sum()),
                "unique_values": int(col_data.nunique())
            }
        else:
            summary = {
                "column": col,
                "type": dtype,
                "note": "Unhandled data type",
                "missing": int(col_data.isna().sum())
            }

        metadata.append(summary)

    return metadata