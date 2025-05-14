import pandas as pd
import numpy as np
from pymongo import MongoClient
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

    cursor = collection.find(query).sort({ 'row_index': 1 })
    data = list(cursor)
    records = [doc['data'] for doc in data if 'data' in doc]

    return pd.DataFrame(records)

def line(dataset, independent_variable, dependent_variable, category_variable=None):
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}})
    df = df.dropna(subset=[independent_variable, dependent_variable, category_variable]) if category_variable is not None else df.dropna(subset=[independent_variable, dependent_variable])
    df.sort_values(by=independent_variable, inplace=True)
    result = {
        "xLabel":independent_variable,
        "yLabel":dependent_variable,
        "categories":[],
        "series":[]
    }
    
    if category_variable is None:
        del result['categories']
        result['series'].append({
            "name": f"{dependent_variable} base {independent_variable}",
            "data": df[[independent_variable, dependent_variable]].to_numpy().tolist(),
        })
        return result

    
    for category, group in df.groupby(category_variable):
        result['categories'].append(category)
        result['series'].append({
            "name": category,
            "data": group[[independent_variable, dependent_variable]].to_numpy().tolist(),
        })
    return result

def extract(dataset, replace_missing_values=False):
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 

    metadata = []

    for col in df.columns:
        d_type = None
        parsed_col = None

        if parsed_col is None and d_type is None:
            parsed_col = pd.to_numeric(df[col], errors='coerce')

            numerator = parsed_col.notna().sum()
            denominator = df[col].isna().sum() 
            denominator = denominator if denominator > 0 else 1 
 
            if (numerator / denominator) > 0.1 and len(parsed_col.dropna().unique()) > 10:
                d_type = 'numeric'
            else:
                parsed_col = None
        
        if parsed_col is None and d_type is None:
            parsed_col = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            numerator = parsed_col.notna().sum()
            denominator = df[col].isna().sum() 
            denominator = denominator if denominator > 0 else 1 
            if (numerator / denominator) > 0.1 or len(parsed_col.dropna().unique()) > 10:
                d_type = 'date'
            else:
                parsed_col = None
        
        if parsed_col is None and d_type is None:
            parsed_col = df[col]
            if len(parsed_col.dropna().unique()) > 10:
                d_type = 'numeric'
            else:
                d_type = 'categorical'
        
        if d_type == 'numeric':
            average = float(parsed_col.min())
            missing_values = int(parsed_col.isna().sum())
            bins = pd.cut(parsed_col, bins=10)
            freq_table = bins.value_counts().sort_index().to_dict()
            categories = {'keys':[], 'values':[]}
            for key, value in freq_table.items():
                if replace_missing_values and (round(key.left, 2) < average < round(key.right, 2)):
                    value += missing_values
                categories['keys'].append(f"{round(key.left, 2)} , {round(key.right, 2)}")
                categories['values'].append(value)
            summary = {
                "column": col,
                "type": d_type,
                "categories": categories,
                "missing": "replaced" if replace_missing_values else missing_values,
                "min": average,
                "max": float(parsed_col.max()),
                "mean": float(parsed_col.mean()),
                "median": float(parsed_col.median()),
            }

        elif d_type == 'date':
            bins = pd.cut(parsed_col, bins=10)
            freq_table = bins.value_counts().sort_index().to_dict()
            categories = {'keys':[], 'values':[]}
            for key, value in freq_table.items():
                categories['keys'].append(f"{str(key.left).split(' ')[0]} , {str(key.right).split(' ')[0]}")
                categories['values'].append(value)

            summary = {
                "column": col,
                "type": d_type,
                "categories": categories,
                "missing": int(parsed_col.isna().sum()),
                "min": parsed_col.min(),
                "max": parsed_col.max(),
            }
        
        elif d_type == "categorical":
            categories = {'keys':[], 'values':[]}
       
            for key, value in parsed_col.value_counts().sort_index().to_dict().items():
                categories['keys'].append(key)
                categories['values'].append(value)

            summary = {
                "column": col,
                "type": d_type,
                "categories": categories,
                "missing": int(parsed_col.isna().sum()),
            }   
        metadata.append(summary)

    return metadata
