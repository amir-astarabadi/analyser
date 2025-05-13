import pandas as pd
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
    result = []
    
    if category_variable is None:
        for col in df.columns:
            if col in [dependent_variable, independent_variable]:
                result.append({
                    "dependents": df[col].tolist()
                })
            else:
                continue
        
        return result
    
    for category, group in df.groupby(category_variable):
        result.append({
            "category": category,
            "independents": group[independent_variable].tolist(),
            "dependents": group[dependent_variable].tolist()
        })
    return result

def extract(dataset, replace_missing_values=False):
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 

    metadata = []

    for col in df.columns:
        d_type = None
        parsed_col = None
        print(col)

        if parsed_col is None and d_type is None:
            parsed_col = pd.to_numeric(df[col], errors='coerce')
            numerator = parsed_col.notna().sum()
            denominator = parsed_col.isna().sum() 
            denominator = denominator if denominator > 0 else 1 
            if (numerator / denominator) > 0.1 and len(parsed_col.dropna().unique()) > 5:
                d_type = 'numeric'
            else:
                parsed_col = None
        
        if parsed_col is None and d_type is None:
            parsed_col = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            numerator = parsed_col.notna().sum()
            denominator = parsed_col.isna().sum() 
            denominator = denominator if denominator > 0 else 1 
            if (numerator / denominator) > 0.1:
                d_type = 'date'
            else:
                parsed_col = None
        
        if parsed_col is None and d_type is None:
            d_type = 'categorical'
            parsed_col = df[col]
        

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
