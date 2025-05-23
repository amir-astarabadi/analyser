import pandas as pd
import numpy as np
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from helpers import dd
from math import sqrt




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
    df[independent_variable] = pd.to_numeric(df[independent_variable], errors='coerce')
    df[dependent_variable] = pd.to_numeric(df[dependent_variable], errors='coerce')
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

def histogram(dataset, independent_variable, category_variable=None, statistics='frequency'):
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}})
    variables = [v for v in [independent_variable, category_variable] if v is not None]
    
    df = df[variables]
    df[independent_variable] = pd.to_numeric(df[independent_variable], errors='coerce')
    df.dropna(inplace=True)
    
    df['bins'] = pd.cut(df[independent_variable], bins=10)
    total = df[independent_variable].notna().sum()
    
    result = {
        "xLabel":independent_variable,
        "yLabel":statistics,
        "categories":set(),
        'xAxis': [],
        "series":[]
    }
    
    if category_variable is not None:
        grouped = df.groupby([category_variable, 'bins'], observed=False).size().to_dict()
        series = {}
        for key, count in grouped.items():
            
            statistic = count
            category = key[0]
            interval = key[1]
            if statistics == 'percent':
                statistic = round((statistic / total) * 100, 3).__float__()
            elif statistics == 'density':
                bin_width = interval.right - interval.left
                statistic = round(statistic / (total * bin_width), 3).__float__()

            result['categories'].add(category)
            axis = f"{round(interval.left, 3)} , {round(interval.right, 3)}"
            if axis not in result['xAxis']:
                result['xAxis'].append(axis)
            
            if series.get(category) is None:
                series[category] = {
                    'name': category,
                    'data': []
                }

            series[category]['data'].append(statistic)
        
        for key, value in series.items():
            result['series'].append(value)
            
        return result
    else:
        del result['categories']
        grouped = df[['bins']].value_counts().sort_index().to_dict()
        result['series'].append({'data': []})
        for interval, count in grouped.items():
            interval = interval[0]
            statistic = count
            if statistics == 'percent':
                statistic = round((statistic / total) * 100, 3).__float__()
            elif statistics == 'density':
                bin_width = interval.right - interval.left
                statistic = round(statistic / (total * bin_width), 3).__float__()
            
            axis = f"{round(interval.left, 3)} , {round(interval.right, 3)}"
            if axis not in result['xAxis']:
                result['xAxis'].append(axis)
            result['series'][0]['data'].append(statistic)
        
        return result

def extract(dataset, replace_missing_values=False):
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 

    metadata = []
    df_rows = len(df)

    for index, col in enumerate(df.columns):
        if col in ['id', '_id'] or (index == 0 and len(pd.to_numeric(df[col], errors='coerce').unique()) == df_rows):
            continue
        d_type, parsed_col = _parse_col(df, col)

        if d_type == 'numeric':
            summary = summarise_numeric_col(parsed_col, d_type, replace_missing_values, col)

        elif d_type == 'date':
            summary = summerise_date_col(parsed_col, d_type, col)
        
        elif d_type == "categorical":
            summary = summerise_categorical_col(parsed_col, d_type, col)
        metadata.append(summary)

    return metadata

def _is_numeric(df, col):
    d_type, parsed_col = None, None
    parsed_col = pd.to_numeric(df[col], errors='coerce')

    numerator = parsed_col.notna().sum()
    denominator = df[col].isna().sum() 
    denominator = denominator if denominator > 0 else 1 

    if (numerator / denominator) > 0.1 and len(parsed_col.dropna().unique()) > 10:
        d_type = 'numeric'
    else:
        parsed_col = None
    
    return d_type, parsed_col

def _is_date(df, col):
    d_type, parsed_col = None, None

    parsed_col = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
    numerator = parsed_col.notna().sum()
    denominator = df[col].isna().sum() 
    denominator = denominator if denominator > 0 else 1 

    if (numerator / denominator) > 0.1 or len(parsed_col.dropna().unique()) > 10:
        d_type = 'date'
    else:
        parsed_col = None
    
    return d_type, parsed_col

def _distict_between_numeric_and_categorical(df, col):
    d_type, parsed_col = None, None

    parsed_col = df[col]
    if len(parsed_col.dropna().unique()) > 10:
        d_type = 'numeric'
    else:
        d_type = 'categorical'

    return d_type, parsed_col

def _parse_col(df, col):
    d_type = None
    parsed_col = None

    d_type, parsed_col = _is_numeric(df, col)    
    if parsed_col is None and d_type is None:
        d_type, parsed_col = _is_date(df, col)
    
    if parsed_col is None and d_type is None:
        d_type, parsed_col = _distict_between_numeric_and_categorical(df, col)
    
    return d_type, parsed_col

def summarise_numeric_col(parsed_col, d_type, replace_missing_values, col):
    average = float(parsed_col.mean())
    missing_values = int(parsed_col.isna().sum())
    bins = pd.cut(parsed_col, bins=10)
    freq_table = bins.value_counts().sort_index().to_dict()
    categories = {'keys':[], 'values':[]}
    for key, value in freq_table.items():
        if replace_missing_values and (round(key.left, 2) <= average < round(key.right, 2)):
            value += missing_values
        categories['keys'].append(f"{round(key.left, 2)} , {round(key.right, 2)}")
        categories['values'].append(int(value))
    var = np.var(parsed_col).__float__()
    return {
        "column": col,
        "type": d_type,
        "categories": categories,
        "missing": "replaced" if replace_missing_values else missing_values,
        "min": float(parsed_col.min()),
        "max": float(parsed_col.max()),
        "mean": average,
        "median": float(parsed_col.median()),
        "var": var,
        "std": sqrt(var),
    }

def summerise_date_col(parsed_col, d_type, col):
    bins = pd.cut(parsed_col, bins=10)
    freq_table = bins.value_counts().sort_index().to_dict()
    categories = {'keys':[], 'values':[]}
    for key, value in freq_table.items():
        categories['keys'].append(f"{str(key.left).split(' ')[0]} , {str(key.right).split(' ')[0]}")
        categories['values'].append(int(value))

    return {
        "column": col,
        "type": d_type,
        "categories": categories,
        "missing": int(parsed_col.isna().sum()),
        "min": parsed_col.min(),
        "max": parsed_col.max(),
    }

def summerise_categorical_col(parsed_col, d_type, col):
    categories = {'keys':[], 'values':[]}

    for key, value in parsed_col.value_counts().sort_index().to_dict().items():
        categories['keys'].append(key)
        categories['values'].append(int(value))

    return {
        "column": col,
        "type": d_type,
        "categories": categories,
        "missing": int(parsed_col.isna().sum()),
    } 


def bar(dataset, independent_variable, category_variable=None, statistic='frequency'):
    lookup_table = {
        'frequency': 'count',
        'percent': 'count',
        'mean': 'mean'
    }
    
    result = {
            "xLabel": independent_variable,
            "yLabel": statistic,
            "categories": set(),
            "xAxis": [],
            "series": [],
    }
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 
    
    variables = [v for v in [independent_variable, category_variable] if v is not None]
    df = df[variables].dropna()
    df[independent_variable] = pd.to_numeric(df[independent_variable], errors='coerce')
    
    if category_variable is None:
        del result['categories']
        result['series'].append({'data': []})
        
        df['bins'] = pd.cut(df[independent_variable], bins=10)
        statistics = df[independent_variable].agg(['mean', 'count', 'std','var', 'median', 'min', 'max']).to_dict()
                
        groups = df.groupby(['bins'],observed=False)[independent_variable].agg([lookup_table[statistic]]).reset_index()
        for group in groups['bins'].unique():
            point = f"{round(group.left, 2)} , {round(group.right, 2)}"
            result['xAxis'].append(point)
            group_df = groups[groups['bins'] == group]
            value = group_df[lookup_table[statistic]].iloc[0] if statistic != 'percent' else round((group_df[lookup_table[statistic]].iloc[0] / statistics['count']) * 100, 3).__float__()
            value = int(value) if statistic == "frequency" else round(value, 3).__float__()
            result['series'][0]['data'].append(value)
        result['series'][0]['statistics'] = statistics
        return result
    
    groups = df.groupby([category_variable],observed=False)[independent_variable].agg(['mean', 'count', 'std','var', 'median', 'min', 'max']).reset_index()
    
    for category in groups[category_variable].unique():
        result['categories'].add(category)
        if category not in result['xAxis']:
            result['xAxis'].append(category)
        category_df = groups[groups[category_variable] == category]
        statistics = category_df[['mean','count','std','var','median','min','max']].iloc[0].to_dict()
        statistics['count'] = int(statistics['count'])
        
        result['series'].append({
            'name': category_df[category_variable].iloc[0],
            'data': category_df[lookup_table[statistic]].iloc[0].tolist() if statistic != 'percent' else round((category_df[lookup_table[statistic]].iloc[0] / statistics['count']) * 100, 3).__float__(),
            'statistics': statistics
        })
    return result 