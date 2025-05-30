import pandas as pd
import numpy as np
from scipy.stats import gaussian_kde
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from helpers import dd, round_float, density_curve
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
        data = df[[independent_variable, dependent_variable]].to_numpy().tolist()
        result['series'].append({
            "name": f"{dependent_variable} base {independent_variable}",
            "data": [[round_float(i[0]), round_float(i[-1])] for i in data],
        })
        return result

    for category, group in df.groupby(category_variable):
        data = group[[independent_variable, dependent_variable]].to_numpy().tolist() 
        result['categories'].append(category)
        result['series'].append({
            "name": category,
            "data": [[round_float(i[0]), round_float(i[-1])] for i in data],
        })
    return result

def histogram(dataset, independent_variable, category_variable=None, statistic='frequency'):
    lookup_table = {
        'frequency': 'count',
        'percent': 'count',
        'density': 'count'
    }
    
    result = {
            "xLabel": independent_variable,
            "yLabel": statistic,
            "xAxis": [],
            "series": [],
            "statistics": dict(),
            "categories": [],
    }
    if statistic == 'density':
        result["density_curve"] = []
    
    necessary_columns = [col for col in ['$$independent_variable', category_variable] if col is not None]
    
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 
    df['$$independent_variable'] = df[independent_variable]
    df = df[necessary_columns]
    if category_variable is not None and df[category_variable].any():
        df['$$category_variable'] = df[category_variable]
        del df[category_variable]
    

    d_type, parsed_col = _parse_col(df, '$$independent_variable')
    del parsed_col

    if d_type == 'categorical':
        df['$$bins'] = df['$$independent_variable']
        statistics = ['count']
        result['statistics']['overall_statitis'] = df['$$independent_variable'].agg(statistics).to_dict()
    else :
        df['$$independent_variable'] = pd.to_numeric(df['$$independent_variable'], errors='coerce')
        df['$$bins'] = pd.cut(df['$$independent_variable'], bins=10)
        statistics = [ 'count', 'std','var', 'median', 'min', 'max', 'mean']
    
    df.dropna(inplace=True)
    
    overall_statitis =  df['$$independent_variable'].agg(statistics).to_dict()
    for key, value in overall_statitis.items():
        overall_statitis[key] = round_float(value)
    result['statistics']['overall_statitis'] = overall_statitis

    if category_variable :
        groups = df.groupby(['$$bins', '$$category_variable'],observed=False)['$$independent_variable'].agg(statistics).reset_index()
    else:
        if statistic == 'density' and d_type == 'numeric':
            result['density_curve'] = density_curve(df['$$independent_variable'])
        groups = df.groupby('$$bins',observed=False)['$$independent_variable'].agg(statistics).reset_index()
    data = []
    series = dict()
    result['categories'] = groups['$$category_variable'].unique() if category_variable else []
        
    result['categories'] = sorted([b for b in result['categories'] if b is not None])
    result['xAxis'] = sorted([b for b in df['$$bins'].unique() if b is not None])
    series = dict()
    calculated_density_curves = set()
    for b in result['xAxis']:
        if category_variable:
            for cat in result['categories']:
                
                if statistic == 'density' and d_type == 'numeric' and cat not in calculated_density_curves:
                    result['density_curve'].append({
                        'name':cat,
                        'data': density_curve(df[(df['$$category_variable'] == cat) & (df['$$bins'] == b)]['$$independent_variable'])
                    })  
                    calculated_density_curves.add(cat)
                    
                row = groups[(groups['$$bins'] == b) & (groups['$$category_variable'] == cat)]
                if not row.empty:
                    stat = round_float(row.iloc[0][lookup_table[statistic]])
                    if statistic == 'percent':
                        stat = (stat / overall_statitis['count']) * 100
                    elif statistic == 'density':
                        stat = (stat / overall_statitis['count'])
                    stat = round_float(stat)

                else:
                    stat = 0
                
                if not series.get(cat):
                    series[cat] = {
                        'name': cat,
                        'data': []
                    }
                
                series[cat]['data'].append(stat)
                                    
        else:
            row = groups[(groups['$$bins'] == b)]
            if not row.empty:
                stat = row.iloc[0][lookup_table[statistic]]
                if statistic == 'percent':
                    stat = (stat / overall_statitis['count']) * 100
                elif statistic == 'density':
                    stat = (stat / overall_statitis['count'])
                stat = round_float(stat)
            else:
                stat = 0
            
            data.append(stat)
            
    for index, bin in enumerate(result['xAxis']):
        if  isinstance(bin, pd.Interval):
            result['xAxis'][index] = f"{ round_float(bin.left)} , { round_float(bin.right)}"
    
    if data :
        result['series'].append({
            'data':data
        })
    if series:
        for key, data in series.items():
            result['series'].append(data)
    
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
    average = round_float(float(parsed_col.mean()))
    missing_values = int(parsed_col.isna().sum())
    bins = pd.cut(parsed_col, bins=10)
    freq_table = bins.value_counts().sort_index().to_dict()
    categories = {'keys':[], 'values':[]}
    for key, value in freq_table.items():
        if replace_missing_values and ( round_float(key.left) <= average <  round_float(key.right)):
            value += missing_values
        categories['keys'].append(f"{ round_float(key.left)} , { round_float(key.right)}")
        categories['values'].append(int(value))
    var = np.var(parsed_col, ddof=1).__float__()
    return {
        "column": col,
        "type": d_type,
        "categories": categories,
        "missing": "replaced" if replace_missing_values else missing_values,
        "min": round_float(float(parsed_col.min())),
        "max": round_float(float(parsed_col.max())),
        "mean": average,
        "median": round_float(float(parsed_col.median())),
        "var": round_float(var),
        "std": round_float(sqrt(var)),
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
        'mean': 'mean',
        'median': 'median',
        'density': 'count'
    }
    
    result = {
            "xLabel": independent_variable,
            "yLabel": statistic,
            "xAxis": [],
            "series": [],
            "statistics": dict(),
            "categories": [],
            
    }
    
    necessary_columns = [col for col in ['$$independent_variable', category_variable] if col is not None]
    
    df = get_dataframe_from_mongo({"dataset_id":{"$eq":int(dataset)}}) 
    df['$$independent_variable'] = df[independent_variable]
    df = df[necessary_columns]
    if category_variable is not None and df[category_variable].any():
        df['$$category_variable'] = df[category_variable]
        del df[category_variable]
    

    d_type, parsed_col = _parse_col(df, '$$independent_variable')
    del parsed_col
    
    if d_type == 'categorical':
        df['$$bins'] = df['$$independent_variable']
        statistics = ['count']
        result['statistics']['overall_statitis'] = df['$$independent_variable'].agg(statistics).to_dict()
    else :
        df['$$independent_variable'] = pd.to_numeric(df['$$independent_variable'], errors='coerce')
        df['$$bins'] = pd.cut(df['$$independent_variable'], bins=10)
        statistics = [ 'count', 'std','var', 'median', 'min', 'max', 'mean']
    
    df.dropna(inplace=True)
    overall_statitis =  df['$$independent_variable'].agg(statistics).to_dict()
    for key, value in overall_statitis.items():
        overall_statitis[key] = round_float(value)
    result['statistics']['overall_statitis'] = overall_statitis

    if category_variable :
        groups = df.groupby(['$$bins', '$$category_variable'],observed=False)['$$independent_variable'].agg(statistics).reset_index()
    else:
        if statistic == 'density':
                result['density_curve'] = density_curve(df['$$bins'])
        groups = df.groupby('$$bins',observed=False)['$$independent_variable'].agg(statistics).reset_index()
    
    data = []
    series = dict()
    result['categories'] = groups['$$category_variable'].unique() if category_variable else []
        
    result['categories'] = sorted([b for b in result['categories'] if b is not None])
    result['xAxis'] = sorted([b for b in df['$$bins'].unique() if b is not None])
    series = dict()
    for b in result['xAxis']:
        if category_variable:
            for cat in result['categories']:
                row = groups[(groups['$$bins'] == b) & (groups['$$category_variable'] == cat)]
                if not row.empty:
                    stat = round_float(row.iloc[0][lookup_table[statistic]])
                    if statistic == 'percent':
                        stat = (stat / overall_statitis['count']) * 100
                    
                else:
                    stat = 0
                
                if not series.get(cat):
                    series[cat] = {
                        'name': cat,
                        'data': []
                    }
                
                series[cat]['data'].append(stat)
                                    
        else:
            row = groups[(groups['$$bins'] == b)]
            if not row.empty:
                stat = row.iloc[0][lookup_table[statistic]]
                if statistic == 'percent':
                    stat = (stat / overall_statitis['count']) * 100
                stat = round_float(stat)
            else:
                stat = 0
            
            data.append(stat)
            
    for index, bin in enumerate(result['xAxis']):
        if  isinstance(bin, pd.Interval):
            result['xAxis'][index] = f"{ round_float(bin.left)} , { round_float(bin.right)}"
    
    if data :
        result['series'].append({
            'data':data
        })
    if series:
        for key, data in series.items():
            result['series'].append(data)
    
    return result
