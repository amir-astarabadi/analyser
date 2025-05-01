from pymongo import MongoClient
import pandas as pd


def extract(dataset):
    df = pd.read_csv(dataset)  # you can also use read_csv() for .csv files
    metadata = []
    for col in df.columns:

        col_data = df[col]
        dtype = pd.api.types.infer_dtype(col_data, skipna=False)
        print(f"{dtype}")

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

        elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_categorical_dtype(col_data):
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

