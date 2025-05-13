from fastapi import FastAPI, Depends
from analyser import extract
from analyser import line

app = FastAPI()

@app.get('/dataset/extract-metadata/{dataset}')
async def extract_metadata(dataset, replace_missing_values=False):
    return extract(dataset, replace_missing_values)

@app.get('/dataset/line-chart/{dataset}')
async def line_chart(dataset, independent_variable, dependent_variable, category_variable=None):
    return line(dataset, independent_variable, dependent_variable, category_variable)


