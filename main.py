from fastapi import FastAPI
from analyser import extract, line, histogram, bar

app = FastAPI()

@app.get('/dataset/extract-metadata/{dataset}')
async def extract_metadata(dataset, replace_missing_values=False):
    return extract(dataset, replace_missing_values)

@app.get('/dataset/line-chart/{dataset}')
async def line_chart(dataset, independent_variable, dependent_variable, category_variable=None):
    return line(dataset, independent_variable, dependent_variable, category_variable)

@app.get('/dataset/scatter-chart/{dataset}')
async def scatter_chart(dataset, independent_variable, dependent_variable, category_variable=None):
    return line(dataset, independent_variable, dependent_variable, category_variable)

@app.get('/dataset/histogram-chart/{dataset}')
async def histogram_chart(dataset, independent_variable,  category_variable=None, statistic='count'):
    return histogram(dataset, independent_variable, category_variable, statistic)

@app.get('/dataset/bar-chart/{dataset}')
async def bar_chart(dataset, independent_variable,  category_variable=None, statistic='frequency'): 
    return bar(dataset, independent_variable, category_variable, statistic)


