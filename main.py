from fastapi import FastAPI, Depends
from analyser import extract

app = FastAPI()

@app.get('/dataset/extract-metadata/{dataset}')
async def extract_metadata(dataset, replace_missing_values=False):
    return extract(dataset, replace_missing_values)



