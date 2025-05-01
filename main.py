from fastapi import FastAPI, Depends
from analyser import extract

app = FastAPI()

@app.get('/dataset/extract-metadata/{dataset}')
async def extract_metadata(dataset):
    return extract(dataset)



