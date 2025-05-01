from fastapi import FastAPI, Depends
from db import db
from analyser import extract
from sqlalchemy.orm import Session
from sqlalchemy import text
from mysqldb import SessionLocal

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
a = False
app = FastAPI()

@app.get('/dataset/{dataset}/extract-metadata')
async def extract_metadata(dataset):
    return extract('./datasets/sample_data.csv')

@app.get("/test-db")
def test_db(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("SHOW TABLES;"))
    except Exception as e:
        # print(f"Error executing query: {e}")
        return {"error": str(e)}
    tables = [row[0] for row in result]
    return {"tables": tables}

