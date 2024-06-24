from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from sqlalchemy import create_engine
import mysql.connector
import pymysql
import pandas as pd

app = FastAPI()

db_connection_path = 'mysql+pymysql://croi_statistics:q1w2e3r4t5@192.168.0.126:3306/croi_statistics'
db_connection = create_engine(db_connection_path)

@app.get("/")
def hello():
    return {"hello": "인삿말"}

@app.get("/hello")
def hello():
    return {"message": "안녕하세요 파이보"}

@app.get("/test1")
def test():
    return {"message": "test1test1"}

@app.get("/test2")
def test():
    return {"message": "test2test21"}

@app.get("/test3")
def test():
    query = "select time, user_age from age"
    df = pd.read_sql(query, db_connection)
    return df.to_json(orient='records')