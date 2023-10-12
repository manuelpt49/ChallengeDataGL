from fastapi import FastAPI, UploadFile, File, Depends, Request

import database as _database
import sqlalchemy.orm as _orm
import pandas as pd
import model
from fastapi.responses import JSONResponse

app = FastAPI()

def get_db():
    db = _database.sessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    _database.base.metadata.create_all(bind=_database.engine)
    return {"Hello": "World"}

@app.post("/upload")
async def upload(file: UploadFile = File(...), table: str=None, db: _orm.Session = Depends(get_db)):
    try:
        data = pd.read_csv(file.file)
        if(table=='employees'):
            data.to_sql(name='employees', con=_database.engine, if_exists='replace', index=False)
            return JSONResponse(content={"message": "Data uploaded successfully"}, status_code=200)
        elif(table=='jobs'):
            data.to_sql(name='jobs', con=_database.engine, if_exists='replace', index=False)
            return JSONResponse(content={"message": "Data uploaded successfully"}, status_code=200)
        elif(table=='departments'):
            data.to_sql(name='departments', con=_database.engine, if_exists='replace', index=False)
            return JSONResponse(content={"message": "Data uploaded successfully"}, status_code=200)
        else:
            return JSONResponse(content={"message": f"Not exist table {table}"}, status_code=404)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)