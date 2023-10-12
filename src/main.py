from fastapi import FastAPI, UploadFile, File, Depends, HTTPException

import utils
import database as _database
import sqlalchemy.orm as _orm
import pandas as pd
import model
from fastapi.responses import JSONResponse

app = FastAPI()



@app.get("/")
def read_root():
    #Creating DB and Tables with their schemas
    _database.base.metadata.create_all(bind=_database.engine)
    return {"Hello": "World"}

@app.post("/upload")
async def upload(file: UploadFile = File(...), table: str=None, db: _orm.Session = Depends(utils.get_db)):
    try:

        #Reading CSV file

        try:   
            data = pd.read_csv(file.file, header=None) #Reading without headers
        except Exception as e:
            raise HTTPException("File is not a CSV")

        if(table=='employees'):
            utils.validateData(data,table)
            data.to_sql(name='employees', con=_database.engine, if_exists='replace', index=False)
            return JSONResponse(content={"message": "Data uploaded successfully"}, status_code=200)
        elif(table=='jobs'):
            utils.validateData(data,table)
            data.to_sql(name='jobs', con=_database.engine, if_exists='replace', index=False)
            return JSONResponse(content={"message": "Data uploaded successfully"}, status_code=200)
        elif(table=='departments'):
            utils.validateData(data,table)
            data.to_sql(name='departments', con=_database.engine, if_exists='replace', index=False)
            return JSONResponse(content={"message": "Data uploaded successfully"}, status_code=200)
        else:
            return JSONResponse(content={"message": f"Not exist table {table}"}, status_code=404)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


        