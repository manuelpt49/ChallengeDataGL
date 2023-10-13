from fastapi import FastAPI, UploadFile, File, Depends, HTTPException

import utils
import database as _database
import sqlalchemy.orm as _orm
from sqlalchemy import Integer, String, DateTime
import pandas as pd
import model
from fastapi.responses import JSONResponse
import logging, sys, traceback
import uvicorn
import datetime as dt

app = FastAPI()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


@app.get("/")
def read_root():
    #Creating DB and Tables with their schemas
    _database.base.metadata.create_all(bind=_database.engine)
    return {"Hello": "World"}

@app.post("/upload")
async def upload(file: UploadFile = File(...), table: str=None, db: _orm.Session = Depends(utils.get_db)):
    try:

    
        if not file.filename.endswith('.csv'):
            raise HTTPException("File is not a CSV")

        data = pd.read_csv(file.file, header=None) #Reading without headers

        if(table=='employees'):
            utils.validateData(data,table)
            dtype_mapping = {
                'id': Integer,
                'name': String,
                'datetime': DateTime(timezone=True),
                'department_id': Integer,
                'job_id': Integer
            }
            data.to_sql(name='employees', con=_database.engine, if_exists='replace', index=False, dtype=dtype_mapping)
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


@app.post("/insert")
async def insert(file: UploadFile = File(...), table: str=None, db: _orm.Session = Depends(utils.get_db)):
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException("File is not a CSV")

        data = pd.read_csv(file.file, header=None) #Reading without headers

        items = []

        if(table=='employees'):
            utils.validateData(data,table)
            
            #Validating if the file has between 1 to 1000 rows
            if data.shape[0]==0 or data.shape[0]>1000:
                raise HTTPException(f"File uploaded exceeds the limit")

            with _database.sessionLocal() as session:
                for i, row in data.iterrows():
                    ##############################################
                    ##Add logic to add batch for Jobs and Departments
                    ##Make a deep preprocessing in CSV. No estimate rows with job or department Id in null

                    #Validate If the Employee Id not exists yet
                    if (session.query(model.Employee).filter(model.Employee.id == int(row['id'])).first()):
                        raise HTTPException(f"Id {row['id']} already exist")
                    
                    #Validate If the row includes a JobId already created in Job Table
                    if (session.query(model.Job).filter(model.Job.id == int(row['job_id'])).first()):
                        if (session.query(model.Department).filter(model.Department.id == int(row['department_id'])).first()):
                        
                            #Adding a new Employee
                            new_Employee = model.Employee(id=int(row['id']), name=str(row['name']), datetime=row['datetime'], department_id= int(row['department_id']), job_id=int(row['job_id']))
                            print(f"{new_Employee.id}, {new_Employee.department_id}, {new_Employee.job_id}")
                            session.add(new_Employee)
                            items.append(new_Employee)
                        else:
                            raise HTTPException(f"DepartmentId {row['department_id']} not exist in Department table. Be sure to first create the item")    
                    else:
                        raise HTTPException(f"JobId {row['job_id']} not exist in Job table. Be sure to first create the item")
                
                #If all process executed succesfully, we will make the commit in DB
                session.commit()
                
            return JSONResponse(content={"message": "Data inserted successfully"}, status_code=200)
        elif(table=='jobs'):
            utils.validateData(data,table)
            data.to_sql(name='jobs', con=_database.engine, if_exists='append', index=False)
            return JSONResponse(content={"message": "Data inserted successfully"}, status_code=200)
        elif(table=='departments'):
            utils.validateData(data,table)
            data.to_sql(name='departments', con=_database.engine, if_exists='append', index=False)
            return JSONResponse(content={"message": "Data inserted successfully"}, status_code=200)
        else:
            return JSONResponse(content={"message": f"Not exist table {table}"}, status_code=404)
        
    except Exception as e:
        logger.info(e)
        traceback.print_exception(type(e), e, e.__traceback__)
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == '__main__':
    uvicorn.run(app,host='0.0.0.0',port=8000)