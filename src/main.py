from fastapi import FastAPI, UploadFile, File, Depends, HTTPException

import utils
import database as _database
import sqlalchemy.orm as _orm
from sqlalchemy import Integer, String, DateTime, text
import pandas as pd
import model
from fastapi.responses import JSONResponse
import logging, sys, traceback
import uvicorn
import datetime as dt
import json

app = FastAPI()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


@app.get("/createdb")
def read_root():
    #Creating DB and Tables with their schemas
    try:
        _database.base.metadata.create_all(bind=_database.engine)
        return JSONResponse(content={"message": "DB created succesfully"}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"message": f"Eror creating db {e}"}, status_code=500)

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

        #Validating if the file has between 1 to 1000 rows
        if data.shape[0]==0 or data.shape[0]>1000:
            raise HTTPException(f"File uploaded exceeds the limit")

        with _database.sessionLocal() as session:
            if(table=='employees'):
                utils.validateData(data,table)
                for i, row in data.iterrows():
                    ##############################################
                    ##Make a deep preprocessing in CSV. No estimate rows with job or department Id in null

                    #Validate If the Employee Id not exists yet
                    if (session.query(model.Employee).filter(model.Employee.id == int(row['id'])).first()):
                        raise HTTPException(f"Id {row['id']} already exists in Employee Table")
                    
                    #Validate If the row includes a JobId already created in Job Table
                    if (session.query(model.Job).filter(model.Job.id == int(row['job_id'])).first()):
                        #Validate If the row includes a DepartmentId already created in Department Table
                        if (session.query(model.Department).filter(model.Department.id == int(row['department_id'])).first()):
                        
                            #Adding a new Employee
                            new_Employee = model.Employee(id=int(row['id']), name=str(row['name']), datetime=row['datetime'], department_id= int(row['department_id']), job_id=int(row['job_id']))
                            session.add(new_Employee)
                            items.append(new_Employee)
                        else:
                            raise HTTPException(f"DepartmentId {row['department_id']} not exist in Department table. Be sure to first create the item")    
                    else:
                        raise HTTPException(f"JobId {row['job_id']} not exist in Job table. Be sure to first create the item")
                
                #If all process executed succesfully, we will make the commit in DB
                print(items)
                session.commit()
                    
                return JSONResponse(content={"message": "Data inserted successfully in Employees table"}, status_code=200)
            
            elif(table=='jobs'):
                utils.validateData(data,table)
                for i, row in data.iterrows():

                    #Validate If the Job Id not exists yet
                    if (session.query(model.Job).filter(model.Job.id == int(row['id'])).first()):
                        raise HTTPException(f"Id {row['id']} already exists in Job Table")
                    
                    #Adding a new Job
                    new_Job = model.Job(id=int(row['id']), job=str(row['job']))
                    session.add(new_Job)
                    items.append(new_Job)

                #If all process executed succesfully, we will make the commit in DB
                print(items)
                session.commit()

                return JSONResponse(content={"message": "Data inserted successfully in Job table"}, status_code=200)
            
            elif(table=='departments'):
                utils.validateData(data,table)
                for i, row in data.iterrows():

                    #Validate If the Department Id not exists yet
                    if (session.query(model.Department).filter(model.Department.id == int(row['id'])).first()):
                        raise HTTPException(f"Id {row['id']} already exists in Job Table")
                    
                    #Adding a new Department
                    new_Job = model.Department(id=int(row['id']), department=str(row['department']))
                    session.add(new_Job)
                    items.append(new_Job)

                #If all process executed succesfully, we will make the commit in DB
                print(items)
                session.commit()

                return JSONResponse(content={"message": "Data inserted successfully in Department table"}, status_code=200)
            else:
                return JSONResponse(content={"message": f"Not exist table {table}"}, status_code=404)
            
                
    except Exception as e:
        logger.info(e)
        traceback.print_exception(type(e), e, e.__traceback__)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/get_hired_by_quarter")
async def insert(year: int=None, db: _orm.Session = Depends(utils.get_db)):
    #This query makes a summary of all hired people by quarter
    #This API receives the year that we want to make the summarize
    try:
        query = text(f"""WITH DB1 AS (
                                    SELECT *,
                                        CASE
                                        WHEN strftime('%m', E.datetime) BETWEEN '01' AND '03' THEN 'Q1'
                                        WHEN strftime('%m', E.datetime) BETWEEN '04' AND '06' THEN 'Q2'
                                        WHEN strftime('%m', E.datetime) BETWEEN '07' AND '09' THEN 'Q3'
                                        ELSE 'Q4'
                                    END AS quarter FROM Employees E 
                                    INNER JOIN Jobs J ON E.job_id=J.id
                                    INNER JOIN Departments D ON E.department_id = D.id
                                    WHERE strftime('%Y', E.datetime)='{year}'
                                ),
                                DB2 AS (
                                    SELECT department_id, department, job_id, job, quarter, COUNT(*) as counts FROM DB1 GROUP BY department_id, department, job_id, job, quarter
                                )
                                SELECT department, job,
                                    MAX(CASE WHEN quarter='Q1' THEN counts ELSE 0 END) AS Q1,
                                    MAX(CASE WHEN quarter='Q2' THEN counts ELSE 0 END) AS Q2,
                                    MAX(CASE WHEN quarter='Q3' THEN counts ELSE 0 END) AS Q3,
                                    MAX(CASE WHEN quarter='Q4' THEN counts ELSE 0 END) AS Q4
                                    FROM DB2 GROUP BY department_id, department, job_id, job
                                    ORDER BY department, job ASC""")
        #Fetching all rows 
        result = db.execute(query).fetchall()
        
        #Creating a dict for each row 
        results = [{i:tuple(result[i])} for i in range(len(result))]

        return JSONResponse(content={"response": results}, status_code=200)
    except Exception as e:
        logger.info(e)
        traceback.print_exception(type(e), e, e.__traceback__)
        return JSONResponse(content={"error": str(e)}, status_code=500)
    

if __name__ == '__main__':
    uvicorn.run(app,host='0.0.0.0',port=8000)