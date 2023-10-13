import database as _database
from fastapi import HTTPException


def get_db():
    db = _database.sessionLocal()
    try:
        yield db
    finally:
        db.close()


def validateData(df, table):
    if table=='employees' and df.shape[1]==5:
        df.columns = ["id", "name", "datetime", "department_id", "job_id"]
    elif table=='jobs' and df.shape[1]==2:
        df.columns = ["id", "job"]
    elif table=='departments' and df.shape[1]==2:
        df.columns = ["id", "department"]
    else:
        raise HTTPException("File doesn't have the number of columns correct")