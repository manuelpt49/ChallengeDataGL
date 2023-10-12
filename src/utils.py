import database as _database
from fastapi import HTTPException
import csv
import magic

def get_db():
    db = _database.sessionLocal()
    try:
        yield db
    finally:
        db.close()

def is_csv(file):
    mime = magic.Magic()
    file_type = mime.from_file(file)
    return 'CSV' in file_type.upper()
    """try:
        dialect = csv.Sniffer().sniff(file.read(1024))
        return dialect.delimiter == ','  # Assuming CSV file uses a comma as delimiter
    except csv.Error:
        return False"""

def validateData(df, table):
    if table=='employees' and df.shape[1]==5:
        df.columns = ["id", "name", "datetime", "department_id", "job_id"]
    elif table=='jobs' and df.shape[1]==2:
        df.columns = ["id", "job"]
    elif table=='departments' and df.shape[1]==2:
        df.columns = ["id", "department"]
    else:
        raise HTTPException("File doesn't have the number of columns correct")