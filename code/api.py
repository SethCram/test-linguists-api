from fastapi import FastAPI, HTTPException, File, UploadFile
import os
import subprocess
import shutil
from uvicorn import run
from fastapi.middleware.cors import CORSMiddleware
from sqlite3 import Connection, connect, OperationalError

#region Helper Functs
        
def store_file(file, destination: str, byte_mode: bool = False):
    """Stores file in destination through writing in byte mode. 

    Args:
        file (_type_): _description_
        destination (str): file path to copy file to
        byte_mode (bool): Whether to copy the file in using bytes or not (for binary)
    Raises:
        HTTPException: Raises error 500 if problem occures.
    """
    
    #if file exists, return it
    if( os.path.exists(destination)):
        raise HTTPException(status_code=400, detail=f"File already exists at {destination}. Rename your uploaded file or delete the pre-exsiting one(s).")
    
    operational_mode = "w"
    
    if(byte_mode):
        operational_mode += "b"
    
    try:
        with open(destination, operational_mode) as file_obj:
            shutil.copyfileobj(file.file, file_obj)
    except Exception:
        rm_file(destination)
        raise HTTPException(status_code=500, detail="There was an error copying the given file into server storage.")
    finally:
        file.file.close()   
        
def rm_file(file_path: str):
    """Removes file from file system.

    Args:
        file_path (str): _description_

    Returns:
        tuple: status_code, message
    """
    try:
        os.remove(file_path)
        status_code = 200
        message = "ok"
    except OSError as error:
        status_code = 404
        message = error
    except:
        status_code = 500
        message = f"Couldn't remove {file_path}"
        
    return status_code, message
    
def rm_dir(folder_path: str):
    """Removes directory and contents from file system.

    Args:
        folder_path (str): _description_

    Returns:
        tuple: status_code, message
    """
    try:
        shutil.rmtree(folder_path)
        status_code = 200
        message = "ok"
    except OSError as error:
        status_code = 404
        message = error
    except:
        status_code = 500
        message = f"Couldn't remove {folder_path} and its contents"
    
    return status_code, message

def create_dir(dir_path: str):
    """Creates directory in file system.

    Args:
        dir_path (str): _description_

    Returns:
        tuple: status_code, message
    """
    #create path to new db dir
    try:
        os.mkdir(dir_path)
        status_code = 200
        message = "ok"
    #if it fails, remove the previously uploaded sql file
    except FileExistsError:
        status_code = 400
        message = "Directory creation failed. A folder using that same name has probably already been uploaded. Rename your uploaded file."
    except Exception:
        status_code = 500
        message = "Directory creation failed."
    
    return status_code, message

def create_sql_path(file_id):
    return os.path.join("sql", file_id + ".sql")

#endregion Helper Functs

# Initialize REST API
app = FastAPI()

#enable communication to api
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/upload/")
def upload(file: UploadFile = File(...)):
    """_Upload a database file into the file system._

    Args:
        file (UploadFile, optional): _A file in Sqlite3 format_. Defaults to File(...).

    Raises:
        HTTPException: _200 for okay or other for directory/file creation failure_

    Returns:
        _json_: _message_
    """
    
    #separate file name into name + ext
    db_id, file_ext = os.path.splitext(file.filename)
    
    #path to new db dir
    db_folder_path = os.path.join("database", db_id)
    
    status_code, message = create_dir(db_folder_path)
    
    if(status_code != 200):
        raise HTTPException(status_code=status_code, detail=message)
    
    #path to new db file
    new_file_path = os.path.join(db_folder_path, db_id + ".sqlite")
    
    store_file(file, new_file_path, byte_mode=True) 
    
    return {"message": f"Successfully uploaded {file.filename} to {new_file_path}"}

@app.post("/api/upload/sql")
def uploadSql(file: UploadFile = File(...)):
    """_Uploads an sql file into the file system.
    Generates an sqlite3 formatted file from the sql file.
    Stores the sqlite3 formatted file in the file system.
    Undoes operations if any step fails._ 

    Args:
        file (UploadFile, optional): _A file containing SQL_. Defaults to File(...).

    Raises:
        HTTPException: _Create database directory error_
        HTTPException: _Conversion from SQL to database file error_

    Returns:
        _json_: _message_
    """
    
    #separate file name into name + ext
    file_id, file_ext = os.path.splitext(file.filename)
    
    #path to new sql file
    sql_file_path = create_sql_path(file_id)
    
    #store sql file in proper spot
    store_file(file, sql_file_path, byte_mode=True)
    
    #path to new db dir
    db_folder_path = os.path.join("database", file_id)
    
    create_dir_code, create_dir_msg = create_dir(db_folder_path)

    #if dir creation failed
    if(create_dir_code != 200):
        #rm file
        rm_file_code, rm_file_msg = rm_file(sql_file_path)
        
        #print locally any file removal error
        if(rm_file_code != 200):
            print(rm_file_msg)
        
        #raise error
        raise HTTPException(status_code=create_dir_code, detail=create_dir_msg)
    
    db_filename = file_id + ".sqlite"
    
    #path to new db file
    db_file_path = os.path.join(db_folder_path, db_filename)
    
    #if couldn't create database file from sql
    if(subprocess.call(["sqlite3", f"{db_file_path}", f".read {sql_file_path}"]) != 0):
        #removed saved SQL file
        rm_file_code, rm_file_msg = rm_file(sql_file_path)
        
        #print locally any file removal error
        if(rm_file_code != 200):
            print(rm_file_msg)
        
        #remove created db folder + any contents that snuck in
        rm_dir_code, rm_dir_msg = rm_dir(db_folder_path)
        
        #print locally any file removal error
        if(rm_dir_code != 200):
            print(rm_dir_msg)
        
        raise HTTPException(status_code=500, detail="Couldn't create database file from uploaded file. Ensure an SQL file is being uploaded.")
    
        
    return {"message": f"Successfully uploaded {file.filename} to {sql_file_path} and {db_filename} to {db_file_path}"}

# Run app
run(app=app, host="0.0.0.0", port=8000, forwarded_allow_ips='*')