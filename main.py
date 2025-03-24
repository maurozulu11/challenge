import os
import sqlite3
import pandas as pd
import avro.schema
import avro.io
import avro.datafile
import io
import json
import uvicorn
from fastapi import FastAPI, HTTPException

app = FastAPI()

# Route of files
CSV_DIRECTORY = os.path.join(os.getcwd(), "files")  

#BD conection function
def get_db_connection():
    conn = sqlite3.connect("database.db")
    conn.row_factory = sqlite3.Row
    return conn

# starting database
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY,
            department TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            job TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hired_employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            datetime TEXT NOT NULL,
            department_id INTEGER,
            job_id INTEGER,
            FOREIGN KEY (department_id) REFERENCES departments(id),
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
    """)
    conn.commit()
    conn.close()

init_db()

@app.post("/upload_csv/")
def upload_csv(file_type: str):
    file_map = {
        "departments": "departments.csv",
        "jobs": "jobs.csv",
        "hired_employees": "hired_employees.csv"
    }

    if file_type not in file_map:
        raise HTTPException(status_code=400, detail="Invalid file type")

    # full path of files
    file_path = os.path.join(CSV_DIRECTORY, file_map[file_type])

    # searching for files
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    try:
        # Read files CSV and set the column name
        df = pd.read_csv(file_path, header=None)
        if file_type == "departments":
            df.columns = ["id", "department"]
        elif file_type == "jobs":
            df.columns = ["id", "job"]
        elif file_type == "hired_employees":
            df.columns = ["id", "name", "datetime", "department_id", "job_id"]

            # reeplace null values
            df["name"].fillna("Unknown", inplace=True)

            df["datetime"].fillna("2000-01-01 00:00:00", inplace=True)

        # Get into database
        conn = get_db_connection()
        df.to_sql(file_type, conn, if_exists='append', index=False)
        conn.close()

        return {"message": f"{file_type} uploaded successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#insert information from hired_employees
@app.post("/insert_hired_employees/")
def insert_hired_employees(data: list):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany("INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", data)
        conn.commit()
        return {"message": "Employees inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

#starting backup function
@app.post("/backup/{table_name}")
def backup_table(table_name: str):
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found in table {table_name}")

        # change field in strings 
        df = df.astype(str)

        schema_json = {
            "type": "record",
            "name": "BackupRecord",
            "fields": [{"name": col, "type": "string"} for col in df.columns]
        }
        schema = avro.schema.parse(json.dumps(schema_json))

        backup_dir = os.path.join(os.getcwd(), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"{table_name}_backup.avro")

        with open(backup_path, "wb") as f:
            writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
            for _, row in df.iterrows():
                writer.append(row.to_dict())  
            writer.close()  

        return {"message": f"Backup of {table_name} saved successfully", "path": backup_path}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#starting fuction restore from vro files to database
@app.post("/restore/{table_name}")
def restore_table(table_name: str):
    try:
        backup_path = os.path.join(os.getcwd(), "backups", f"{table_name}_backup.avro")
        if not os.path.exists(backup_path):
            raise HTTPException(status_code=404, detail=f"Backup file not found for table {table_name}")

        restored_data = []
        with open(backup_path, "rb") as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            for record in reader:
                restored_data.append(record)
            reader.close()

        if not restored_data:
            raise HTTPException(status_code=404, detail=f"No data found in backup for {table_name}")

        columns = ", ".join(restored_data[0].keys())  
        placeholders = ", ".join(["?" for _ in restored_data[0]])  

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.executemany(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", 
                           [tuple(row.values()) for row in restored_data])
        conn.commit()
        conn.close()

        return {"message": f"Table {table_name} restored successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9000, reload=True)
