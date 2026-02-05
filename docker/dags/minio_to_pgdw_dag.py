import os
import boto3
import psycopg2
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import json

# Load environment variables
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Postgres Config --------
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT", 5432)
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]

# -------- Python Helpers --------
def normalize_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, (int, float, str)):
        return v
    if hasattr(v, "item"):          # numpy scalar → python scalar
        return v.item()
    return v

def sanitize_for_json(v):
    """Convert any problematic value to JSON-compatible Python type."""
    if pd.isna(v):      # handles np.nan, pd.NA, None
        return None
    if isinstance(v, (np.integer, np.int64, np.int32)):
        return int(v)
    if isinstance(v, (np.floating, np.float64, np.float32)):
        return float(v)
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    return v
# -------- Python Callables --------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    local_files = {}

    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])

        local_files[table] = []

        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)

    return local_files


def load_to_postgres(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")

    if not local_files:
        print("No files found in MinIO.")
        return

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    cur = conn.cursor()

    # 👇 force all queries into the raw schema
    cur.execute(f"SET search_path TO {PG_SCHEMA};")

    for table in TABLES:
        files = local_files.get(table, [])
        
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        for file_path in files:
            print(f"Loading {file_path} into {table}")

            df = pd.read_parquet(file_path)
            if table == "transactions":
                # Force nullable integer so NaN can become None
                df["related_account_id"] = (
                    df["related_account_id"]
                    .astype("Int64")   # pandas nullable integer
                )

            if df.empty:
                print(f"{file_path} is empty, skipping.")
                continue

            cols = list(df.columns)
            #values = [tuple(x) for x in df.to_numpy()]
            values = [
                tuple(normalize_value(v) for v in row)
                for row in df.itertuples(index=False, name=None)
            ]

            insert_sql = f"""
                INSERT INTO {table} ({', '.join(cols)})
                VALUES %s
            """
            try:
                """ DEBUG
                if table == "transactions":
                    for i, row in enumerate(values):
                        try:
                            execute_values(
                                cur,
                                insert_sql,
                                [row],   # 👈 single row
                                page_size=1
                            )
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            print("❌ FAILED ROW INDEX:", i)
                            print("❌ FAILED ROW DATA:")
                            for col, val in zip(cols, row):
                                print(f"   {col}: {val} ({type(val)})")
                            raise
                else:
                    execute_values(cur, insert_sql, values, page_size=1000)
                    conn.commit()
                    print(f"Inserted {len(df)} rows into {table}")
                """
                execute_values(cur, insert_sql, values, page_size=1000)
                conn.commit()
                print(f"Inserted {len(df)} rows into {table}")
            except Exception as e:
                conn.rollback()
                raise RuntimeError(f"Failed loading table {table}: {e}")

    cur.close()
    conn.close()


def load_raw_to_postgres(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")

    if not local_files:
        print("No files found in MinIO.")
        return

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    cur = conn.cursor()

    # 👇 force all queries into the raw schema
    cur.execute(f"SET search_path TO {PG_SCHEMA};")

    for table, files in local_files.items():
        raw_table = f"raw_{table}"  # use one-column raw table
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        for f in files:
            df = pd.read_parquet(f)
            if df.empty:
                print(f"{f} is empty, skipping.")
                continue
            
            # Convert each row to JSON-compatible dictionary
            json_rows = [
                {col: sanitize_for_json(val) for col, val in row.items()}
                for row in df.to_dict(orient="records")
            ]
            # Convert dictionaries to JSON strings
            values = [(json.dumps(row),) for row in json_rows]

            insert_sql = f"INSERT INTO {raw_table} (data) VALUES %s"
            try:
                execute_values(cur, insert_sql, values, page_size=1000)
                conn.commit()
                print(f"Inserted {len(values)} rows into {raw_table}")
            except Exception as e:
                conn.rollback()
                raise RuntimeError(f"Failed inserting into {raw_table}: {e}")

    cur.close()
    conn.close()

# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="minio_to_postgres_banking",
    default_args=default_args,
    description="Load MinIO parquet into Postgres RAW tables",
    schedule='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_postgres",
        python_callable=load_raw_to_postgres,
    )

    task1 >> task2
