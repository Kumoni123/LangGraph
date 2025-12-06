# src/agents/ingestion.py
import os
import boto3
import pandas as pd
import io
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

BUCKET_NAME = os.getenv("S3_BUCKET")
FOLDER_PATH = os.getenv("S3_KEY")  # carpeta en S3

def ingestion_agent(state):
    # Conectar a S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # Listar todos los objetos en la carpeta
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER_PATH)
    if "Contents" not in response:
        state["data"] = None
        return state

    parquet_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(".parquet")]
    if not parquet_keys:
        state["data"] = None
        return state

    # Leer todos los parquet y concatenarlos en un DataFrame
    dfs = []
    for key in parquet_keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        dfs.append(df)

    state["data"] = pd.concat(dfs, ignore_index=True)
    return state
