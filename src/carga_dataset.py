import uuid
import json
import pandas as pd
import boto3
import io
from langsmith import Client

# Inicializa el cliente
client = Client()

# Leer parquet desde S3
s3 = boto3.client("s3")
bucket = "belc-bigdata-models-dlk-qas"
key = "pcd_model/datatrain/fr/train/codpais=PA/aniocampana=202508/data_01c0d68e-0619-07af-0050-ee83c6222afe_006_6_0.snappy.parquet"

obj = s3.get_object(Bucket=bucket, Key=key)
buffer = io.BytesIO(obj["Body"].read())
df = pd.read_parquet(buffer, engine="fastparquet")

# Crear dataset (sin ejemplos aún)
dataset = client.create_dataset(
    dataset_name="mi_dataset_langgraph_parquet",
    description="Dataset cargado directamente desde parquet S3"
)

# Subir ejemplos en batches para evitar problemas con parquet grande
batch_size = 1000  # ajustable según tamaño del parquet
for start in range(0, len(df), batch_size):
    end = start + batch_size
    batch = df.iloc[start:end]

    inputs = [{"data": row.to_dict()} for _, row in batch.iterrows()]
    outputs = [{} for _ in range(len(batch))]  # vacío, o puedes mapear alguna columna específica
    metadata = [{"source": "parquet_s3"} for _ in range(len(batch))]

    client.create_examples(
        dataset_id=dataset.id,
        inputs=inputs,
        outputs=outputs,
        metadata=metadata
    )
    print(f"Subidos ejemplos {start} a {end} de {len(df)}")

print("Dataset completo subido. ID:", dataset.id)
