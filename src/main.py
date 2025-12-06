import os
from graph_pipeline import build_graph
from agents.ingestion import ingestion_agent
from agents.validation import validation_agent
from agents.reporting import reporting_agent
from agents.emailer import email_agent
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()  # Esto carga LANGSMITH_API_KEY y otras variables
print("LangSmith API Key cargada:", os.getenv("LANGSMITH_API_KEY") is not None)
bucket = os.getenv("S3_BUCKET")
key = os.getenv("S3_KEY")


def main():
    graph = build_graph()

    # Configura aquí tu bucket y key del parquet
    state = {
        "s3_bucket": bucket,
        "s3_key": key
    }

    # Ejecución del pipeline
    state = ingestion_agent(state)  # Lee parquet desde S3
    state = validation_agent(state)  # Valida dataframesu
    state = reporting_agent(state)   # Genera reporte
    state = email_agent(state)       # Envía correo

    print("Estado final:", state)
    if state.get("errors"):
        print("Errores detectados:", state["errors"])

if __name__ == "__main__":
    main()
