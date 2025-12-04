import os
from graph_pipeline import build_graph
from agents.ingestion import ingestion_agent
from agents.validation import validation_agent
from agents.reporting import reporting_agent
from agents.emailer import email_agent

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data")

def find_csv_files():
    return [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.lower().endswith(".csv")
    ]

def main():
    graph = build_graph()
    csv_files = find_csv_files()

    if not csv_files:
        print("No hay CSVs en data/")
        return

    for file_path in csv_files:
        print(f"Procesando: {file_path}")
        state = {"file_path": file_path}

        # Ejecución manual del pipeline
        state = ingestion_agent(state)
        state = validation_agent(state)
        state = reporting_agent(state)
        state = email_agent(state)  # ahora envía correo siempre

        print("Estado final:", state)

if __name__ == "__main__":
    main()
