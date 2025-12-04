import os
import pandas as pd

def ingestion_agent(state):
    # Subimos dos niveles para llegar a la carpeta raíz del proyecto y luego data/
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    data_folder = os.path.join(base_dir, "data")

    if not os.path.exists(data_folder):
        state["data"] = None
        state["file_path"] = None
        state["has_errors"] = True
        state["validation_report"] = f"Carpeta data no encontrada en {data_folder}"
        return state

    csv_files = [f for f in os.listdir(data_folder) if f.endswith(".csv")]

    if not csv_files:
        state["data"] = None
        state["file_path"] = None
        state["has_errors"] = True
        state["validation_report"] = "No se encontraron archivos CSV en la carpeta data."
        return state

    # Tomamos el primer CSV encontrado
    csv_path = os.path.join(data_folder, csv_files[0])
    try:
        df = pd.read_csv(csv_path)
        state["data"] = df
        state["file_path"] = csv_files[0]
        state["has_errors"] = False
        return state
    except Exception as e:
        state["data"] = None
        state["file_path"] = csv_files[0]
        state["has_errors"] = True
        state["validation_report"] = f"Error leyendo CSV: {e}"
        return state
