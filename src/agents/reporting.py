from anthropic import Anthropic
import os

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

def reporting_agent(state):
    df = state.get("data")
    if df is None:
        state["final_report"] = "No se pudo generar reporte: no se cargó parquet."
        return state

    # Generar resumen simple
    summary = []
    summary.append(f"Columnas: {list(df.columns)}")
    summary.append(f"Cantidad de filas: {len(df)}")
    nulls = df.isnull().sum()
    for col, n in nulls.items():
        if n > 0:
            summary.append(f"Columna '{col}' tiene {n} valores nulos.")
    dups = df.duplicated().sum()
    if dups > 0:
        summary.append(f"Hay {dups} filas duplicadas.")

    # Prompt normal (HUMAN_PROMPT y AI_PROMPT ya NO existen)
    prompt_text = (
        "Genera un reporte profesional en español del parquet procesado, usando la siguiente información:\n\n"
        + "\n".join(summary)
    )

    try:
        resp = client.messages.create(
            model="claude-3-haiku-20240307",  # modelo 100% compatible
            max_tokens=2000,
            messages=[
                {"role": "user", "content": prompt_text}
            ]
        )

        # Extraer el texto correctamente
        content = resp.content[0].text if resp.content else "No se pudo extraer el texto."
        state["final_report"] = content

    except Exception as e:
        state["final_report"] = f"No se pudo generar reporte: {e}"

    return state
