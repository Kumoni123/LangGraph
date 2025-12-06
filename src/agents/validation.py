import numpy as np
import pandas as pd

def validation_agent(state):
    df: pd.DataFrame = state.get("data")  # <-- usamos "data" en lugar de "_df"

    issues = []

    if df is None:
        issues.append("No se cargó ningún parquet.")
    else:
        # Nulls
        nulls = df.isnull().sum()
        for col, n in nulls.items():
            if n > 0:
                issues.append(f"Columna '{col}' tiene {n} valores nulos.")

        # Duplicados
        dups = df.duplicated().sum()
        if dups > 0:
            issues.append(f"Hay {dups} filas duplicadas.")

        # Outliers numéricos
        numeric_cols = df.select_dtypes(include=[np.number])
        for col in numeric_cols:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            out_count = df[(df[col] < lower) | (df[col] > upper)].shape[0]
            if out_count > 0:
                issues.append(f"Columna '{col}' tiene {out_count} outliers.")

    # Guardar resultados en el state
    state["validation_report"] = "\n".join(issues) if issues else "✔ No se encontraron problemas."
    state["has_errors"] = len(issues) > 0

    return state
