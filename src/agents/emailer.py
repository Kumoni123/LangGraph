import smtplib
from email.mime.text import MIMEText
import os

def email_agent(state):
    sender = os.getenv("EMAIL_USER")
    password = os.getenv("EMAIL_PASSWORD")
    receiver = os.getenv("EMAIL_RECEIVER")

    # Crear mensaje de estado
    if state.get("has_errors", False):
        status_text = "Se detectaron errores en el parquet:\n" + "\n".join(state.get("validation_errors", []))
    else:
        status_text = "El parquet se procesó correctamente."

    report_text = state.get("final_report", "")
    full_message = f"{status_text}\n\nReporte de Claude:\n{report_text}"

    msg = MIMEText(full_message, _charset="utf-8")
    msg["Subject"] = f"Estado del dataset: {state.get('file_path')}"
    msg["From"] = sender
    msg["To"] = receiver

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(sender, password)
        server.sendmail(sender, receiver.split(","), msg.as_string())
        server.quit()
        return {"email_status": "Correo enviado correctamente."}
    except Exception as e:
        return {"email_status": f"Error enviando correo: {str(e)}"}
