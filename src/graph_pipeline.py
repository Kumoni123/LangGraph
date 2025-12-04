from langgraph.graph import Graph
from agents.ingestion import ingestion_agent
from agents.validation import validation_agent
from agents.reporting import reporting_agent
from agents.emailer import email_agent

def build_graph():
    g = Graph()

    # Nodos
    g.add_node("ingestion", ingestion_agent)
    g.add_node("validation", validation_agent)
    g.add_node("reporting", reporting_agent)
    g.add_node("email", email_agent)

    # Entry point
    g.set_entry_point("ingestion")

    # Conexiones secuenciales
    g.add_edge("ingestion", "validation")
    g.add_edge("validation", "reporting")
    g.add_edge("reporting", "email")  # email siempre se ejecuta, pero en el agente controlamos si enviar
    
    # Email termina ejecución
    #g.add_edge("email", None)  # None = fin del grafo

    return g
