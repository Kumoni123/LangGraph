from typing import TypedDict

class PipelineState(TypedDict):
    files_to_process: list
    current_file: str
    #dataframe: Any
    dataset_info: dict
    anomalies: dict
    report: str
    email_sent: bool
    anthropic_api_key: str

