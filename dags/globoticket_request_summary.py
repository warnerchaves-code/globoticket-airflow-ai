"""Summarize a Globoticket promoter request with an LLM task."""
from pathlib import Path

import pendulum

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.sdk import dag, task

REQUEST_FILE = Path("/opt/airflow/assets/promoter_request.txt")
LLM_CONN_ID = "globoticket_llm"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["globoticket"],
)
def globoticket_request_summary():
    @task
    def read_request() -> str:
        return REQUEST_FILE.read_text()

    summarize = LLMOperator(
        task_id="summarize",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You summarize event requests for the Globoticket intake team. "
            "Use only facts stated in the request. "
            "If a detail is not stated, say that it is not stated."
        ),
        prompt=(
            "Summarize this promoter request in two sentences.\n\n"
            "Promoter request:\n"
            "{{ ti.xcom_pull(task_ids='read_request') }}"
        ),
    )

    read_request() >> summarize


globoticket_request_summary()
