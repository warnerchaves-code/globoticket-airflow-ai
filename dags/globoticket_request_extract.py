"""Extract a typed EventRequest from a Globoticket promoter request."""
from pathlib import Path

import pendulum

from airflow.sdk import dag, task

from globoticket_models import EventRequest

REQUEST_FILE = Path("/opt/airflow/assets/promoter_request.txt")
LLM_CONN_ID = "globoticket_llm"

# What the intake team needs before a request can go any further.
REQUIRED_FIELDS = ["event_name", "venue", "event_datetime",
                   "expected_attendance", "proposed_ticket_price"]


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["globoticket"],
)
def globoticket_request_extract():
    @task
    def read_request() -> str:
        return REQUEST_FILE.read_text()

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        output_type=EventRequest,
        system_prompt=(
            "You extract event details from promoter requests for the Globoticket intake team. "
            "Use only facts stated in the request. "
            "If a value is not stated, return null for that field."
        ),
    )
    def extract(request: str) -> str:
        return f"Extract the event details from this promoter request.\n\nPromoter request:\n{request}"

    @task
    def check_fields(event: EventRequest) -> dict:
        print("type:", type(event).__name__)
        missing = [name for name in REQUIRED_FIELDS if getattr(event, name) is None]
        present = [name for name in REQUIRED_FIELDS if name not in missing]
        print("present:", ", ".join(present))
        print("missing:", ", ".join(missing) or "none")
        print("complete:", not missing)
        return {"complete": not missing, "missing": missing}

    check_fields(extract(read_request()))


globoticket_request_extract()
