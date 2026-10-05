"""Extract a typed EventRequest from a Globoticket event brief file in Blob Storage."""
import pendulum

from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
from airflow.sdk import Param, dag

from globoticket_models import EventRequest

EVENT_BRIEFS = "abfs://globoticket-intake/event-briefs/"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["globoticket"],
    params={
        "event_brief": Param(
            "event_brief_cassette_revival_toronto.md",
            type="string",
            enum=["event_brief_cassette_revival_toronto.md", "event_brief_spring_showcase.md"],
            description="The event brief file to extract.",
        ),
    },
)
def globoticket_event_brief_extract():
    LLMFileAnalysisOperator(
        task_id="extract_event_brief",
        llm_conn_id="globoticket_llm",
        file_path=EVENT_BRIEFS + "{{ params.event_brief }}",
        file_conn_id="globoticket_blob",
        system_prompt=(
            "You extract event details from event briefs for the Globoticket intake team. "
            "Use only facts stated in the event brief. "
            "If a value is not stated, or is too vague to use, return null for that field. "
            "A value is too vague when it describes something instead of naming it, "
            "such as a city instead of a venue or a season instead of a date."
        ),
        prompt="Extract the event details from this event brief.",
        output_type=EventRequest,
    )


globoticket_event_brief_extract()
