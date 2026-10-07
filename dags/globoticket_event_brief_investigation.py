"""Investigate a complete Globoticket event brief for conflicts with an agent."""
import logging
from datetime import timedelta

import pendulum
from pydantic_ai import FunctionToolset
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowFailException

from globoticket_models import EventRequest
from globoticket_tools import find_catalog_events, find_venue_bookings

log = logging.getLogger(__name__)

EVENT_BRIEFS = "abfs://globoticket-intake/event-briefs/"

# What the intake team needs before an event brief can go any further.
REQUIRED_FIELDS = ["event_name", "venue", "event_datetime",
                   "expected_attendance", "proposed_ticket_price"]

# Bounded retries and per-attempt usage limits for the extraction and routing tasks.
AI_TASK_LIMITS = dict(
    retries=2,
    retry_delay=timedelta(seconds=30),
    usage_limits=UsageLimits(request_limit=3, total_tokens_limit=2000),
)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["globoticket"],
    params={
        "event_brief": Param(
            "event_brief_cassette_revival_toronto.md",
            type="string",
            enum=["event_brief_cassette_revival_toronto.md", "event_brief_paper_lanterns_dublin.md"],
            description="The event brief file to investigate.",
        ),
    },
)
def globoticket_event_brief_investigation():
    extract = LLMFileAnalysisOperator(
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
        **AI_TASK_LIMITS,
    )

    route = LLMBranchOperator(
        task_id="route",
        llm_conn_id="globoticket_llm",
        system_prompt=(
            "You route event requests for the Globoticket intake team. "
            "An event request is complete when every required detail is stated and specific: "
            "the event name, a named venue, a specific date and time, the expected attendance "
            "and the proposed ticket price."
        ),
        prompt=(
            "Route this event request.\n\n"
            "Extracted event request:\n"
            "{{ ti.xcom_pull(task_ids='extract_event_brief').model_dump_json(indent=2) }}"
        ),
        branches={
            "check_conflicts": "Every required detail is stated and specific.",
            "request_clarification": "A required detail is missing or vague.",
        },
        **AI_TASK_LIMITS,
    )

    @task.agent(
        llm_conn_id="globoticket_llm",
        system_prompt=(
            "You check event requests for conflicts before the Globoticket intake team reviews them. "
            "Use find_catalog_events and find_venue_bookings, both from the day before to the day "
            "after the event date. Report only what the tools return, with the IDs they found. "
            "You can't approve, change or publish anything."
        ),
        toolsets=[FunctionToolset([find_catalog_events, find_venue_bookings])],
        usage_limits=UsageLimits(request_limit=6, tool_calls_limit=6, total_tokens_limit=5000),
        # One tool per turn: the model calls a tool, reads the result, then decides the next step
        agent_params={"model_settings": {"parallel_tool_calls": False}},
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def check_conflicts(event: EventRequest) -> str:
        # The hard rule, in code, before the model is called
        missing = [name for name in REQUIRED_FIELDS if getattr(event, name) is None]
        if missing:
            raise AirflowFailException(f"Required fields are empty: {', '.join(missing)}")
        return (
            "Check this event request for conflicts.\n\n"
            f"Act: {event.event_name}\n"
            f"Venue: {event.venue}\n"
            f"Event date: {event.event_datetime.date()}"
        )

    @task(retries=0)
    def request_clarification(event: EventRequest):
        missing = [name for name in REQUIRED_FIELDS if getattr(event, name) is None]
        log.info("Ask the promoter for: %s", ", ".join(missing) or "the vague details")

    extract >> route
    route >> [check_conflicts(extract.output), request_clarification(extract.output)]


globoticket_event_brief_investigation()
