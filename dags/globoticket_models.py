"""Typed outputs for the Globoticket intake pipeline."""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class EventRequest(BaseModel):
    """What a promoter's request states about an event.

    Every field is optional so the model can report that a request doesn't
    state something. Whether a request is complete is decided downstream.
    """

    event_name: str | None = Field(
        default=None, description="The act or event name, as written in the request.")
    venue: str | None = Field(
        default=None, description="The venue's name. Leave empty unless a specific venue is named.")
    event_datetime: datetime | None = Field(
        default=None, description="When the show starts. Leave empty unless a specific date is stated.")
    expected_attendance: int | None = Field(
        default=None, description="The expected number of attendees, if the request gives one.")
    proposed_ticket_price: float | None = Field(
        default=None, description="The proposed price of one ticket. Leave empty unless a price is stated.")


class InvestigationResult(BaseModel):
    """What the conflict-check agent found, for the intake team to review.

    The agent fills this in from what its tools returned. An empty list means the
    tools found nothing of that kind.
    """

    conflict_status: Literal["conflict", "no_conflict"] = Field(
        description="conflict if the tools found any matching catalog event or overlapping booking, "
                    "otherwise no_conflict.")
    explanation: str = Field(
        description="One or two sentences on what the tools found.")
    matching_event_ids: list[str] = Field(
        default_factory=list,
        description="Catalog event IDs the tools returned that match this request. Empty if none.")
    conflicting_booking_ids: list[str] = Field(
        default_factory=list,
        description="Venue booking IDs the tools returned that overlap this request. Empty if none.")
