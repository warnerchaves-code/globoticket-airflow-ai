"""Typed outputs for the Globoticket intake pipeline."""
from datetime import datetime

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
