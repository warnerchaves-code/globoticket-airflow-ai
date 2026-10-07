"""Read-only lookup tools for the Globoticket conflict-check agent.

Each tool does one lookup with typed inputs, through an existing Airflow Connection, and
logs what it was asked and what it found. The provider logs tool arguments only at
DEBUG, so these log lines are the evidence you read in the task log.
"""
import logging
from datetime import date

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


def venue_name(venue: str) -> str:
    """'Orpheum Hall, Toronto' -> 'orpheum hall'. The catalog and the API store the venue name alone."""
    return venue.split(",")[0].strip().lower()


def find_catalog_events(act: str, venue: str, date_from: date, date_to: date) -> list[dict]:
    """Find catalog events for this act, or at this venue, between date_from and date_to (inclusive)."""
    rows = PostgresHook(postgres_conn_id="globoticket_pg").get_records(
        "SELECT event_id, name, act, venue, city, event_date, status FROM events "
        "WHERE (lower(act) = lower(%s) OR lower(venue) = %s) AND event_date BETWEEN %s AND %s "
        "ORDER BY event_date",
        parameters=(act, venue_name(venue), date_from, date_to),
    )
    columns = ("event_id", "name", "act", "venue", "city", "event_date", "status")
    events = [dict(zip(columns, (str(v) for v in row))) for row in rows]
    log.info("find_catalog_events(act=%r, venue=%r, %s to %s) found: %s",
             act, venue, date_from, date_to, ", ".join(e["event_id"] for e in events) or "none")
    return events


def find_venue_bookings(venue: str, date_from: date, date_to: date) -> list[dict]:
    """Find venue bookings at this venue that overlap date_from to date_to (inclusive)."""
    response = HttpHook(method="GET", http_conn_id="globoticket_api").run(
        "/venue-bookings",
        data={"venue": venue_name(venue), "date_from": str(date_from), "date_to": str(date_to)},
    )
    bookings = response.json()
    log.info("find_venue_bookings(venue=%r, %s to %s) found: %s",
             venue, date_from, date_to, ", ".join(b["booking_id"] for b in bookings) or "none")
    return bookings
