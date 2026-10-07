"""Globoticket bookings API: a small, read-only, authenticated service.

It stands in for a Globoticket system the data team doesn't own. Module 3's agent reads
venue bookings from it through a tool, using the globoticket_api Connection.

Run:  uvicorn main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

API_TOKEN = os.environ.get("GLOBOTICKET_API_TOKEN", "dev-token-change-me")
BOOKINGS: list[dict] = json.loads((Path(__file__).parent / "bookings.json").read_text(encoding="utf-8"))

app = FastAPI(title="Globoticket Bookings API", version="1.0.0",
              description="Read-only venue bookings for Globoticket's intake pipeline.")
bearer = HTTPBearer(auto_error=True)


def require_token(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> None:
    if creds.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or expired API token.",
                            headers={"WWW-Authenticate": "Bearer"})


def _venue_name(venue: str) -> str:
    # "Orpheum Hall, Toronto" and "orpheum hall" both mean the venue named Orpheum Hall
    return venue.split(",")[0].strip().lower()


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "bookings": len(BOOKINGS)}


@app.get("/venue-bookings", tags=["bookings"], dependencies=[Depends(require_token)])
def venue_bookings(venue: str = Query(...), date_from: date = Query(...), date_to: date = Query(...)) -> list[dict]:
    """Bookings at this venue that overlap date_from to date_to (inclusive)."""
    name = _venue_name(venue)
    return [b for b in BOOKINGS
            if b["venue"].lower() == name
            and date.fromisoformat(b["start"]) <= date_to
            and date.fromisoformat(b["end"]) >= date_from]


@app.get("/venue-bookings/{booking_id}", tags=["bookings"], dependencies=[Depends(require_token)])
def venue_booking(booking_id: str) -> dict:
    """One booking by ID, or 404."""
    for b in BOOKINGS:
        if b["booking_id"] == booking_id:
            return b
    raise HTTPException(status_code=404, detail=f"No booking {booking_id}.")
