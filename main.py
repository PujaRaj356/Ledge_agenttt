from __future__ import annotations

import asyncio
import json
import os
import re
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

BUSINESS_TZ = ZoneInfo("Asia/Kolkata")
DISCOVERY_URL = os.getenv("LEDGER_DISCOVERY_URL", "").strip()
HTTP_TIMEOUT = float(os.getenv("LEDGER_HTTP_TIMEOUT", "45"))
APP_BUILD = "ledger-agent-fix-20261018-v2"

DATA: dict[str, Any] = {
    "ready": False,
    "error": None,
    "orders": [],
    "rates": {"USD": 1.0},
    "products": [],
    "regions": [],
}
_LOAD_LOCK = asyncio.Lock()


class AnswerRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=BUSINESS_TZ)
        return parsed.astimezone(BUSINESS_TZ)
    except (TypeError, ValueError):
        return None


async def request_json(client: httpx.AsyncClient, url: str) -> Any:
    response = await client.get(url, headers={"Accept": "application/json", "User-Agent": "LedgerAgent/1.0"})
    response.raise_for_status()
    return response.json()


async def load_ledger(force: bool = False) -> None:
    if DATA["ready"] and not force:
        return
    async with _LOAD_LOCK:
        if DATA["ready"] and not force:
            return
        if not DISCOVERY_URL:
            DATA["error"] = "LEDGER_DISCOVERY_URL is not configured"
            return
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
                discovery = await request_json(client, DISCOVERY_URL)
                links = discovery.get("links", {}) if isinstance(discovery, dict) else {}
                export_url = links.get("export")
                rates_url = links.get("rates")
                if not export_url or not rates_url:
                    raise ValueError("Discovery response must provide export and rates links")

                export_response, rates = await asyncio.gather(
                    client.get(export_url, headers={"Accept": "application/x-ndjson", "User-Agent": "LedgerAgent/1.0"}),
                    request_json(client, rates_url),
                )
                export_response.raise_for_status()
                raw = export_response.text
                rows: list[dict[str, Any]] = []
                # The export endpoint returns newline-delimited JSON (one order per line).
                for line_number, line in enumerate(raw.splitlines(), start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        # If a provider wraps NDJSON in a JSON document, handle that too.
                        if line_number == 1:
                            try:
                                obj = json.loads(raw)
                                if isinstance(obj, list):
                                    rows = [x for x in obj if isinstance(x, dict)]
