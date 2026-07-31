from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx


@dataclass
class GeoPoint:
    name: str
    display_name: str
    lat: float
    lon: float
    place_type: Optional[str] = None
    place_class: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    importance: Optional[float] = None


class NominatimGeocodeTool:
    """
    A tiny external grounding tool for the travel-agent-runtime demo.

    This is intentionally not a production map stack. It is used to verify
    whether a destination or POI can be resolved into real-world coordinates.
    """

    def __init__(
        self,
        base_url: str = "https://nominatim.openstreetmap.org/search",
        user_agent: str = "travel-agent-runtime-demo/0.1 (https://github.com/YingzuoLiu/travel-agent-runtime-demo)",
        min_interval_seconds: float = 1.1,
        timeout_seconds: float = 10.0,
    ):
        self.base_url = base_url
        self.user_agent = user_agent
        self.min_interval_seconds = min_interval_seconds
        self.timeout_seconds = timeout_seconds

        self._last_request_time = 0.0
        self._cache: Dict[Tuple[str, Optional[str], int], List[GeoPoint]] = {}

    def search(
        self,
        query: str,
        country: Optional[str] = None,
        limit: int = 3,
    ) -> List[GeoPoint]:
        cache_key = (query.lower().strip(), country.lower().strip() if country else None, limit)

        if cache_key in self._cache:
            return self._cache[cache_key]

        self._respect_rate_limit()

        full_query = f"{query}, {country}" if country else query

        params = {
            "q": full_query,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": limit,
            "dedupe": 1,
        }

        headers = {
            "User-Agent": self.user_agent,
        }

        with httpx.Client(timeout=self.timeout_seconds, headers=headers) as client:
            response = client.get(self.base_url, params=params)
            response.raise_for_status()
            raw_results = response.json()

        results = [self._parse_result(item) for item in raw_results]
        self._cache[cache_key] = results
        return results

    def _respect_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time

        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)

        self._last_request_time = time.monotonic()

    def _parse_result(self, item: Dict[str, Any]) -> GeoPoint:
        address = item.get("address", {})

        city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or address.get("county")
        )

        return GeoPoint(
            name=item.get("name") or item.get("display_name", "").split(",")[0],
            display_name=item.get("display_name", ""),
            lat=float(item["lat"]),
            lon=float(item["lon"]),
            place_type=item.get("type"),
            place_class=item.get("category") or item.get("class"),
            country=address.get("country"),
            city=city,
            importance=item.get("importance"),
        )
