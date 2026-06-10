"""
homeharvest.rightmove
~~~~~~~~~~~~~~~~~~~~~

Experimental scraper for Rightmove UK search pages.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlencode, urljoin

import requests

from .. import DEFAULT_HEADERS, Scraper
from ..models import Address, Description, ListingType, Property, PropertyType, ReturnType


class RightmoveScraper(Scraper):
    BASE_URL = "https://www.rightmove.co.uk"
    SEARCH_PATH = "/property-for-sale/find.html"
    DEFAULT_PAGE_SIZE = 24

    def _build_search_url(self, offset: int = 0) -> str:
        params: dict[str, Any] = {
            "searchLocation": self.location,
            "useLocationIdentifier": "false",
            "index": offset,
        }

        if self.listing_type == ListingType.FOR_RENT:
            path = "/property-to-rent/find.html"
        else:
            path = self.SEARCH_PATH

        if self.beds_min is not None:
            params["minBedrooms"] = self.beds_min
        if self.beds_max is not None:
            params["maxBedrooms"] = self.beds_max
        if self.price_min is not None:
            params["minPrice"] = self.price_min
        if self.price_max is not None:
            params["maxPrice"] = self.price_max

        if self.property_type:
            params["propertyTypes"] = ",".join(property_type.value for property_type in self.property_type)

        if self.exclude_pending:
            params["includeSSTC"] = "false"

        return f"{self.BASE_URL}{path}?{urlencode(params)}"

    def _fetch_search_page(self, offset: int = 0) -> str:
        headers = {
            **DEFAULT_HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Origin": self.BASE_URL,
            "Referer": self.BASE_URL,
        }
        response = requests.get(
            self._build_search_url(offset),
            headers=headers,
            proxies=self.proxies,
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    def search(self):
        html = self._fetch_search_page(self.offset)
        listings = parse_rightmove_search_page(html)

        if self.limit:
            listings = listings[: self.limit]

        if self.return_type == ReturnType.raw:
            return listings

        return [listing for listing in (rightmove_listing_to_property(item, self.location, self.listing_type) for item in listings) if listing]


def parse_rightmove_search_page(html: str) -> list[dict]:
    """Extract listing-like objects from Rightmove embedded page JSON."""
    payloads = _extract_embedded_json(html)
    listings: list[dict] = []
    seen: set[str] = set()

    for payload in payloads:
        for item in _walk_listing_dicts(payload):
            listing_id = str(item.get("id") or item.get("propertyId") or item.get("listingId") or "")
            if listing_id and listing_id in seen:
                continue
            if listing_id:
                seen.add(listing_id)
            listings.append(item)

    return listings


def rightmove_listing_to_property(item: dict, search_location: str, listing_type: ListingType | list[ListingType] | None) -> Property | None:
    property_id = str(item.get("id") or item.get("propertyId") or item.get("listingId") or "")
    property_url = _property_url(item)

    if not property_id or not property_url:
        return None

    location = item.get("location") if isinstance(item.get("location"), dict) else {}
    price = item.get("price") if isinstance(item.get("price"), dict) else {}
    listing_status = "FOR_RENT" if listing_type == ListingType.FOR_RENT else "FOR_SALE"

    return Property(
        source_site="rightmove",
        country="GB",
        currency=_currency(price) or "GBP",
        property_url=property_url,
        property_id=property_id,
        listing_id=property_id,
        status=listing_status,
        address=Address(
            full_line=item.get("displayAddress"),
            city=search_location,
        ),
        list_price=_price_amount(price),
        latitude=_number(location.get("latitude")),
        longitude=_number(location.get("longitude")),
        description=Description(
            primary_photo=_primary_photo(item),
            style=PropertyType.OTHER,
            beds=_integer(item.get("bedrooms")),
            baths_full=_integer(item.get("bathrooms")),
            text=item.get("summary") or item.get("description"),
        ),
    )


def _extract_embedded_json(html: str) -> list[Any]:
    payloads: list[Any] = []

    for pattern in (
        r"window\.jsonModel\s*=\s*(\{.*?\})\s*</script>",
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
    ):
        for match in re.finditer(pattern, html, flags=re.DOTALL):
            candidate = match.group(1).strip().rstrip(";")
            try:
                payloads.append(json.loads(candidate))
            except json.JSONDecodeError:
                continue

    return payloads


def _walk_listing_dicts(value: Any):
    if isinstance(value, dict):
        if _looks_like_listing(value):
            yield value
        for child in value.values():
            yield from _walk_listing_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_listing_dicts(child)


def _looks_like_listing(value: dict) -> bool:
    has_id = any(key in value for key in ("id", "propertyId", "listingId"))
    has_address = any(key in value for key in ("displayAddress", "address"))
    has_url = any(key in value for key in ("propertyUrl", "url", "path"))
    return has_id and has_address and has_url


def _property_url(item: dict) -> str | None:
    value = item.get("propertyUrl") or item.get("url") or item.get("path")
    if not value:
        return None
    return urljoin(RightmoveScraper.BASE_URL, str(value))


def _price_amount(price: dict) -> int | None:
    amount = _integer(price.get("amount"))
    if amount is not None:
        return amount

    display_prices = price.get("displayPrices")
    if isinstance(display_prices, list) and display_prices:
        display_price = display_prices[0].get("displayPrice") if isinstance(display_prices[0], dict) else display_prices[0]
        return _integer(display_price)

    return None


def _currency(price: dict) -> str | None:
    currency = price.get("currencyCode") or price.get("currency")
    return str(currency).upper() if currency else None


def _primary_photo(item: dict) -> str | None:
    images = item.get("propertyImages")
    if isinstance(images, dict):
        images = images.get("images")
    if not isinstance(images, list) or not images:
        return None

    first = images[0]
    if not isinstance(first, dict):
        return None

    src = first.get("srcUrl") or first.get("url")
    return str(src) if src else None


def _integer(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
