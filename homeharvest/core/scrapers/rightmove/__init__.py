"""
homeharvest.rightmove
~~~~~~~~~~~~~~~~~~~~~

Experimental scraper for Rightmove UK search pages.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlencode, urljoin

import requests

from .. import DEFAULT_HEADERS, Scraper
from ..models import Address, Description, ListingType, Property, PropertyType, ReturnType


class RightmoveScraper(Scraper):
    BASE_URL = "https://www.rightmove.co.uk"
    SEARCH_PATH = "/property-for-sale/find.html"
    DEFAULT_PAGE_SIZE = 24
    LOCATION_IDENTIFIERS = {
        "london": "REGION^87490",
        "manchester": "REGION^904",
        "birmingham": "REGION^162",
        "leeds": "REGION^787",
        "liverpool": "REGION^813",
    }

    def _build_search_url(self, offset: int = 0) -> str:
        params: dict[str, Any] = {
            "locationIdentifier": self._resolve_location_identifier(),
            "index": offset,
            "sortType": 2,
            "propertyTypes": "",
            "mustHave": "",
            "dontShow": "",
            "furnishTypes": "",
            "keywords": "",
            "channel": "BUY",
            "currencyCode": "GBP",
            "includeSSTC": "false" if self.exclude_pending else "true",
        }

        if self.listing_type == ListingType.FOR_RENT:
            path = "/property-to-rent/find.html"
            params["channel"] = "RENT"
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

        return f"{self.BASE_URL}{path}?{urlencode(params)}"

    def _resolve_location_identifier(self) -> str:
        if "^" in self.location:
            return self.location

        key = self.location.strip().lower()
        if key in self.LOCATION_IDENTIFIERS:
            return self.LOCATION_IDENTIFIERS[key]

        raise ValueError(
            f"Rightmove location '{self.location}' is not mapped yet. "
            "Use a Rightmove locationIdentifier like 'REGION^87490' or add the city to LOCATION_IDENTIFIERS."
        )

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
        listings: list[dict] = []
        seen: set[str] = set()
        offset = self.offset

        while len(listings) < self.limit:
            html = self._fetch_search_page(offset)
            page_listings = parse_rightmove_search_page(html)
            if not page_listings:
                break

            before = len(listings)
            for item in page_listings:
                listing_id = str(item.get("id") or item.get("propertyId") or item.get("listingId") or "")
                if listing_id and listing_id in seen:
                    continue
                if listing_id:
                    seen.add(listing_id)
                listings.append(item)
                if len(listings) >= self.limit:
                    break

            if len(listings) == before or len(page_listings) < self.DEFAULT_PAGE_SIZE:
                break
            offset += self.DEFAULT_PAGE_SIZE

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
    list_price = _price_amount(price)
    sqft = _display_size_sqft(item.get("displaySize"))

    return Property(
        source_site="rightmove",
        country=str(item.get("countryCode") or "GB"),
        currency=_currency(price) or "GBP",
        property_url=property_url,
        property_id=property_id,
        listing_id=property_id,
        status=listing_status,
        address=Address(
            full_line=item.get("displayAddress"),
            city=search_location,
        ),
        list_price=list_price,
        list_date=_parse_datetime(item.get("firstVisibleDate")),
        last_update_date=_parse_datetime(item.get("updateDate") or (item.get("listingUpdate") or {}).get("listingUpdateDate")),
        prc_sqft=int(list_price / sqft) if list_price and sqft else None,
        latitude=_number(location.get("latitude")),
        longitude=_number(location.get("longitude")),
        description=Description(
            primary_photo=_primary_photo(item),
            alt_photos=_photos(item)[1:] or None,
            style=_property_style(item),
            beds=_integer(item.get("bedrooms")),
            baths_full=_integer(item.get("bathrooms")),
            sqft=sqft,
            text=item.get("summary") or item.get("description"),
        ),
        advertisers=_advertisers(item),
        tags=_tags(item),
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
    photos = _photos(item)
    return photos[0] if photos else None


def _photos(item: dict) -> list[str]:
    images = item.get("propertyImages")
    if isinstance(images, dict):
        images = images.get("images")
    if not images:
        images = item.get("images")
    if not isinstance(images, list) or not images:
        return []

    photos: list[str] = []
    for image in images:
        if not isinstance(image, dict):
            continue
        src = image.get("srcUrl") or image.get("url")
        if src:
            photos.append(str(src))
    return photos


def _property_style(item: dict) -> PropertyType:
    subtype = str(item.get("propertySubType") or item.get("propertyTypeFullDescription") or "").lower()
    if "flat" in subtype or "apartment" in subtype:
        return PropertyType.CONDO
    if "terrace" in subtype or "townhouse" in subtype:
        return PropertyType.TOWNHOMES
    if "detached" in subtype or "bungalow" in subtype or "house" in subtype:
        return PropertyType.SINGLE_FAMILY
    if "land" in subtype:
        return PropertyType.LAND
    return PropertyType.OTHER


def _display_size_sqft(value: Any) -> int | None:
    if not value:
        return None
    text = str(value).lower()
    if "sq" not in text:
        return None
    return _integer(text)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _advertisers(item: dict):
    from ..models import Advertisers, Agent, Office

    customer = item.get("customer") if isinstance(item.get("customer"), dict) else {}
    if not customer:
        return None

    return Advertisers(
        agent=Agent(
            uuid=str(customer.get("branchId")) if customer.get("branchId") else None,
            name=customer.get("brandTradingName") or customer.get("branchDisplayName"),
            href=urljoin(RightmoveScraper.BASE_URL, customer.get("branchLandingPageUrl") or ""),
            phones=[{"number": customer.get("contactTelephone"), "type": "office"}] if customer.get("contactTelephone") else None,
        ),
        office=Office(
            uuid=str(customer.get("branchId")) if customer.get("branchId") else None,
            name=customer.get("branchDisplayName") or customer.get("branchName"),
            href=urljoin(RightmoveScraper.BASE_URL, customer.get("branchLandingPageUrl") or ""),
            phones=[{"number": customer.get("contactTelephone"), "type": "office"}] if customer.get("contactTelephone") else None,
        ),
    )


def _tags(item: dict) -> list[str] | None:
    tags = []
    for key in ("propertySubType", "tenure", "transactionType", "displayStatus"):
        value = item.get(key)
        if isinstance(value, dict):
            value = value.get("tenureType")
        if value:
            tags.append(str(value))
    return tags or None


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
