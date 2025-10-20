from __future__ import annotations
from typing import Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import uuid
from ...exceptions import AuthenticationError
from .models import Property, ListingType, SiteName, SearchPropertyType, ReturnType
import json
from pydantic import BaseModel


class ScraperInput(BaseModel):
    location: str
    listing_type: ListingType
    property_type: list[SearchPropertyType] | None = None
    radius: float | None = None
    mls_only: bool | None = False
    proxy: str | None = None
    last_x_days: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    foreclosure: bool | None = False
    extra_property_data: bool | None = True
    exclude_pending: bool | None = False
    limit: int = 10000
    return_type: ReturnType = ReturnType.pandas

    # New date/time filtering parameters
    past_hours: int | None = None
    datetime_from: str | None = None
    datetime_to: str | None = None

    # New property filtering parameters
    beds_min: int | None = None
    beds_max: int | None = None
    baths_min: float | None = None
    baths_max: float | None = None
    sqft_min: int | None = None
    sqft_max: int | None = None
    price_min: int | None = None
    price_max: int | None = None
    lot_sqft_min: int | None = None
    lot_sqft_max: int | None = None
    year_built_min: int | None = None
    year_built_max: int | None = None

    # New sorting parameters
    sort_by: str | None = None
    sort_direction: str = "desc"


class Scraper:
    session = None

    def __init__(
        self,
        scraper_input: ScraperInput,
    ):
        self.location = scraper_input.location
        self.listing_type = scraper_input.listing_type
        self.property_type = scraper_input.property_type

        if not self.session:
            Scraper.session = requests.Session()
            retries = Retry(
                total=3, backoff_factor=4, status_forcelist=[429, 403], allowed_methods=frozenset(["GET", "POST"])
            )

            adapter = HTTPAdapter(max_retries=retries)
            Scraper.session.mount("http://", adapter)
            Scraper.session.mount("https://", adapter)
            Scraper.session.headers.update(
                {
                    "accept": "application/json, text/javascript",
                    "accept-language": "en-US,en;q=0.9",
                    "cache-control": "no-cache",
                    "content-type": "application/json",
                    "origin": "https://www.realtor.com",
                    "pragma": "no-cache",
                    "priority": "u=1, i",
                    "rdc-ab-tests": "commute_travel_time_variation:v1",
                    "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
                }
            )

        if scraper_input.proxy:
            proxy_url = scraper_input.proxy
            proxies = {"http": proxy_url, "https": proxy_url}
            self.session.proxies.update(proxies)

        self.listing_type = scraper_input.listing_type
        self.radius = scraper_input.radius
        self.last_x_days = scraper_input.last_x_days
        self.mls_only = scraper_input.mls_only
        self.date_from = scraper_input.date_from
        self.date_to = scraper_input.date_to
        self.foreclosure = scraper_input.foreclosure
        self.extra_property_data = scraper_input.extra_property_data
        self.exclude_pending = scraper_input.exclude_pending
        self.limit = scraper_input.limit
        self.return_type = scraper_input.return_type

        # New date/time filtering
        self.past_hours = scraper_input.past_hours
        self.datetime_from = scraper_input.datetime_from
        self.datetime_to = scraper_input.datetime_to

        # New property filtering
        self.beds_min = scraper_input.beds_min
        self.beds_max = scraper_input.beds_max
        self.baths_min = scraper_input.baths_min
        self.baths_max = scraper_input.baths_max
        self.sqft_min = scraper_input.sqft_min
        self.sqft_max = scraper_input.sqft_max
        self.price_min = scraper_input.price_min
        self.price_max = scraper_input.price_max
        self.lot_sqft_min = scraper_input.lot_sqft_min
        self.lot_sqft_max = scraper_input.lot_sqft_max
        self.year_built_min = scraper_input.year_built_min
        self.year_built_max = scraper_input.year_built_max

        # New sorting
        self.sort_by = scraper_input.sort_by
        self.sort_direction = scraper_input.sort_direction

    def search(self) -> list[Union[Property | dict]]: ...

    @staticmethod
    def _parse_home(home) -> Property: ...

    def handle_location(self): ...

    @staticmethod
    def get_access_token():
        device_id = str(uuid.uuid4()).upper()

        response = requests.post(
            "https://graph.realtor.com/auth/token",
            headers={
                "Host": "graph.realtor.com",
                "Accept": "*/*",
                "Content-Type": "Application/json",
                "X-Client-ID": "rdc_mobile_native,iphone",
                "X-Visitor-ID": device_id,
                "X-Client-Version": "24.21.23.679885",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": "Realtor.com/24.21.23.679885 CFNetwork/1494.0.7 Darwin/23.4.0",
            },
            data=json.dumps(
                {
                    "grant_type": "device_mobile",
                    "device_id": device_id,
                    "client_app_id": "rdc_mobile_native,24.21.23.679885,iphone",
                }
            ),
        )

        data = response.json()

        if not (access_token := data.get("access_token")):
            raise AuthenticationError(
                "Failed to get access token, use a proxy/vpn or wait a moment and try again.", response=response
            )

        return access_token
