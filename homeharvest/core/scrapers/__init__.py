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
    listing_type: ListingType | list[ListingType] | None
    property_type: list[SearchPropertyType] | None = None
    radius: float | None = None
    mls_only: bool | None = False
    proxy: str | None = None
    last_x_days: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    date_from_precision: str | None = None  # "day" or "hour"
    date_to_precision: str | None = None    # "day" or "hour"
    foreclosure: bool | None = False
    extra_property_data: bool | None = True
    exclude_pending: bool | None = False
    limit: int = 10000
    offset: int = 0
    return_type: ReturnType = ReturnType.pandas

    # New date/time filtering parameters
    past_hours: int | None = None

    # New last_update_date filtering parameters
    updated_since: str | None = None
    updated_in_past_hours: int | None = None

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

    # Pagination control
    parallel: bool = True


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
                total=3, backoff_factor=4, status_forcelist=[429], allowed_methods=frozenset(["GET", "POST"])
            )

            adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
            Scraper.session.mount("http://", adapter)
            Scraper.session.mount("https://", adapter)
            Scraper.session.headers.update(
                {
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'no-cache',
                    'Origin': 'https://www.realtor.com',
                    'Pragma': 'no-cache',
                    'Referer': 'https://www.realtor.com/',
                    'rdc-client-name': 'RDC_WEB_SRP_FS_PAGE',
                    'rdc-client-version': '3.0.2515',
                    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
                    'x-is-bot': 'false',
                }
            )

        self.proxy = scraper_input.proxy
        if self.proxy:
            proxies = {"http": self.proxy, "https": self.proxy}
            self.session.proxies.update(proxies)

        self.listing_type = scraper_input.listing_type
        self.radius = scraper_input.radius
        self.last_x_days = scraper_input.last_x_days
        self.mls_only = scraper_input.mls_only
        self.date_from = scraper_input.date_from
        self.date_to = scraper_input.date_to
        self.date_from_precision = scraper_input.date_from_precision
        self.date_to_precision = scraper_input.date_to_precision
        self.foreclosure = scraper_input.foreclosure
        self.extra_property_data = scraper_input.extra_property_data
        self.exclude_pending = scraper_input.exclude_pending
        self.limit = scraper_input.limit
        self.offset = scraper_input.offset
        self.return_type = scraper_input.return_type

        # New date/time filtering
        self.past_hours = scraper_input.past_hours

        # New last_update_date filtering
        self.updated_since = scraper_input.updated_since
        self.updated_in_past_hours = scraper_input.updated_in_past_hours

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

        # Pagination control
        self.parallel = scraper_input.parallel

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
