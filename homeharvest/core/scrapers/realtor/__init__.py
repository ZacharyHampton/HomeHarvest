"""
homeharvest.realtor.__init__
~~~~~~~~~~~~

This module implements the scraper for realtor.com
"""

from __future__ import annotations

import copy
import json
import logging
import random
import threading
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from json import JSONDecodeError
from typing import Dict, Union

from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_not_exception_type,
    wait_exponential,
    stop_after_attempt,
)

from .. import Scraper, DEFAULT_HEADERS
from ..http import HttpResponse
from ....exceptions import AuthenticationError, RateLimitError, HomeHarvestWarning, NetworkError
from ..models import (
    Property,
    ListingType,
    ReturnType
)
from .queries import GENERAL_RESULTS_QUERY, HOMES_DATA, SEARCH_SUGGESTIONS_QUERY, EXTRA_PROPERTY_FRAGMENT
from .processors import (
    process_property,
    process_extra_property_details,
    get_key
)

logger = logging.getLogger("homeharvest")

#: Errors that mean "this request failed, but results fetched so far are still good".
RECOVERABLE_ERRORS = (AuthenticationError, RateLimitError, NetworkError, JSONDecodeError)

#: Listing types searched when listing_type=None and a date filter is used.
#: OFF_MARKET has no list/sold date to filter on, so it is left out in that case.
DATED_LISTING_TYPES = [
    ListingType.FOR_SALE,
    ListingType.FOR_RENT,
    ListingType.SOLD,
    ListingType.PENDING,
]


class RealtorScraper(Scraper):
    SEARCH_GQL_URL = "https://www.realtor.com/frontdoor/graphql"
    NUM_PROPERTY_WORKERS = 20  #: CPU-only parsing threads (no network)
    DEFAULT_PAGE_SIZE = 200

    #: Politeness / resilience settings
    MAX_PAGE_WORKERS = 4  #: max concurrent page requests when parallel=True
    DETAIL_BATCH_SIZE = 20  #: properties per extra-details request (the API rejects large queries)
    DETAIL_WORKERS = 4  #: max concurrent extra-details requests when parallel=True (HttpClient caps the total at 4)
    MAX_RETRIES = 5  #: retries for 429 / 5xx responses
    NETWORK_RETRIES = 2  #: retries for connection / timeout / protocol errors
    BACKOFF_BASE = 1.0  #: seconds; doubles every retry
    BACKOFF_CAP = 30.0  #: max seconds to wait between retries
    RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

    #: sort fields the search API accepts (pending_date is sorted client-side only)
    SERVER_SORT_FIELDS = {"list_date", "sold_date", "list_price", "sqft", "beds", "baths", "last_update_date"}

    def __init__(self, scraper_input):
        super().__init__(scraper_input)

    @staticmethod
    def _minify_query(query: str) -> str:
        """Minify GraphQL query by collapsing whitespace to single spaces."""
        # Split on whitespace, filter empty strings, join with single space
        return ' '.join(query.split())

    @staticmethod
    def _sleep(seconds: float) -> None:
        time.sleep(seconds)

    def _backoff_seconds(self, attempt: int, response: HttpResponse | None = None) -> float:
        """Exponential backoff with full jitter, honoring Retry-After when the server sends it."""
        if response is not None:
            retry_after = response.headers.get("Retry-After") if response.headers else None
            if retry_after:
                try:
                    return min(self.BACKOFF_CAP, max(0.0, float(retry_after))) + random.uniform(0, 1)
                except ValueError:
                    pass
        delay = min(self.BACKOFF_CAP, self.BACKOFF_BASE * (2 ** attempt))
        return delay / 2 + random.uniform(0, delay / 2)

    def _graphql_post(self, query: str, variables: dict, operation_name: str) -> dict:
        """
        Execute a GraphQL query.

        Retries 429 / 5xx responses with exponential backoff and jitter. Raises
        RateLimitError if the server is still rate limiting after MAX_RETRIES, and
        AuthenticationError on 403 Forbidden.

        Args:
            query: GraphQL query string (must include operationName matching operation_name param)
            variables: Query variables dictionary
            operation_name: Name of the GraphQL operation

        Returns:
            Response JSON dictionary
        """
        payload = {
            "operationName": operation_name,
            "query": self._minify_query(query),
            "variables": variables,
        }
        body = json.dumps(payload, separators=(',', ':'))

        #: with a rotating proxy a 403 can clear up on the next request, so retry it a little,
        #: on a new connection (the pooled one would keep the same proxy exit IP)
        max_forbidden_retries = 2 if self.proxy else 0
        forbidden_retries = 0
        network_retries = 0
        attempt = 0

        #: another thread was told to slow down (429): wait with it instead of piling on
        wait = self.http.pause_remaining()
        if wait > 0:
            self._sleep(wait)

        while True:
            try:
                response = self.http.post(
                    self.SEARCH_GQL_URL, headers=DEFAULT_HEADERS, data=body, fresh_connection=forbidden_retries > 0
                )
            except NetworkError as exc:
                if network_retries >= self.NETWORK_RETRIES:
                    raise
                delay = self._backoff_seconds(network_retries)
                logger.info("%s on %s, retrying in %.1fs", exc, operation_name, delay)
                self._sleep(delay)
                network_retries += 1
                continue

            if response.status_code in self.RETRY_STATUS_CODES:
                if attempt >= self.MAX_RETRIES:
                    if response.status_code == 429:
                        raise RateLimitError(
                            f"Rate limited (429 Too Many Requests) after {self.MAX_RETRIES} retries. "
                            "Wait a few minutes, use a proxy, or use parallel=False.",
                            response=response,
                        )
                    response.raise_for_status()
                delay = self._backoff_seconds(attempt, response)
                logger.info("HTTP %s from %s, retrying in %.1fs", response.status_code, operation_name, delay)
                if response.status_code == 429:
                    self.http.pause(delay)
                self._sleep(delay)
                attempt += 1
                continue

            if response.status_code == 403:
                if forbidden_retries < max_forbidden_retries:
                    forbidden_retries += 1
                    self._sleep(self._backoff_seconds(forbidden_retries))
                    continue
                raise AuthenticationError(
                    "Received 403 Forbidden from the property data API. "
                    "You may be blocked for sending too many requests; wait a bit, use a proxy, or use parallel=False.",
                    response=response,
                )

            return response.json()

    @retry(
        retry=retry_if_not_exception_type((AuthenticationError, RateLimitError)),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def handle_location(self):
        variables = {
            "searchInput": {
                "search_term": self.location
            }
        }

        response_json = self._graphql_post(SEARCH_SUGGESTIONS_QUERY, variables, "Search_suggestions")

        if (
            response_json is None
            or "data" not in response_json
            or response_json["data"] is None
            or "search_suggestions" not in response_json["data"]
            or response_json["data"]["search_suggestions"] is None
            or "geo_results" not in response_json["data"]["search_suggestions"]
            or not response_json["data"]["search_suggestions"]["geo_results"]
        ):
            # If we got a 400 error with "Required parameter is missing", raise to trigger retry
            if response_json and "errors" in response_json:
                error_msgs = [e.get("message", "") for e in response_json.get("errors", [])]
                if any("Required parameter is missing" in msg for msg in error_msgs):
                    raise Exception(f"Transient API error: {error_msgs}")
            return None

        geo_result = response_json["data"]["search_suggestions"]["geo_results"][0]
        geo = geo_result.get("geo", {})

        result = {
            "text": geo_result.get("text"),
            "area_type": geo.get("area_type"),
            "city": geo.get("city"),
            "state_code": geo.get("state_code"),
            "postal_code": geo.get("postal_code"),
            "county": geo.get("county"),
            "centroid": geo.get("centroid"),
        }

        if geo.get("area_type") == "address":
            # Try to get mpr_id directly from API response first
            if geo.get("mpr_id"):
                result["mpr_id"] = geo.get("mpr_id")
            else:
                # Fallback: extract from _id field if it has addr: prefix
                geo_id = geo.get("_id", "")
                if geo_id.startswith("addr:"):
                    result["mpr_id"] = geo_id.replace("addr:", "")

        return result

    def get_latest_listing_id(self, property_id: str) -> str | None:
        query = """
                fragment ListingFragment on Listing {
                    listing_id
                    primary
                }
                query GetPropertyListingId($property_id: ID!) {
                    property(id: $property_id) {
                        listings {
                            ...ListingFragment
                        }
                    }
                }
                """

        variables = {"property_id": property_id}
        response_json = self._graphql_post(query, variables, "GetPropertyListingId")

        property_info = response_json["data"]["property"]
        if property_info["listings"] is None:
            return None

        primary_listing = next(
            (listing for listing in property_info["listings"] if listing["primary"]),
            None,
        )
        if primary_listing:
            return primary_listing["listing_id"]
        else:
            return property_info["listings"][0]["listing_id"]

    def handle_home(self, property_id: str) -> list[Property]:
        """Fetch single home with proper error handling."""
        query = (
            """query GetHomeDetails($property_id: ID!) {
                    home(property_id: $property_id) %s
                }"""
            % HOMES_DATA
        )

        variables = {"property_id": property_id}

        try:
            data = self._graphql_post(query, variables, "GetHomeDetails")

            # Check for errors or missing data
            if "errors" in data or "data" not in data:
                return []

            if data["data"] is None or "home" not in data["data"]:
                return []

            property_info = data["data"]["home"]
            if property_info is None:
                return []

            # Process based on return type
            if self.return_type != ReturnType.raw:
                return [process_property(property_info, self.mls_only, self.extra_property_data,
                                       self.exclude_pending, self.listing_type, get_key,
                                       process_extra_property_details)]
            else:
                return [property_info]

        except Exception:
            return []

    def general_search(
        self, variables: dict, search_type: str, finish: bool = True
    ) -> Dict[str, Union[int, Union[list[Property], list[dict]]]]:
        """
        Handles a location area & returns a list of properties.

        finish=False returns the raw results (no extra details, not processed); pass them
        to _finish_page later.
        """

        date_param = ""

        # Determine date field based on listing type
        # Convert listing_type to list for uniform handling
        if self.listing_type is None:
            # When None, return all common listing types as documented
            # Note: NEW_COMMUNITY, OTHER, and READY_TO_BUILD are excluded as they typically return no results
            listing_types = [
                ListingType.FOR_SALE,
                ListingType.FOR_RENT,
                ListingType.SOLD,
                ListingType.PENDING,
                ListingType.OFF_MARKET,
            ]
            date_field = None  # When no listing_type is specified, skip date filtering
        elif isinstance(self.listing_type, list):
            listing_types = self.listing_type
            # For multiple types, we'll use a general date field or skip
            date_field = None  # Skip date filtering for mixed types
        else:
            listing_types = [self.listing_type]
            # Determine date field for single type
            if self.listing_type == ListingType.SOLD:
                date_field = "sold_date"
            elif self.listing_type in [ListingType.FOR_SALE, ListingType.FOR_RENT]:
                date_field = "list_date"
            else:  # PENDING or other types
                # Skip server-side date filtering for PENDING as both pending_date and contract_date
                # filters are broken in the API. Client-side filtering will be applied later.
                date_field = None

        # Build date parameter (expand to full days if hour-based filtering is used)
        if date_field:
            # Check if we have hour precision (need to extract date part for API, then filter client-side)
            has_hour_precision = (self.date_from_precision == "hour" or self.date_to_precision == "hour")

            if has_hour_precision and (self.date_from or self.date_to):
                # Hour-based datetime filtering: extract date parts for API, client-side filter by hours
                from datetime import datetime

                min_date = None
                max_date = None

                if self.date_from:
                    try:
                        dt_from = datetime.fromisoformat(self.date_from.replace('Z', '+00:00'))
                        min_date = dt_from.strftime("%Y-%m-%d")
                    except (ValueError, AttributeError):
                        pass

                if self.date_to:
                    try:
                        dt_to = datetime.fromisoformat(self.date_to.replace('Z', '+00:00'))
                        max_date = dt_to.strftime("%Y-%m-%d")
                    except (ValueError, AttributeError):
                        pass

                if min_date and max_date:
                    date_param = f'{date_field}: {{ min: "{min_date}", max: "{max_date}" }}'
                elif min_date:
                    date_param = f'{date_field}: {{ min: "{min_date}" }}'
                elif max_date:
                    date_param = f'{date_field}: {{ max: "{max_date}" }}'

            elif self.past_hours:
                # Query API for past N days (minimum 1 day), client-side filter by hours
                days = max(1, int(self.past_hours / 24) + 1)  # Round up to cover the full period
                date_param = f'{date_field}: {{ min: "$today-{days}D" }}'

            elif self.date_from and self.date_to:
                date_param = f'{date_field}: {{ min: "{self.date_from}", max: "{self.date_to}" }}'
            elif self.last_x_days:
                date_param = f'{date_field}: {{ min: "$today-{self.last_x_days}D" }}'

        property_type_param = ""
        if self.property_type:
            property_types = [pt.value for pt in self.property_type]
            property_type_param = f"type: {json.dumps(property_types)}"

        # Build property filter parameters
        property_filters = []

        if self.beds_min is not None or self.beds_max is not None:
            beds_filter = "beds: {"
            if self.beds_min is not None:
                beds_filter += f" min: {self.beds_min}"
            if self.beds_max is not None:
                beds_filter += f" max: {self.beds_max}"
            beds_filter += " }"
            property_filters.append(beds_filter)

        if self.baths_min is not None or self.baths_max is not None:
            baths_filter = "baths: {"
            if self.baths_min is not None:
                baths_filter += f" min: {self.baths_min}"
            if self.baths_max is not None:
                baths_filter += f" max: {self.baths_max}"
            baths_filter += " }"
            property_filters.append(baths_filter)

        if self.sqft_min is not None or self.sqft_max is not None:
            sqft_filter = "sqft: {"
            if self.sqft_min is not None:
                sqft_filter += f" min: {self.sqft_min}"
            if self.sqft_max is not None:
                sqft_filter += f" max: {self.sqft_max}"
            sqft_filter += " }"
            property_filters.append(sqft_filter)

        if self.price_min is not None or self.price_max is not None:
            price_filter = "list_price: {"
            if self.price_min is not None:
                price_filter += f" min: {self.price_min}"
            if self.price_max is not None:
                price_filter += f" max: {self.price_max}"
            price_filter += " }"
            property_filters.append(price_filter)

        if self.lot_sqft_min is not None or self.lot_sqft_max is not None:
            lot_sqft_filter = "lot_sqft: {"
            if self.lot_sqft_min is not None:
                lot_sqft_filter += f" min: {self.lot_sqft_min}"
            if self.lot_sqft_max is not None:
                lot_sqft_filter += f" max: {self.lot_sqft_max}"
            lot_sqft_filter += " }"
            property_filters.append(lot_sqft_filter)

        if self.year_built_min is not None or self.year_built_max is not None:
            year_built_filter = "year_built: {"
            if self.year_built_min is not None:
                year_built_filter += f" min: {self.year_built_min}"
            if self.year_built_max is not None:
                year_built_filter += f" max: {self.year_built_max}"
            year_built_filter += " }"
            property_filters.append(year_built_filter)

        property_filters_param = "\n".join(property_filters)

        # Build sort parameter (fields the API rejects, like pending_date, are sorted client-side only)
        server_sort_by = self.sort_by if self.sort_by in self.SERVER_SORT_FIELDS else None
        if server_sort_by:
            sort_param = f"sort: [{{ field: {server_sort_by}, direction: {self.sort_direction} }}]"
        elif isinstance(self.listing_type, ListingType) and self.listing_type == ListingType.SOLD:
            sort_param = "sort: [{ field: sold_date, direction: desc }]"
        else:
            sort_param = ""  #: prioritize normal fractal sort from realtor

        # Handle PENDING with or_filters
        # Only use or_filters when PENDING is the only type or mixed only with FOR_SALE
        # Using or_filters with other types (SOLD, FOR_RENT, etc.) will exclude those types
        has_pending = ListingType.PENDING in listing_types
        other_types = [lt for lt in listing_types if lt not in [ListingType.PENDING, ListingType.FOR_SALE]]
        use_or_filters = has_pending and len(other_types) == 0
        pending_or_contingent_param = (
            "or_filters: { contingent: true, pending: true }" if use_or_filters else ""
        )

        # Build bucket parameter (only use fractal sort if no custom sort is specified)
        bucket_param = ""
        if not server_sort_by:
            bucket_param = 'bucket: { sort: "fractal_v1.1.3_fr" }'

        # Build status parameter
        # For PENDING, we need to query as FOR_SALE with or_filters for pending/contingent
        status_types = []
        for lt in listing_types:
            if lt == ListingType.PENDING:
                if ListingType.FOR_SALE not in status_types:
                    status_types.append(ListingType.FOR_SALE)
            else:
                if lt not in status_types:
                    status_types.append(lt)

        # Build status parameter string
        if status_types:
            status_values = [st.value.lower() for st in status_types]
            if len(status_values) == 1:
                status_param = f"status: {status_values[0]}"
            else:
                status_param = f"status: [{', '.join(status_values)}]"
        else:
            status_param = ""  # No status parameter means return all types

        open_house_param = self._open_house_param()

        is_foreclosure = ""

        if variables.get("foreclosure") is True:
            is_foreclosure = "foreclosure: true"
        elif variables.get("foreclosure") is False:
            is_foreclosure = "foreclosure: false"

        if search_type == "comps":  #: comps search, came from an address
            query = """query GetHomeSearch(
                    $coordinates: [Float]!
                    $radius: String!
                    $offset: Int!,
                    ) {
                        homeSearch: home_search(
                            query: {
                                %s
                                nearby: {
                                    coordinates: $coordinates
                                    radius: $radius
                                }
                                %s
                                %s
                                %s
                                %s
                                %s
                                %s
                            }
                            %s
                            limit: 200
                            offset: $offset
                    ) %s
                }""" % (
                is_foreclosure,
                status_param,
                date_param,
                property_type_param,
                property_filters_param,
                pending_or_contingent_param,
                open_house_param,
                sort_param,
                GENERAL_RESULTS_QUERY,
            )
        elif search_type == "area":  #: general search, came from a general location
            query = """query GetHomeSearch(
                                $search_location: SearchLocation,
                                $offset: Int
                            ) {
                                homeSearch: home_search(
                                    query: {
                                        %s
                                        search_location: $search_location
                                        %s
                                        %s
                                        %s
                                        %s
                                        %s
                                        %s
                                    }
                                    %s
                                    %s
                                    limit: 200
                                    offset: $offset
                                ) %s
                            }""" % (
                is_foreclosure,
                status_param,
                date_param,
                property_type_param,
                property_filters_param,
                pending_or_contingent_param,
                open_house_param,
                bucket_param,
                sort_param,
                GENERAL_RESULTS_QUERY,
            )
        else:  #: general search, came from an address
            query = (
                """query GetHomeSearch(
                        $property_id: [ID]!
                        $offset: Int!,
                    ) {
                        homeSearch: home_search(
                            query: {
                                property_id: $property_id
                            }
                            limit: 1
                            offset: $offset
                        ) %s
                    }"""
                % GENERAL_RESULTS_QUERY
            )

        response_json = self._graphql_post(query, variables, "GetHomeSearch")
        search_key = "homeSearch"

        properties: list[Union[Property, dict]] = []

        if (
            response_json is None
            or "data" not in response_json
            or response_json["data"] is None
            or search_key not in response_json["data"]
            or response_json["data"][search_key] is None
            or "results" not in response_json["data"][search_key]
        ):
            return {"total": 0, "properties": []}

        properties_list = response_json["data"][search_key]["results"]
        total_properties = response_json["data"][search_key]["total"]
        offset = variables.get("offset", 0)

        #: limit the number of properties to be processed
        #: example: offset=0, limit=250 -> the page at offset 200 keeps 50;
        #: offset=200, limit=200 -> the page at offset 200 keeps all 200
        properties_list: list[dict] = properties_list[: self.offset + self.limit - offset]

        return {
            "total": total_properties,
            "properties": self._finish_page(properties_list) if finish else properties_list,
        }

    def _finish_page(self, properties_list: list[dict]) -> list:
        """Add extra details (if asked for) to one page of raw results and process it."""
        if self.extra_property_data and not self._defer_extra_property_data:
            self._merge_extra_details(properties_list)
        return self._process_results(properties_list)

    #: set on the inner search of _search_pending_with_dates: fetch raw, enrich later
    _defer_extra_property_data = False

    def _merge_extra_details(self, properties_list: list[dict]) -> None:
        """Fetch extra details for raw search results and merge them in place."""
        if not properties_list:
            return
        property_ids = [data["property_id"] for data in properties_list]
        extra_property_details = self.get_bulk_prop_details(property_ids) or {}

        for result in properties_list:
            specific_details_for_property = dict(extra_property_details.get(result["property_id"], {}))

            #: address is retrieved on both homes and search homes, so when merged, homes overrides,
            # this gets the internal data we want and only updates that (migrate to a func if more fields)
            if "location" in specific_details_for_property:
                result["location"].update(specific_details_for_property["location"])
                del specific_details_for_property["location"]

            result.update(specific_details_for_property)

    def _process_results(self, properties_list: list[dict]) -> list:
        """Turn raw results into Property models (unless return_type is raw), keeping API order."""
        if self.return_type == ReturnType.raw or self._defer_extra_property_data:
            return properties_list

        with ThreadPoolExecutor(max_workers=self.NUM_PROPERTY_WORKERS) as executor:
            futures = [
                executor.submit(process_property, result, self.mls_only, self.extra_property_data,
                                self.exclude_pending, self.listing_type, get_key, process_extra_property_details)
                for result in properties_list
            ]
            return [prop for prop in (future.result() for future in futures) if prop]

    def _search_pending_with_dates(self, location_info: dict):
        """PENDING + date filter: the API can't filter or sort on pending_date, so fetch the pending
        listings raw, filter them client-side, and only then fetch extra details for the survivors."""
        inner = copy.copy(self)
        inner._defer_extra_property_data = True
        homes = inner._search_location(location_info)  #: raw dicts, already date-filtered

        if self.extra_property_data:
            self._merge_extra_details(homes)

        homes = self._process_results(homes)
        if self.sort_by:
            homes = self._apply_sort(homes)
        return homes

    def _open_house_param(self) -> str:
        """Server-side open house filter (only listings with an open house in the window)."""
        if self.open_house_from or self.open_house_to:
            min_date = self.open_house_from or "$today"
            if self.open_house_to:
                return f'open_house: {{ min: "{min_date}", max: "{self.open_house_to}" }}'
            return f'open_house: {{ min: "{min_date}" }}'
        if self.open_house:
            return 'open_house: { min: "$today" }'
        return ""

    def _has_date_filter(self) -> bool:
        return bool(self.last_x_days or self.past_hours or self.date_from or self.date_to)

    def _split_listing_types(self) -> list[ListingType] | None:
        """Listing types to search one at a time, or None for a single combined search.

        Each listing type filters on a different date (sold_date for sold, list_date for
        for_sale / for_rent, pending_date client-side for pending), so a date filter with
        several listing types needs one search per type (issue #143).
        """
        if not self._has_date_filter():
            return None
        if self.listing_type is None:
            return list(DATED_LISTING_TYPES)
        if isinstance(self.listing_type, list) and len(self.listing_type) > 1:
            return list(dict.fromkeys(self.listing_type))  #: dedupe, keep order
        return None

    def search(self):
        try:
            return self._search()
        finally:
            self.http.close()  #: drop the pooled connection (it reopens if this scraper is reused)

    def _search(self):
        location_info = self.handle_location()
        if not location_info:
            return []

        if location_info["area_type"] == "address" and not self.radius:  #: single address search, non comps
            return self.handle_home(location_info["mpr_id"])

        #: a one-item list behaves exactly like a single listing type
        if isinstance(self.listing_type, list) and len(self.listing_type) == 1:
            self.listing_type = self.listing_type[0]

        split_types = self._split_listing_types()
        if split_types:
            return self._search_each_listing_type(location_info, split_types)

        return self._search_location(location_info)

    def _search_each_listing_type(self, location_info: dict, listing_types: list[ListingType]):
        """Run one search per listing type (each with its own date field), merge, dedupe on property_id."""
        merged = []
        seen = set()

        for listing_type in listing_types:
            child = copy.copy(self)
            child.listing_type = listing_type
            child.sort_by = self.sort_by
            child.sort_direction = self.sort_direction

            #: same auto-sort scrape_property applies for a lone PENDING search
            if (
                listing_type == ListingType.PENDING
                and not self.sort_by
                and not (self.updated_since or self.updated_in_past_hours)
            ):
                child.sort_by = "pending_date"
                child.sort_direction = "desc"

            try:
                child_homes = child._search_location(location_info)
            except RECOVERABLE_ERRORS as exc:
                if not merged:
                    raise
                done = listing_types.index(listing_type)
                warnings.warn(
                    f"HomeHarvest stopped early: the {listing_type.value.lower()} search failed "
                    f"({type(exc).__name__}: {exc}). Returning the {len(merged)} properties from "
                    f"{', '.join(lt.value.lower() for lt in listing_types[:done])}; skipped "
                    f"{', '.join(lt.value.lower() for lt in listing_types[done:])}.",
                    HomeHarvestWarning,
                    stacklevel=3,
                )
                break

            for home in child_homes:
                property_id = home.get("property_id") if isinstance(home, dict) else getattr(home, "property_id", None)
                if property_id in seen:
                    continue
                seen.add(property_id)
                merged.append(home)

        if self.sort_by:
            merged = self._apply_sort(merged)

        return merged[: self.limit]

    def _fetch_remaining_pages(
        self, homes: list | Future, search_variables: dict, search_type: str, total: int
    ) -> list:
        """Fetch pages after the first. If a page fails (403, 429, network), keep what was
        already fetched, warn, and stop instead of losing the whole search.

        `homes` is the first page, or a Future of it while its extra details are still
        being fetched (so they overlap with the later pages)."""
        offsets = list(range(
            self.offset + self.DEFAULT_PAGE_SIZE,
            min(total, self.offset + self.limit),
            self.DEFAULT_PAGE_SIZE,
        ))
        if not offsets:
            return homes.result() if isinstance(homes, Future) else homes

        failures: list[tuple[int, Exception]] = []

        def fetch(page_offset: int):
            return self.general_search(
                variables=search_variables | {"offset": page_offset},
                search_type=search_type,
            )

        if self.parallel:
            pages: dict[int, list] = {}
            with ThreadPoolExecutor(max_workers=self.MAX_PAGE_WORKERS) as executor:
                futures = {executor.submit(fetch, page_offset): page_offset for page_offset in offsets}
                for future in as_completed(futures):
                    page_offset = futures[future]
                    if future.cancelled():
                        continue
                    try:
                        pages[page_offset] = future.result()["properties"]
                    except RECOVERABLE_ERRORS as exc:
                        failures.append((page_offset, exc))
                        #: stop hammering the API: drop pages that haven't started yet
                        for other in futures:
                            other.cancel()

            if isinstance(homes, Future):
                homes = homes.result()
            for page_offset in sorted(pages):  #: preserve API sort order
                homes.extend(pages[page_offset])
            skipped = len(offsets) - len(pages) - len(failures)
        else:
            skipped = 0
            for index, page_offset in enumerate(offsets):
                # Check if we should continue based on time-based filters
                if not self._should_fetch_more_pages(homes):
                    break
                try:
                    homes.extend(fetch(page_offset)["properties"])
                except RECOVERABLE_ERRORS as exc:
                    failures.append((page_offset, exc))
                    skipped = len(offsets) - index - 1
                    break

        if failures:
            first_error = failures[0][1]
            missing_pages = len(failures) + skipped
            warnings.warn(
                f"HomeHarvest stopped early: {missing_pages} of {len(offsets) + 1} result pages could not be "
                f"fetched ({type(first_error).__name__}: {first_error}). Returning the {len(homes)} properties "
                f"fetched so far. Try again later, use a proxy, or use parallel=False.",
                HomeHarvestWarning,
                stacklevel=4,
            )
        return homes

    def _search_location(self, location_info: dict):
        if (
            self.listing_type == ListingType.PENDING
            and self._has_date_filter()
            and not self._defer_extra_property_data
        ):
            return self._search_pending_with_dates(location_info)

        location_type = location_info["area_type"]

        search_variables = {
            "offset": self.offset,
        }

        search_type = "comps" if location_type == "address" else "area"

        if location_type == "address":  #: general search, comps (radius)
            if not location_info.get("centroid"):
                return []

            centroid = location_info["centroid"]
            coordinates = [centroid["lon"], centroid["lat"]]  # GeoJSON order: [lon, lat]
            search_variables |= {
                "coordinates": coordinates,
                "radius": "{}mi".format(self.radius),
            }

        else:  #: general search (city, county, postal_code, etc.)
            search_variables |= {
                "search_location": {"location": location_info.get("text")},
            }

        if self.foreclosure:
            search_variables["foreclosure"] = self.foreclosure

        #: with extra details, fetch the first page's details while the later pages download
        overlap = self.parallel and self.extra_property_data and not self._defer_extra_property_data

        result = self.general_search(search_variables, search_type=search_type, finish=not overlap)
        total = result["total"]
        homes = result["properties"]
        more_pages = self.offset + self.DEFAULT_PAGE_SIZE < min(total, self.offset + self.limit)

        # Fetch remaining pages (parallel or sequential), keeping partial results on errors
        if overlap and more_pages:
            with ThreadPoolExecutor(max_workers=1) as first_page_pool:
                first_page = first_page_pool.submit(self._finish_page, homes)
                homes = self._fetch_remaining_pages(first_page, search_variables, search_type, total)
        elif overlap:
            homes = self._finish_page(homes)
        elif more_pages:
            homes = self._fetch_remaining_pages(homes, search_variables, search_type, total)

        # Apply client-side hour-based filtering if needed
        # (API only supports day-level filtering, so we post-filter for hour precision)
        has_hour_precision = (self.date_from_precision == "hour" or self.date_to_precision == "hour")
        if self.past_hours or has_hour_precision:
            homes = self._apply_hour_based_date_filter(homes)
        # Apply client-side date filtering for PENDING properties
        # (server-side filters are broken in the API)
        elif self.listing_type == ListingType.PENDING and (self.last_x_days or self.date_from):
            homes = self._apply_pending_date_filter(homes)

        # Apply client-side filtering by last_update_date if specified
        if self.updated_since or self.updated_in_past_hours:
            homes = self._apply_last_update_date_filter(homes)

        # Apply client-side sort to ensure results are properly ordered
        # This is necessary after filtering and to guarantee sort order across page boundaries
        if self.sort_by:
            homes = self._apply_sort(homes)

        # Apply raw data filters (exclude_pending and mls_only) for raw return type
        # These filters are normally applied in process_property() but are bypassed for raw data
        if self.return_type == ReturnType.raw or self._defer_extra_property_data:
            homes = self._apply_raw_data_filters(homes)

        return homes

    def _apply_hour_based_date_filter(self, homes):
        """Apply client-side hour-based date filtering for all listing types.

        This is used when past_hours or date_from/date_to have hour precision,
        since the API only supports day-level filtering.
        """
        if not homes:
            return homes

        from datetime import datetime, timedelta

        # Determine date range with hour precision
        date_range = None

        if self.past_hours:
            cutoff_datetime = datetime.now() - timedelta(hours=self.past_hours)
            date_range = {'type': 'since', 'date': cutoff_datetime}
        elif self.date_from or self.date_to:
            try:
                from_datetime = None
                to_datetime = None

                if self.date_from:
                    from_datetime_str = self.date_from.replace('Z', '+00:00') if self.date_from.endswith('Z') else self.date_from
                    from_datetime = datetime.fromisoformat(from_datetime_str).replace(tzinfo=None)

                if self.date_to:
                    to_datetime_str = self.date_to.replace('Z', '+00:00') if self.date_to.endswith('Z') else self.date_to
                    to_datetime = datetime.fromisoformat(to_datetime_str).replace(tzinfo=None)

                if from_datetime and to_datetime:
                    date_range = {'type': 'range', 'from_date': from_datetime, 'to_date': to_datetime}
                elif from_datetime:
                    date_range = {'type': 'since', 'date': from_datetime}
                elif to_datetime:
                    date_range = {'type': 'until', 'date': to_datetime}
            except (ValueError, AttributeError):
                return homes  # If parsing fails, return unfiltered

        if not date_range:
            return homes

        # Determine which date field to use based on listing type
        date_field_name = self._get_date_field_for_listing_type()

        filtered_homes = []

        for home in homes:
            # Extract the appropriate date for this property
            property_date = self._extract_date_from_home(home, date_field_name)

            # Handle properties without dates
            if property_date is None:
                # For PENDING, include contingent properties without pending_date
                if self.listing_type == ListingType.PENDING and self._is_contingent(home):
                    filtered_homes.append(home)
                continue

            # Check if property date falls within the specified range
            if self._is_datetime_in_range(property_date, date_range):
                filtered_homes.append(home)

        return filtered_homes

    def _get_date_field_for_listing_type(self):
        """Get the appropriate date field name for the current listing type."""
        if self.listing_type == ListingType.SOLD:
            return 'last_sold_date'
        elif self.listing_type == ListingType.PENDING:
            return 'pending_date'
        else:  # FOR_SALE or FOR_RENT
            return 'list_date'

    def _extract_date_from_home(self, home, date_field_name):
        """Extract a date field from a home (handles both dict and Property object).

        Falls back to last_status_change_date if the primary date field is not available,
        providing more precise filtering for PENDING/SOLD properties.
        """
        if isinstance(home, dict):
            date_value = home.get(date_field_name)
        else:
            date_value = getattr(home, date_field_name, None)

        if date_value:
            return self._parse_date_value(date_value)

        # Fallback to last_status_change_date if primary date field is missing
        # This is useful for PENDING/SOLD properties where the specific date might be unavailable
        if isinstance(home, dict):
            fallback_date = home.get('last_status_change_date')
        else:
            fallback_date = getattr(home, 'last_status_change_date', None)

        if fallback_date:
            return self._parse_date_value(fallback_date)

        return None

    def _is_datetime_in_range(self, date_obj, date_range):
        """Check if a datetime object falls within the specified date range (with hour precision)."""
        if date_range['type'] == 'since':
            return date_obj >= date_range['date']
        elif date_range['type'] == 'until':
            return date_obj <= date_range['date']
        elif date_range['type'] == 'range':
            return date_range['from_date'] <= date_obj <= date_range['to_date']
        return False

    def _apply_pending_date_filter(self, homes):
        """Apply client-side date filtering for PENDING properties based on pending_date field.
        For contingent properties without pending_date, tries fallback date fields."""
        if not homes:
            return homes
            
        from datetime import datetime, timedelta
        
        # Determine date range for filtering
        date_range = self._get_date_range()
        if not date_range:
            return homes
            
        filtered_homes = []
        
        for home in homes:
            # pending_date, falling back to last_status_change_date (when a contingent
            # listing without a pending_date went contingent)
            property_date = self._extract_date_from_home(home, 'pending_date')

            # Handle properties without any date (include contingent properties)
            if property_date is None:
                if self._is_contingent(home):
                    filtered_homes.append(home)  # Include contingent without date filter
                continue
            
            # Check if property date falls within the specified range
            if self._is_date_in_range(property_date, date_range):
                filtered_homes.append(home)
                
        return filtered_homes
    
    def _get_pending_date(self, home):
        """Extract pending_date from a home property (handles both dict and Property object)."""
        if isinstance(home, dict):
            return home.get('pending_date')
        else:
            # Assume it's a Property object
            return getattr(home, 'pending_date', None)
    
    
    def _is_contingent(self, home):
        """Check if a property is contingent."""
        if isinstance(home, dict):
            flags = home.get('flags', {})
            return flags.get('is_contingent', False)
        else:
            # Property object - check flags attribute
            if hasattr(home, 'flags') and home.flags:
                return getattr(home.flags, 'is_contingent', False)
            return False

    def _apply_last_update_date_filter(self, homes):
        """Apply client-side filtering by last_update_date.

        This is used when updated_since or updated_in_past_hours are specified.
        Filters properties based on when they were last updated.
        """
        if not homes:
            return homes

        from datetime import datetime, timedelta, timezone

        # Determine date range for last_update_date filtering
        date_range = None

        if self.updated_in_past_hours:
            # Use UTC now, strip timezone to match naive property dates
            cutoff_datetime = (datetime.now(timezone.utc) - timedelta(hours=self.updated_in_past_hours)).replace(tzinfo=None)
            date_range = {'type': 'since', 'date': cutoff_datetime}
        elif self.updated_since:
            try:
                since_datetime_str = self.updated_since.replace('Z', '+00:00') if self.updated_since.endswith('Z') else self.updated_since
                since_datetime = datetime.fromisoformat(since_datetime_str).replace(tzinfo=None)
                date_range = {'type': 'since', 'date': since_datetime}
            except (ValueError, AttributeError):
                return homes  # If parsing fails, return unfiltered

        if not date_range:
            return homes

        filtered_homes = []

        for home in homes:
            # Extract last_update_date from the property
            property_date = self._extract_date_from_home(home, 'last_update_date')

            # Skip properties without last_update_date
            if property_date is None:
                continue

            # Check if property date falls within the specified range
            if self._is_datetime_in_range(property_date, date_range):
                filtered_homes.append(home)

        return filtered_homes

    def _get_date_range(self):
        """Get the date range for filtering based on instance parameters."""
        from datetime import datetime, timedelta, timezone

        if self.last_x_days:
            # Use UTC now, strip timezone to match naive property dates
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=self.last_x_days)).replace(tzinfo=None)
            return {'type': 'since', 'date': cutoff_date}
        elif self.date_from and self.date_to:
            try:
                # Parse and strip timezone to match naive property dates
                from_date_str = self.date_from.replace('Z', '+00:00') if self.date_from.endswith('Z') else self.date_from
                to_date_str = self.date_to.replace('Z', '+00:00') if self.date_to.endswith('Z') else self.date_to
                from_date = datetime.fromisoformat(from_date_str).replace(tzinfo=None)
                to_date = datetime.fromisoformat(to_date_str).replace(tzinfo=None)
                return {'type': 'range', 'from_date': from_date, 'to_date': to_date}
            except ValueError:
                return None
        return None
    
    def _extract_property_date_for_filtering(self, home):
        """Extract pending_date from a property for filtering.
        
        Returns parsed datetime object or None.
        """
        date_value = self._get_pending_date(home)
        if date_value:
            return self._parse_date_value(date_value)
        return None
    
    def _parse_date_value(self, date_value):
        """Parse a date value (string or datetime) into a timezone-naive datetime object."""
        from datetime import datetime
        
        if isinstance(date_value, datetime):
            return date_value.replace(tzinfo=None)
        
        if not isinstance(date_value, str):
            return None
            
        try:
            # Handle timezone indicators
            if date_value.endswith('Z'):
                date_value = date_value[:-1] + '+00:00'
            elif '.' in date_value and date_value.endswith('Z'):
                date_value = date_value.replace('Z', '+00:00')
            
            # Try ISO format first
            try:
                parsed_date = datetime.fromisoformat(date_value)
                return parsed_date.replace(tzinfo=None)
            except ValueError:
                # Try simple datetime format: '2025-08-29 00:00:00'
                return datetime.strptime(date_value, '%Y-%m-%d %H:%M:%S')
                
        except (ValueError, AttributeError):
            return None
    
    def _is_date_in_range(self, date_obj, date_range):
        """Check if a datetime object falls within the specified date range."""
        if date_range['type'] == 'since':
            return date_obj >= date_range['date']
        elif date_range['type'] == 'range':
            return date_range['from_date'] <= date_obj <= date_range['to_date']
        return False

    def _should_fetch_more_pages(self, first_page):
        """Determine if we should continue pagination based on first page results.

        This optimization prevents unnecessary API calls when using time-based filters
        with date sorting. If the last property on page 1 is already outside the time
        window, all future pages will also be outside (due to sort order).

        Args:
            first_page: List of properties from the first page

        Returns:
            bool: True if we should continue pagination, False to stop early
        """
        from datetime import datetime, timedelta, timezone

        # Check for last_update_date filters
        if (self.updated_since or self.updated_in_past_hours) and self.sort_by == "last_update_date":
            if not first_page:
                return False

            last_property = first_page[-1]
            last_date = self._extract_date_from_home(last_property, 'last_update_date')

            if not last_date:
                return True

            # Build date range for last_update_date filter
            if self.updated_since:
                try:
                    cutoff_datetime = datetime.fromisoformat(self.updated_since.replace('Z', '+00:00') if self.updated_since.endswith('Z') else self.updated_since)
                    # Strip timezone to match naive datetimes from _parse_date_value
                    cutoff_datetime = cutoff_datetime.replace(tzinfo=None)
                    date_range = {'type': 'since', 'date': cutoff_datetime}
                except ValueError:
                    return True
            elif self.updated_in_past_hours:
                # Use UTC now, strip timezone to match naive property dates
                cutoff_datetime = (datetime.now(timezone.utc) - timedelta(hours=self.updated_in_past_hours)).replace(tzinfo=None)
                date_range = {'type': 'since', 'date': cutoff_datetime}
            else:
                return True

            return self._is_datetime_in_range(last_date, date_range)

        # No optimization applicable, continue pagination
        return True

    def _apply_sort(self, homes):
        """Apply client-side sorting to ensure results are properly ordered.

        This is necessary because:
        1. Multi-page results need to be re-sorted after concatenation
        2. Filtering operations may disrupt the original sort order

        Args:
            homes: List of properties (either dicts or Property objects)

        Returns:
            Sorted list of properties
        """
        if not homes or not self.sort_by:
            return homes

        def get_sort_key(home):
            """Extract the sort field value from a home (handles both dict and Property object)."""
            from datetime import datetime

            if isinstance(home, dict):
                value = home.get(self.sort_by)
            else:
                # Property object
                value = getattr(home, self.sort_by, None)

            # Handle None values - push them to the end
            if value is None:
                # Use a sentinel value that sorts to the end
                return (1, 0) if self.sort_direction == "desc" else (1, float('inf'))

            # For datetime fields, convert string to datetime for proper sorting
            if self.sort_by in ['list_date', 'sold_date', 'pending_date', 'last_update_date']:
                if isinstance(value, str):
                    try:
                        # Handle timezone indicators
                        date_value = value
                        if date_value.endswith('Z'):
                            date_value = date_value[:-1] + '+00:00'
                        parsed_date = datetime.fromisoformat(date_value)
                        # Normalize to timezone-naive for consistent comparison
                        return 0, parsed_date.replace(tzinfo=None)
                    except (ValueError, AttributeError):
                        # If parsing fails, treat as None
                        return (1, 0) if self.sort_direction == "desc" else (1, float('inf'))
                # Handle datetime objects directly (normalize timezone)
                if isinstance(value, datetime):
                    return 0, value.replace(tzinfo=None)
                return 0, value

            # For numeric fields, ensure we can compare
            return 0, value

        # Sort the homes
        reverse = (self.sort_direction == "desc")
        sorted_homes = sorted(homes, key=get_sort_key, reverse=reverse)

        return sorted_homes

    def _apply_raw_data_filters(self, homes):
        """Apply exclude_pending and mls_only filters for raw data returns.

        These filters are normally applied in process_property(), but that function
        is bypassed when return_type="raw", so we need to apply them here instead.

        Args:
            homes: List of properties (either dicts or Property objects)

        Returns:
            Filtered list of properties
        """
        if not homes:
            return homes

        # Only filter raw data (dict objects)
        # Property objects have already been filtered in process_property()
        if homes and not isinstance(homes[0], dict):
            return homes

        filtered_homes = []

        for home in homes:
            # Apply exclude_pending filter
            if self.exclude_pending and self.listing_type != ListingType.PENDING:
                flags = home.get('flags', {})
                is_pending = flags.get('is_pending', False)
                is_contingent = flags.get('is_contingent', False)

                if is_pending or is_contingent:
                    continue  # Skip this property

            # Apply mls_only filter
            if self.mls_only:
                source = home.get('source', {})
                if not source or not source.get('id'):
                    continue  # Skip this property

            filtered_homes.append(home)

        return filtered_homes


    def _fetch_detail_batch(self, property_ids: list[str]) -> dict:
        """One request for up to DETAIL_BATCH_SIZE homes, using a shared fragment to keep the query small."""
        aliases = " ".join(
            f"home_{property_id}: home(property_id: {property_id}) {{ ...HomeExtraData }}"
            for property_id in property_ids
        )
        query = f"query GetHomeDetails {{ {aliases} }} {EXTRA_PROPERTY_FRAGMENT}"

        data = self._graphql_post(query, {}, "GetHomeDetails")

        #: transient "Required parameter is missing" errors: try once more
        if not data.get("data") and any(
            "Required parameter is missing" in (error.get("message") or "") for error in data.get("errors") or []
        ):
            self._sleep(self._backoff_seconds(0))
            data = self._graphql_post(query, {}, "GetHomeDetails")

        return data

    def get_bulk_prop_details(self, property_ids: list[str]) -> dict:
        """
        Fetch extra property details (schools, tax history, buyer's agent, price history, ...)
        in batches of DETAIL_BATCH_SIZE, up to DETAIL_WORKERS at a time when parallel=True
        (one at a time otherwise). Returns a map of property_id to its details.

        Problems are reported with a HomeHarvestWarning instead of silently returning nothing.
        """
        if not self.extra_property_data or not property_ids:
            return {}

        property_ids = list(dict.fromkeys(property_ids))  #: dedupe, keep order
        batches = [
            property_ids[start:start + self.DETAIL_BATCH_SIZE]
            for start in range(0, len(property_ids), self.DETAIL_BATCH_SIZE)
        ]
        blocked = threading.Event()

        def fetch(batch: list[str]):
            if blocked.is_set():
                return "skipped", None
            try:
                return "ok", self._fetch_detail_batch(batch)
            except (AuthenticationError, RateLimitError) as exc:
                blocked.set()  #: blocked: stop asking for more details, keep the search results
                return "blocked", exc
            except RECOVERABLE_ERRORS as exc:
                return "error", exc

        #: the first batch goes alone, so a 403 / 429 costs one request, not a burst
        outcomes = [fetch(batches[0])]
        rest = batches[1:]
        if rest and not blocked.is_set():
            if self.parallel and len(rest) > 1:
                with ThreadPoolExecutor(max_workers=min(self.DETAIL_WORKERS, len(rest))) as executor:
                    outcomes.extend(executor.map(fetch, rest))
            else:
                for batch in rest:
                    outcomes.append(fetch(batch))
                    if blocked.is_set():
                        break

        details: dict[str, dict] = {}
        error_messages: list[str] = []
        missing = 0
        for index, batch in enumerate(batches):
            status, value = outcomes[index] if index < len(outcomes) else ("skipped", None)
            if status != "ok":
                missing += len(batch)
                if value is not None:
                    error_messages.append(f"{type(value).__name__}: {value}")
                continue

            homes = value.get("data") or {}
            for error in value.get("errors") or []:
                error_messages.append(error.get("message") or str(error))

            found = {key[len("home_"):]: home for key, home in homes.items() if home}
            if not found:
                missing += len(batch)
            details.update(found)

        if error_messages and not missing:
            #: errors on individual fields, but every home came back: not worth a warning
            logger.info("Extra property details returned partial errors: %s", error_messages[:3])
        elif error_messages:
            unique = list(dict.fromkeys(error_messages))
            message = (
                f"Extra property details (schools, tax history, buyer's agent, ...) are missing for {missing} of "
                f"{len(property_ids)} properties. API error: {'; '.join(unique[:3])}"
            )
            logger.warning(message)
            warnings.warn(message, HomeHarvestWarning, stacklevel=2)

        return details
