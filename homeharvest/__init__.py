import warnings
import pandas as pd
from datetime import datetime, timedelta, date
from .core.scrapers import ScraperInput
from .utils import (
    process_result, ordered_properties, validate_input, validate_dates, validate_limit,
    validate_offset, validate_datetime, validate_filters, validate_sort, validate_last_update_filters,
    convert_to_datetime_string, extract_timedelta_hours, extract_timedelta_days, detect_precision_and_convert
)
from .core.scrapers.realtor import RealtorScraper
from .core.scrapers.models import ListingType, SearchPropertyType, ReturnType, Property
from typing import Union, Optional, List

def scrape_property(
    location: str,
    listing_type: str | list[str] | None = None,
    return_type: str = "pandas",
    property_type: Optional[List[str]] = None,
    radius: float = None,
    mls_only: bool = False,
    past_days: int | timedelta = None,
    proxy: str = None,
    date_from: datetime | date | str = None,
    date_to: datetime | date | str = None,
    foreclosure: bool = None,
    extra_property_data: bool = True,
    exclude_pending: bool = False,
    limit: int = 10000,
    offset: int = 0,
    # New date/time filtering parameters
    past_hours: int | timedelta = None,
    # New last_update_date filtering parameters
    updated_since: datetime | str = None,
    updated_in_past_hours: int | timedelta = None,
    # New property filtering parameters
    beds_min: int = None,
    beds_max: int = None,
    baths_min: float = None,
    baths_max: float = None,
    sqft_min: int = None,
    sqft_max: int = None,
    price_min: int = None,
    price_max: int = None,
    lot_sqft_min: int = None,
    lot_sqft_max: int = None,
    year_built_min: int = None,
    year_built_max: int = None,
    # New sorting parameters
    sort_by: str = None,
    sort_direction: str = "desc",
    # Pagination control
    parallel: bool = True,
) -> Union[pd.DataFrame, list[dict], list[Property]]:
    """
    Scrape properties from Realtor.com based on a given location and listing type.

    :param location: Location to search (e.g. "Dallas, TX", "85281", "2530 Al Lipscomb Way")
    :param listing_type: Listing Type - can be a string, list of strings, or None.
        Options: for_sale, for_rent, sold, pending, off_market, new_community, other, ready_to_build
        Examples: "for_sale", ["for_sale", "pending"], None (returns all types)
    :param return_type: Return type (pandas, pydantic, raw)
    :param property_type: Property Type (single_family, multi_family, condos, condo_townhome_rowhome_coop, condo_townhome, townhomes, duplex_triplex, farm, land, mobile)
    :param radius: Get properties within _ (e.g. 1.0) miles. Only applicable for individual addresses.
    :param mls_only: If set, fetches only listings with MLS IDs.
    :param proxy: Proxy to use for scraping
    :param past_days: Get properties sold or listed (dependent on your listing_type) in the last _ days.
        - PENDING: Filters by pending_date. Contingent properties without pending_date are included.
        - SOLD: Filters by sold_date (when property was sold)
        - FOR_SALE/FOR_RENT: Filters by list_date (when property was listed)
    :param date_from, date_to: Get properties sold or listed (dependent on your listing_type) between these dates.
        Accepts multiple formats for flexible precision:
        - Date strings: "2025-01-20" (day-level precision)
        - Datetime strings: "2025-01-20T14:30:00" (hour-level precision)
        - date objects: date(2025, 1, 20) (day-level precision)
        - datetime objects: datetime(2025, 1, 20, 14, 30) (hour-level precision)
        The precision is automatically detected based on the input format.
        Timezone handling: Naive datetimes are treated as local time and automatically converted to UTC.
        Timezone-aware datetimes are converted to UTC. For best results, use timezone-aware datetimes.
    :param foreclosure: If set, fetches only foreclosure listings.
    :param extra_property_data: Increases requests by O(n). If set, this fetches additional property data (e.g. agent, broker, property evaluations etc.)
    :param exclude_pending: If true, this excludes pending or contingent properties from the results, unless listing type is pending.
    :param limit: Limit the number of results returned. Maximum is 10,000.
    :param offset: Starting position for pagination within the 10k limit (offset + limit cannot exceed 10,000). Use with limit to fetch results in chunks (e.g., offset=200, limit=200 fetches results 200-399). Should be a multiple of 200 (page size) for optimal performance. Default is 0. Note: Cannot be used to bypass the 10k API limit - use date ranges (date_from/date_to) to narrow searches and fetch more data.

    New parameters:
    :param past_hours: Get properties in the last _ hours (requires client-side filtering). Accepts int or timedelta.
    :param updated_since: Filter by last_update_date (when property was last updated). Accepts datetime object or ISO 8601 string (client-side filtering).
        Timezone handling: Naive datetimes (like datetime.now()) are treated as local time and automatically converted to UTC.
        Timezone-aware datetimes are converted to UTC. Examples:
        - datetime.now() - uses your local timezone
        - datetime.now(timezone.utc) - uses UTC explicitly
    :param updated_in_past_hours: Filter by properties updated in the last _ hours. Accepts int or timedelta (client-side filtering)
    :param beds_min, beds_max: Filter by number of bedrooms
    :param baths_min, baths_max: Filter by number of bathrooms
    :param sqft_min, sqft_max: Filter by square footage
    :param price_min, price_max: Filter by listing price
    :param lot_sqft_min, lot_sqft_max: Filter by lot size
    :param year_built_min, year_built_max: Filter by year built
    :param sort_by: Sort results by field (list_date, sold_date, list_price, sqft, beds, baths, last_update_date)
    :param sort_direction: Sort direction (asc, desc)
    :param parallel: Controls pagination strategy. True (default) = fetch all pages in parallel for maximum speed.
        False = fetch pages sequentially with early termination checks (useful for rate limiting or narrow time windows).
        Sequential mode will stop paginating as soon as time-based filters indicate no more matches are possible.

    Note: past_days and past_hours also accept timedelta objects for more Pythonic usage.
    """
    validate_input(listing_type)
    validate_limit(limit)
    validate_offset(offset, limit)
    validate_filters(
        beds_min, beds_max, baths_min, baths_max, sqft_min, sqft_max,
        price_min, price_max, lot_sqft_min, lot_sqft_max, year_built_min, year_built_max
    )
    validate_sort(sort_by, sort_direction)

    # Validate new last_update_date filtering parameters
    validate_last_update_filters(
        convert_to_datetime_string(updated_since),
        extract_timedelta_hours(updated_in_past_hours)
    )

    # Convert listing_type to appropriate format
    if listing_type is None:
        converted_listing_type = None
    elif isinstance(listing_type, list):
        converted_listing_type = [ListingType(lt.upper()) for lt in listing_type]
    else:
        converted_listing_type = ListingType(listing_type.upper())

    # Convert date_from/date_to with precision detection
    converted_date_from, date_from_precision = detect_precision_and_convert(date_from)
    converted_date_to, date_to_precision = detect_precision_and_convert(date_to)

    # Validate converted dates
    validate_dates(converted_date_from, converted_date_to)

    # Convert datetime/timedelta objects to appropriate formats
    converted_past_days = extract_timedelta_days(past_days)
    converted_past_hours = extract_timedelta_hours(past_hours)
    converted_updated_since = convert_to_datetime_string(updated_since)
    converted_updated_in_past_hours = extract_timedelta_hours(updated_in_past_hours)

    # Auto-apply optimal sort for time-based filters (unless user specified different sort)
    if (converted_updated_since or converted_updated_in_past_hours) and not sort_by:
        sort_by = "last_update_date"
        if not sort_direction:
            sort_direction = "desc"  # Most recent first

    # Auto-apply optimal sort for PENDING listings with date filters
    # PENDING API filtering is broken, so we rely on client-side filtering
    # Sorting by pending_date ensures efficient pagination with early termination
    elif (converted_listing_type == ListingType.PENDING and
          (converted_past_days or converted_past_hours or converted_date_from) and
          not sort_by):
        sort_by = "pending_date"
        if not sort_direction:
            sort_direction = "desc"  # Most recent first

    scraper_input = ScraperInput(
        location=location,
        listing_type=converted_listing_type,
        return_type=ReturnType(return_type.lower()),
        property_type=[SearchPropertyType[prop.upper()] for prop in property_type] if property_type else None,
        proxy=proxy,
        radius=radius,
        mls_only=mls_only,
        last_x_days=converted_past_days,
        date_from=converted_date_from,
        date_to=converted_date_to,
        date_from_precision=date_from_precision,
        date_to_precision=date_to_precision,
        foreclosure=foreclosure,
        extra_property_data=extra_property_data,
        exclude_pending=exclude_pending,
        limit=limit,
        offset=offset,
        # New date/time filtering
        past_hours=converted_past_hours,
        # New last_update_date filtering
        updated_since=converted_updated_since,
        updated_in_past_hours=converted_updated_in_past_hours,
        # New property filtering
        beds_min=beds_min,
        beds_max=beds_max,
        baths_min=baths_min,
        baths_max=baths_max,
        sqft_min=sqft_min,
        sqft_max=sqft_max,
        price_min=price_min,
        price_max=price_max,
        lot_sqft_min=lot_sqft_min,
        lot_sqft_max=lot_sqft_max,
        year_built_min=year_built_min,
        year_built_max=year_built_max,
        # New sorting
        sort_by=sort_by,
        sort_direction=sort_direction,
        # Pagination control
        parallel=parallel,
    )

    site = RealtorScraper(scraper_input)
    results = site.search()

    if scraper_input.return_type != ReturnType.pandas:
        return results

    properties_dfs = [df for result in results if not (df := process_result(result)).empty]
    if not properties_dfs:
        return pd.DataFrame()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)

        return pd.concat(properties_dfs, ignore_index=True, axis=0)[ordered_properties].replace(
            {"None": pd.NA, None: pd.NA, "": pd.NA}
        )
