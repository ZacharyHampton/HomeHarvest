import warnings
import pandas as pd
from .core.scrapers import ScraperInput
from .utils import process_result, ordered_properties, validate_input, validate_dates, validate_limit, validate_datetime, validate_filters, validate_sort
from .core.scrapers.realtor import RealtorScraper
from .core.scrapers.models import ListingType, SearchPropertyType, ReturnType, Property
from typing import Union, Optional, List

def scrape_property(
    location: str,
    listing_type: str = "for_sale",
    return_type: str = "pandas",
    property_type: Optional[List[str]] = None,
    radius: float = None,
    mls_only: bool = False,
    past_days: int = None,
    proxy: str = None,
    date_from: str = None,
    date_to: str = None,
    foreclosure: bool = None,
    extra_property_data: bool = True,
    exclude_pending: bool = False,
    limit: int = 10000,
    # New date/time filtering parameters
    past_hours: int = None,
    datetime_from: str = None,
    datetime_to: str = None,
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
) -> Union[pd.DataFrame, list[dict], list[Property]]:
    """
    Scrape properties from Realtor.com based on a given location and listing type.

    :param location: Location to search (e.g. "Dallas, TX", "85281", "2530 Al Lipscomb Way")
    :param listing_type: Listing Type (for_sale, for_rent, sold, pending)
    :param return_type: Return type (pandas, pydantic, raw)
    :param property_type: Property Type (single_family, multi_family, condos, condo_townhome_rowhome_coop, condo_townhome, townhomes, duplex_triplex, farm, land, mobile)
    :param radius: Get properties within _ (e.g. 1.0) miles. Only applicable for individual addresses.
    :param mls_only: If set, fetches only listings with MLS IDs.
    :param proxy: Proxy to use for scraping
    :param past_days: Get properties sold or listed (dependent on your listing_type) in the last _ days.
        - PENDING: Filters by pending_date. Contingent properties without pending_date are included.
        - SOLD: Filters by sold_date (when property was sold)
        - FOR_SALE/FOR_RENT: Filters by list_date (when property was listed)
    :param date_from, date_to: Get properties sold or listed (dependent on your listing_type) between these dates. format: 2021-01-28
    :param foreclosure: If set, fetches only foreclosure listings.
    :param extra_property_data: Increases requests by O(n). If set, this fetches additional property data (e.g. agent, broker, property evaluations etc.)
    :param exclude_pending: If true, this excludes pending or contingent properties from the results, unless listing type is pending.
    :param limit: Limit the number of results returned. Maximum is 10,000.

    New parameters:
    :param past_hours: Get properties in the last _ hours (requires client-side filtering)
    :param datetime_from, datetime_to: ISO 8601 datetime strings for precise time filtering (e.g. "2025-01-20T14:30:00")
    :param beds_min, beds_max: Filter by number of bedrooms
    :param baths_min, baths_max: Filter by number of bathrooms
    :param sqft_min, sqft_max: Filter by square footage
    :param price_min, price_max: Filter by listing price
    :param lot_sqft_min, lot_sqft_max: Filter by lot size
    :param year_built_min, year_built_max: Filter by year built
    :param sort_by: Sort results by field (list_date, sold_date, list_price, sqft, beds, baths)
    :param sort_direction: Sort direction (asc, desc)
    """
    validate_input(listing_type)
    validate_dates(date_from, date_to)
    validate_limit(limit)
    validate_datetime(datetime_from)
    validate_datetime(datetime_to)
    validate_filters(
        beds_min, beds_max, baths_min, baths_max, sqft_min, sqft_max,
        price_min, price_max, lot_sqft_min, lot_sqft_max, year_built_min, year_built_max
    )
    validate_sort(sort_by, sort_direction)

    scraper_input = ScraperInput(
        location=location,
        listing_type=ListingType(listing_type.upper()),
        return_type=ReturnType(return_type.lower()),
        property_type=[SearchPropertyType[prop.upper()] for prop in property_type] if property_type else None,
        proxy=proxy,
        radius=radius,
        mls_only=mls_only,
        last_x_days=past_days,
        date_from=date_from,
        date_to=date_to,
        foreclosure=foreclosure,
        extra_property_data=extra_property_data,
        exclude_pending=exclude_pending,
        limit=limit,
        # New date/time filtering
        past_hours=past_hours,
        datetime_from=datetime_from,
        datetime_to=datetime_to,
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
