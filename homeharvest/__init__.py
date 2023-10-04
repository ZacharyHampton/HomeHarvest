import warnings
import pandas as pd
from .core.scrapers import ScraperInput
from .utils import process_result, ordered_properties, validate_input
from .core.scrapers.realtor import RealtorScraper
from .core.scrapers.models import ListingType
from .exceptions import InvalidListingType, NoResultsFound


def scrape_property(
    location: str,
    listing_type: str = "for_sale",
    radius: float = None,
    mls_only: bool = False,
    last_x_days: int = None,
    proxy: str = None,
) -> pd.DataFrame:
    """
    Scrape properties from Realtor.com based on a given location and listing type.
    """
    validate_input(listing_type)

    scraper_input = ScraperInput(
        location=location,
        listing_type=ListingType[listing_type.upper()],
        proxy=proxy,
        radius=radius,
        mls_only=mls_only,
        last_x_days=last_x_days,
    )

    site = RealtorScraper(scraper_input)
    results = site.search()

    properties_dfs = [process_result(result) for result in results]
    if not properties_dfs:
        raise NoResultsFound("no results found for the query")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        return pd.concat(properties_dfs, ignore_index=True, axis=0)[ordered_properties]
