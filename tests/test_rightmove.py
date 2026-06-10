from homeharvest.core.scrapers import ScraperInput
from homeharvest.core.scrapers.models import ListingType, ReturnType, SiteName
from homeharvest.core.scrapers.rightmove import (
    RightmoveScraper,
    parse_rightmove_search_page,
    rightmove_listing_to_property,
)


def test_rightmove_builds_for_sale_search_url():
    scraper = RightmoveScraper(
        ScraperInput(
            location="London",
            site=SiteName.RIGHTMOVE,
            listing_type=ListingType.FOR_SALE,
            return_type=ReturnType.raw,
            beds_min=2,
            price_max=750000,
        )
    )

    url = scraper._build_search_url()

    assert url.startswith("https://www.rightmove.co.uk/property-for-sale/find.html?")
    assert "locationIdentifier=REGION%5E87490" in url
    assert "minBedrooms=2" in url
    assert "maxPrice=750000" in url


def test_rightmove_builds_rental_search_url():
    scraper = RightmoveScraper(
        ScraperInput(
            location="Manchester",
            site=SiteName.RIGHTMOVE,
            listing_type=ListingType.FOR_RENT,
            return_type=ReturnType.raw,
        )
    )

    assert scraper._build_search_url().startswith("https://www.rightmove.co.uk/property-to-rent/find.html?")


def test_parse_rightmove_json_model():
    html = """
    <html>
      <script>
        window.jsonModel = {
          "properties": [
            {
              "id": 123,
              "displayAddress": "1 Test Street, London",
              "propertyUrl": "/properties/123#/?channel=RES_BUY",
              "price": {"amount": 625000, "currencyCode": "GBP"},
              "bedrooms": 2,
              "bathrooms": 1,
              "summary": "Two-bedroom flat.",
              "location": {"latitude": 51.5, "longitude": -0.12},
              "propertyImages": {"images": [{"srcUrl": "https://media.rightmove.co.uk/test.jpg"}]}
            }
          ]
        }
      </script>
    </html>
    """

    listings = parse_rightmove_search_page(html)

    assert len(listings) == 1
    assert listings[0]["id"] == 123


def test_rightmove_listing_to_property():
    listing = {
        "id": 123,
        "displayAddress": "1 Test Street, London",
        "propertyUrl": "/properties/123#/?channel=RES_BUY",
        "price": {"displayPrices": [{"displayPrice": "£625,000"}]},
        "bedrooms": "2",
        "bathrooms": "1",
        "displaySize": "800 sq. ft.",
        "propertySubType": "Flat",
        "firstVisibleDate": "2026-06-01T14:51:49Z",
        "summary": "Two-bedroom flat.",
        "location": {"latitude": "51.5", "longitude": "-0.12"},
        "propertyImages": {"images": [{"srcUrl": "https://media.rightmove.co.uk/test.jpg"}]},
    }

    prop = rightmove_listing_to_property(listing, "London", ListingType.FOR_SALE)

    assert prop.source_site == "rightmove"
    assert prop.country == "GB"
    assert prop.currency == "GBP"
    assert prop.list_price == 625000
    assert prop.prc_sqft == 781
    assert prop.address.full_line == "1 Test Street, London"
    assert prop.description.beds == 2
    assert prop.description.sqft == 800
    assert prop.latitude == 51.5


def test_rightmove_accepts_raw_location_identifier():
    scraper = RightmoveScraper(
        ScraperInput(
            location="REGION^87490",
            site=SiteName.RIGHTMOVE,
            listing_type=ListingType.FOR_SALE,
            return_type=ReturnType.raw,
        )
    )

    assert "locationIdentifier=REGION%5E87490" in scraper._build_search_url()
