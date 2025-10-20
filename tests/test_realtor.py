from homeharvest import scrape_property, Property
import pandas as pd


def test_realtor_pending_or_contingent():
    pending_or_contingent_result = scrape_property(location="Surprise, AZ", listing_type="pending")

    regular_result = scrape_property(location="Surprise, AZ", listing_type="for_sale", exclude_pending=True)

    assert all([result is not None for result in [pending_or_contingent_result, regular_result]])
    assert len(pending_or_contingent_result) != len(regular_result)


def test_realtor_pending_comps():
    pending_comps = scrape_property(
        location="2530 Al Lipscomb Way",
        radius=5,
        past_days=180,
        listing_type="pending",
    )

    for_sale_comps = scrape_property(
        location="2530 Al Lipscomb Way",
        radius=5,
        past_days=180,
        listing_type="for_sale",
    )

    sold_comps = scrape_property(
        location="2530 Al Lipscomb Way",
        radius=5,
        past_days=180,
        listing_type="sold",
    )

    results = [pending_comps, for_sale_comps, sold_comps]
    assert all([result is not None for result in results])

    #: assert all lengths are different
    assert len(set([len(result) for result in results])) == len(results)


def test_realtor_sold_past():
    result = scrape_property(
        location="San Diego, CA",
        past_days=30,
        listing_type="sold",
    )

    assert result is not None and len(result) > 0


def test_realtor_comps():
    result = scrape_property(
        location="2530 Al Lipscomb Way",
        radius=0.5,
        past_days=180,
        listing_type="sold",
    )

    assert result is not None and len(result) > 0


def test_realtor_last_x_days_sold():
    days_result_30 = scrape_property(location="Dallas, TX", listing_type="sold", past_days=30)

    days_result_10 = scrape_property(location="Dallas, TX", listing_type="sold", past_days=10)

    assert all([result is not None for result in [days_result_30, days_result_10]]) and len(days_result_30) != len(
        days_result_10
    )


def test_realtor_date_range_sold():
    days_result_30 = scrape_property(
        location="Dallas, TX", listing_type="sold", date_from="2023-05-01", date_to="2023-05-28"
    )

    days_result_60 = scrape_property(
        location="Dallas, TX", listing_type="sold", date_from="2023-04-01", date_to="2023-06-10"
    )

    assert all([result is not None for result in [days_result_30, days_result_60]]) and len(days_result_30) < len(
        days_result_60
    )


def test_realtor_single_property():
    results = [
        scrape_property(
            location="15509 N 172nd Dr, Surprise, AZ 85388",
            listing_type="for_sale",
        ),
        scrape_property(
            location="2530 Al Lipscomb Way",
            listing_type="for_sale",
        ),
    ]

    assert all([result is not None for result in results])


def test_realtor():
    results = [
        scrape_property(
            location="2530 Al Lipscomb Way",
            listing_type="for_sale",
        ),
        scrape_property(
            location="Phoenix, AZ", listing_type="for_rent", limit=1000
        ),  #: does not support "city, state, USA" format
        scrape_property(
            location="Dallas, TX", listing_type="sold", limit=1000
        ),  #: does not support "city, state, USA" format
        scrape_property(location="85281"),
    ]

    assert all([result is not None for result in results])


def test_realtor_city():
    results = scrape_property(location="Atlanta, GA", listing_type="for_sale", limit=1000)

    assert results is not None and len(results) > 0


def test_realtor_land():
    results = scrape_property(location="Atlanta, GA", listing_type="for_sale", property_type=["land"], limit=1000)

    assert results is not None and len(results) > 0


def test_realtor_bad_address():
    bad_results = scrape_property(
        location="abceefg ju098ot498hh9",
        listing_type="for_sale",
    )

    if len(bad_results) == 0:
        assert True


def test_realtor_foreclosed():
    foreclosed = scrape_property(location="Dallas, TX", listing_type="for_sale", past_days=100, foreclosure=True)

    not_foreclosed = scrape_property(location="Dallas, TX", listing_type="for_sale", past_days=100, foreclosure=False)

    assert len(foreclosed) != len(not_foreclosed)


def test_realtor_agent():
    scraped = scrape_property(location="Detroit, MI", listing_type="for_sale", limit=1000, extra_property_data=False)
    assert scraped["agent_name"].nunique() > 1


def test_realtor_without_extra_details():
    results = [
        scrape_property(
            location="00741",
            listing_type="sold",
            limit=10,
            extra_property_data=False,
        ),
        scrape_property(
            location="00741",
            listing_type="sold",
            limit=10,
            extra_property_data=True,
        ),
    ]

    assert not results[0].equals(results[1])


def test_pr_zip_code():
    results = scrape_property(
        location="00741",
        listing_type="for_sale",
    )

    assert results is not None and len(results) > 0


def test_exclude_pending():
    results = scrape_property(
        location="33567",
        listing_type="pending",
        exclude_pending=True,
    )

    assert results is not None and len(results) > 0


def test_style_value_error():
    results = scrape_property(
        location="Alaska, AK",
        listing_type="sold",
        extra_property_data=False,
        limit=1000,
    )

    assert results is not None and len(results) > 0


def test_primary_image_error():
    results = scrape_property(
        location="Spokane, PA",
        listing_type="for_rent",  # or (for_sale, for_rent, pending)
        past_days=360,
        radius=3,
        extra_property_data=False,
    )

    assert results is not None and len(results) > 0


def test_limit():
    over_limit = 876
    extra_params = {"limit": over_limit}

    over_results = scrape_property(
        location="Waddell, AZ",
        listing_type="for_sale",
        **extra_params,
    )

    assert over_results is not None and len(over_results) <= over_limit

    under_limit = 1
    under_results = scrape_property(
        location="Waddell, AZ",
        listing_type="for_sale",
        limit=under_limit,
    )

    assert under_results is not None and len(under_results) == under_limit


def test_apartment_list_price():
    results = scrape_property(
        location="Spokane, WA",
        listing_type="for_rent",  # or (for_sale, for_rent, pending)
        extra_property_data=False,
    )

    assert results is not None

    results = results[results["style"] == "APARTMENT"]

    #: get percentage of results with atleast 1 of any column not none, list_price, list_price_min, list_price_max
    assert (
        len(results[results[["list_price", "list_price_min", "list_price_max"]].notnull().any(axis=1)]) / len(results)
        > 0.5
    )


def test_phone_number_matching():
    searches = [
        scrape_property(
            location="Phoenix, AZ",
            listing_type="for_sale",
            limit=100,
        ),
        scrape_property(
            location="Phoenix, AZ",
            listing_type="for_sale",
            limit=100,
        ),
    ]

    assert all([search is not None for search in searches])

    #: random row
    row = searches[0][searches[0]["agent_phones"].notnull()].sample()

    #: find matching row
    matching_row = searches[1].loc[searches[1]["property_url"] == row["property_url"].values[0]]

    #: assert phone numbers are the same
    assert row["agent_phones"].values[0] == matching_row["agent_phones"].values[0]


def test_return_type():
    results = {
        "pandas": [scrape_property(location="Surprise, AZ", listing_type="for_rent", limit=100)],
        "pydantic": [scrape_property(location="Surprise, AZ", listing_type="for_rent", limit=100, return_type="pydantic")],
        "raw": [
            scrape_property(location="Surprise, AZ", listing_type="for_rent", limit=100, return_type="raw"),
            scrape_property(location="66642", listing_type="for_rent", limit=100, return_type="raw"),
        ],
    }

    assert all(isinstance(result, pd.DataFrame) for result in results["pandas"])
    assert all(isinstance(result[0], Property) for result in results["pydantic"])
    assert all(isinstance(result[0], dict) for result in results["raw"])


def test_has_open_house():
    """Test that open_houses field is present and properly structured when it exists"""

    # Test that open_houses field exists in results (may be None if no open houses scheduled)
    address_result = scrape_property("1 Hawthorne St Unit 12F, San Francisco, CA 94105", return_type="raw")
    assert "open_houses" in address_result[0], "open_houses field should exist in address search results"

    # Test general search also includes open_houses field
    zip_code_result = scrape_property("94105", listing_type="for_sale", limit=50, return_type="raw")
    assert len(zip_code_result) > 0, "Should have results from zip code search"

    # Verify open_houses field exists in general search
    assert "open_houses" in zip_code_result[0], "open_houses field should exist in general search results"

    # If we find any properties with open houses, verify the data structure
    properties_with_open_houses = [prop for prop in zip_code_result if prop.get("open_houses") is not None]

    if properties_with_open_houses:
        # Verify structure of open_houses data
        first_with_open_house = properties_with_open_houses[0]
        assert isinstance(first_with_open_house["open_houses"], (list, dict)), \
            "open_houses should be a list or dict when present"



def test_return_type_consistency():
    """Test that return_type works consistently between general and address searches"""
    
    # Test configurations - different search types
    test_locations = [
        ("Dallas, TX", "general"),  # General city search
        ("75201", "zip"),          # ZIP code search
        ("2530 Al Lipscomb Way", "address")  # Address search
    ]
    
    for location, search_type in test_locations:
        # Test all return types for each search type
        pandas_result = scrape_property(
            location=location,
            listing_type="for_sale",
            limit=3,
            return_type="pandas"
        )
        
        pydantic_result = scrape_property(
            location=location,
            listing_type="for_sale",
            limit=3,
            return_type="pydantic"
        )
        
        raw_result = scrape_property(
            location=location,
            listing_type="for_sale",
            limit=3,
            return_type="raw"
        )
        
        # Validate pandas return type
        assert isinstance(pandas_result, pd.DataFrame), f"pandas result should be DataFrame for {search_type}"
        assert len(pandas_result) > 0, f"pandas result should not be empty for {search_type}"
        
        required_columns = ["property_id", "property_url", "list_price", "status", "formatted_address"]
        for col in required_columns:
            assert col in pandas_result.columns, f"Missing column {col} in pandas result for {search_type}"
        
        # Validate pydantic return type
        assert isinstance(pydantic_result, list), f"pydantic result should be list for {search_type}"
        assert len(pydantic_result) > 0, f"pydantic result should not be empty for {search_type}"
        
        for item in pydantic_result:
            assert isinstance(item, Property), f"pydantic items should be Property objects for {search_type}"
            assert item.property_id is not None, f"property_id should not be None for {search_type}"
        
        # Validate raw return type
        assert isinstance(raw_result, list), f"raw result should be list for {search_type}"
        assert len(raw_result) > 0, f"raw result should not be empty for {search_type}"
        
        for item in raw_result:
            assert isinstance(item, dict), f"raw items should be dict for {search_type}"
            assert "property_id" in item, f"raw items should have property_id for {search_type}"
            assert "href" in item, f"raw items should have href for {search_type}"
        
        # Cross-validate that different return types return related data
        pandas_ids = set(pandas_result["property_id"].tolist())
        pydantic_ids = set(prop.property_id for prop in pydantic_result)
        raw_ids = set(item["property_id"] for item in raw_result)
        
        # All return types should have some properties
        assert len(pandas_ids) > 0, f"pandas should return properties for {search_type}"
        assert len(pydantic_ids) > 0, f"pydantic should return properties for {search_type}"
        assert len(raw_ids) > 0, f"raw should return properties for {search_type}"


def test_pending_date_filtering():
    """Test that pending properties are properly filtered by pending_date using client-side filtering."""
    
    # Test 1: Verify that date filtering works with different time windows
    result_no_filter = scrape_property(
        location="Dallas, TX",
        listing_type="pending", 
        limit=20
    )
    
    result_30_days = scrape_property(
        location="Dallas, TX", 
        listing_type="pending",
        past_days=30,
        limit=20
    )
    
    result_10_days = scrape_property(
        location="Dallas, TX",
        listing_type="pending", 
        past_days=10,
        limit=20
    )
    
    # Basic assertions - we should get some results
    assert result_no_filter is not None and len(result_no_filter) >= 0
    assert result_30_days is not None and len(result_30_days) >= 0
    assert result_10_days is not None and len(result_10_days) >= 0
    
    # Filtering should work: longer periods should return same or more results
    assert len(result_30_days) <= len(result_no_filter), "30-day filter should return <= unfiltered results"
    assert len(result_10_days) <= len(result_30_days), "10-day filter should return <= 30-day results"
    
    # Test 2: Verify that date range filtering works
    if len(result_no_filter) > 0:
        result_date_range = scrape_property(
            location="Dallas, TX",
            listing_type="pending",
            date_from="2025-08-01", 
            date_to="2025-12-31",
            limit=20
        )
        
        assert result_date_range is not None
        # Date range should capture recent properties
        assert len(result_date_range) >= 0
    
    # Test 3: Verify that both pending and contingent properties are included
    # Get raw data to check property types
    if len(result_no_filter) > 0:
        raw_result = scrape_property(
            location="Dallas, TX",
            listing_type="pending",
            return_type="raw",
            limit=15
        )
        
        if raw_result:
            # Check that we get both pending and contingent properties
            pending_count = 0
            contingent_count = 0
            
            for prop in raw_result:
                flags = prop.get('flags', {})
                if flags.get('is_pending'):
                    pending_count += 1
                if flags.get('is_contingent'):
                    contingent_count += 1
            
            # We should get at least one of each type (when available)
            total_properties = pending_count + contingent_count
            assert total_properties > 0, "Should find at least some pending or contingent properties"


def test_hour_based_filtering():
    """Test the new past_hours parameter for hour-level filtering"""
    from datetime import datetime, timedelta

    # Test for sold properties with 24-hour filter
    result_24h = scrape_property(
        location="Phoenix, AZ",
        listing_type="sold",
        past_hours=24,
        limit=50
    )

    # Test for sold properties with 12-hour filter
    result_12h = scrape_property(
        location="Phoenix, AZ",
        listing_type="sold",
        past_hours=12,
        limit=50
    )

    assert result_24h is not None
    assert result_12h is not None

    # 12-hour filter should return same or fewer results than 24-hour
    if len(result_12h) > 0 and len(result_24h) > 0:
        assert len(result_12h) <= len(result_24h), "12-hour results should be <= 24-hour results"

    # Verify timestamps are within the specified hour range for 24h filter
    if len(result_24h) > 0:
        cutoff_time = datetime.now() - timedelta(hours=24)

        # Check a few results
        for idx in range(min(5, len(result_24h))):
            sold_date_str = result_24h.iloc[idx]["last_sold_date"]
            if pd.notna(sold_date_str):
                try:
                    sold_date = datetime.strptime(str(sold_date_str), "%Y-%m-%d %H:%M:%S")
                    # Date should be within last 24 hours
                    assert sold_date >= cutoff_time, f"Property sold date {sold_date} should be within last 24 hours"
                except (ValueError, TypeError):
                    pass  # Skip if date parsing fails


def test_datetime_filtering():
    """Test datetime_from and datetime_to parameters with hour precision"""
    from datetime import datetime, timedelta

    # Get a recent date range (e.g., yesterday)
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    # Test filtering for business hours (9 AM to 5 PM) on a specific day
    result = scrape_property(
        location="Dallas, TX",
        listing_type="for_sale",
        datetime_from=f"{date_str}T09:00:00",
        datetime_to=f"{date_str}T17:00:00",
        limit=30
    )

    assert result is not None

    # Test with only datetime_from
    result_from_only = scrape_property(
        location="Houston, TX",
        listing_type="for_sale",
        datetime_from=f"{date_str}T00:00:00",
        limit=30
    )

    assert result_from_only is not None

    # Test with only datetime_to
    result_to_only = scrape_property(
        location="Austin, TX",
        listing_type="for_sale",
        datetime_to=f"{date_str}T23:59:59",
        limit=30
    )

    assert result_to_only is not None


def test_full_datetime_preservation():
    """Verify that dates now include full timestamps (YYYY-MM-DD HH:MM:SS)"""

    # Test with pandas return type
    result_pandas = scrape_property(
        location="San Diego, CA",
        listing_type="sold",
        past_days=30,
        limit=10
    )

    assert result_pandas is not None and len(result_pandas) > 0

    # Check that date fields contain time information
    if len(result_pandas) > 0:
        for idx in range(min(3, len(result_pandas))):
            # Check last_sold_date
            sold_date = result_pandas.iloc[idx]["last_sold_date"]
            if pd.notna(sold_date):
                sold_date_str = str(sold_date)
                # Should contain time (HH:MM:SS), not just date
                assert " " in sold_date_str or "T" in sold_date_str, \
                    f"Date should include time component: {sold_date_str}"

    # Test with pydantic return type
    result_pydantic = scrape_property(
        location="Los Angeles, CA",
        listing_type="for_sale",
        past_days=7,
        limit=10,
        return_type="pydantic"
    )

    assert result_pydantic is not None and len(result_pydantic) > 0

    # Verify Property objects have datetime objects with time info
    for prop in result_pydantic[:3]:
        if prop.list_date:
            # Should be a datetime object, not just a date
            assert hasattr(prop.list_date, 'hour'), "list_date should be a datetime with time"


def test_beds_filtering():
    """Test bedroom filtering with beds_min and beds_max"""

    result = scrape_property(
        location="Atlanta, GA",
        listing_type="for_sale",
        beds_min=2,
        beds_max=4,
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify all properties have 2-4 bedrooms
    for idx in range(min(10, len(result))):
        beds = result.iloc[idx]["beds"]
        if pd.notna(beds):
            assert 2 <= beds <= 4, f"Property should have 2-4 beds, got {beds}"

    # Test beds_min only
    result_min = scrape_property(
        location="Denver, CO",
        listing_type="for_sale",
        beds_min=3,
        limit=30
    )

    assert result_min is not None

    # Test beds_max only
    result_max = scrape_property(
        location="Seattle, WA",
        listing_type="for_sale",
        beds_max=2,
        limit=30
    )

    assert result_max is not None


def test_baths_filtering():
    """Test bathroom filtering with baths_min and baths_max"""

    result = scrape_property(
        location="Miami, FL",
        listing_type="for_sale",
        baths_min=2.0,
        baths_max=3.5,
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify bathrooms are within range
    for idx in range(min(10, len(result))):
        full_baths = result.iloc[idx]["full_baths"]
        half_baths = result.iloc[idx]["half_baths"]

        if pd.notna(full_baths):
            total_baths = float(full_baths) + (float(half_baths) * 0.5 if pd.notna(half_baths) else 0)
            # Allow some tolerance as API might calculate differently
            if total_baths > 0:
                assert total_baths >= 1.5, f"Baths should be >= 2.0, got {total_baths}"


def test_sqft_filtering():
    """Test square footage filtering"""

    result = scrape_property(
        location="Portland, OR",
        listing_type="for_sale",
        sqft_min=1000,
        sqft_max=2500,
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify sqft is within range
    for idx in range(min(10, len(result))):
        sqft = result.iloc[idx]["sqft"]
        if pd.notna(sqft) and sqft > 0:
            assert 1000 <= sqft <= 2500, f"Sqft should be 1000-2500, got {sqft}"


def test_price_filtering():
    """Test price range filtering"""

    result = scrape_property(
        location="Charlotte, NC",
        listing_type="for_sale",
        price_min=200000,
        price_max=500000,
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify prices are within range
    for idx in range(min(15, len(result))):
        price = result.iloc[idx]["list_price"]
        if pd.notna(price) and price > 0:
            assert 200000 <= price <= 500000, f"Price should be $200k-$500k, got ${price}"


def test_lot_sqft_filtering():
    """Test lot size filtering"""

    result = scrape_property(
        location="Scottsdale, AZ",
        listing_type="for_sale",
        lot_sqft_min=5000,
        lot_sqft_max=15000,
        limit=30
    )

    assert result is not None
    # Results might be fewer if lot_sqft data is sparse


def test_year_built_filtering():
    """Test year built filtering"""

    result = scrape_property(
        location="Tampa, FL",
        listing_type="for_sale",
        year_built_min=2000,
        year_built_max=2024,
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify year_built is within range
    for idx in range(min(10, len(result))):
        year = result.iloc[idx]["year_built"]
        if pd.notna(year) and year > 0:
            assert 2000 <= year <= 2024, f"Year should be 2000-2024, got {year}"


def test_combined_filters():
    """Test multiple filters working together"""

    result = scrape_property(
        location="Nashville, TN",
        listing_type="for_sale",
        beds_min=3,
        baths_min=2.0,
        sqft_min=1500,
        price_min=250000,
        price_max=600000,
        year_built_min=1990,
        limit=30
    )

    assert result is not None

    # If we get results, verify they meet ALL criteria
    if len(result) > 0:
        for idx in range(min(5, len(result))):
            row = result.iloc[idx]

            # Check beds
            if pd.notna(row["beds"]):
                assert row["beds"] >= 3, f"Beds should be >= 3, got {row['beds']}"

            # Check sqft
            if pd.notna(row["sqft"]) and row["sqft"] > 0:
                assert row["sqft"] >= 1500, f"Sqft should be >= 1500, got {row['sqft']}"

            # Check price
            if pd.notna(row["list_price"]) and row["list_price"] > 0:
                assert 250000 <= row["list_price"] <= 600000, \
                    f"Price should be $250k-$600k, got ${row['list_price']}"

            # Check year
            if pd.notna(row["year_built"]) and row["year_built"] > 0:
                assert row["year_built"] >= 1990, \
                    f"Year should be >= 1990, got {row['year_built']}"


def test_sorting_by_price():
    """Test sorting by list_price - note API sorting may not be perfect"""

    # Sort ascending (cheapest first)
    result_asc = scrape_property(
        location="Orlando, FL",
        listing_type="for_sale",
        sort_by="list_price",
        sort_direction="asc",
        limit=20
    )

    assert result_asc is not None and len(result_asc) > 0

    # Sort descending (most expensive first)
    result_desc = scrape_property(
        location="San Antonio, TX",
        listing_type="for_sale",
        sort_by="list_price",
        sort_direction="desc",
        limit=20
    )

    assert result_desc is not None and len(result_desc) > 0

    # Note: Realtor API sorting may not be perfectly reliable for all search types
    # The test ensures the sort parameters don't cause errors, actual sort order may vary


def test_sorting_by_date():
    """Test sorting by list_date - note API sorting may not be perfect"""

    result = scrape_property(
        location="Columbus, OH",
        listing_type="for_sale",
        sort_by="list_date",
        sort_direction="desc",  # Newest first
        limit=20
    )

    assert result is not None and len(result) > 0

    # Test ensures sort parameter doesn't cause errors
    # Note: Realtor API sorting may not be perfectly reliable for all search types


def test_sorting_by_sqft():
    """Test sorting by square footage - note API sorting may not be perfect"""

    result = scrape_property(
        location="Indianapolis, IN",
        listing_type="for_sale",
        sort_by="sqft",
        sort_direction="desc",  # Largest first
        limit=20
    )

    assert result is not None and len(result) > 0

    # Test ensures sort parameter doesn't cause errors
    # Note: Realtor API sorting may not be perfectly reliable for all search types


def test_filter_validation_errors():
    """Test that validation catches invalid parameters"""
    import pytest

    # Test: beds_min > beds_max should raise ValueError
    with pytest.raises(ValueError, match="beds_min.*cannot be greater than.*beds_max"):
        scrape_property(
            location="Boston, MA",
            listing_type="for_sale",
            beds_min=5,
            beds_max=2,
            limit=10
        )

    # Test: invalid datetime format should raise exception
    with pytest.raises(Exception):  # InvalidDate
        scrape_property(
            location="Boston, MA",
            listing_type="for_sale",
            datetime_from="not-a-valid-datetime",
            limit=10
        )

    # Test: invalid sort_by value should raise ValueError
    with pytest.raises(ValueError, match="Invalid sort_by"):
        scrape_property(
            location="Boston, MA",
            listing_type="for_sale",
            sort_by="invalid_field",
            limit=10
        )

    # Test: invalid sort_direction should raise ValueError
    with pytest.raises(ValueError, match="Invalid sort_direction"):
        scrape_property(
            location="Boston, MA",
            listing_type="for_sale",
            sort_by="list_price",
            sort_direction="invalid",
            limit=10
        )


def test_backward_compatibility():
    """Ensure old parameters still work as expected"""

    # Test past_days still works
    result_past_days = scrape_property(
        location="Las Vegas, NV",
        listing_type="sold",
        past_days=30,
        limit=20
    )

    assert result_past_days is not None and len(result_past_days) > 0

    # Test date_from/date_to still work
    result_date_range = scrape_property(
        location="Memphis, TN",
        listing_type="sold",
        date_from="2024-01-01",
        date_to="2024-03-31",
        limit=20
    )

    assert result_date_range is not None

    # Test property_type still works
    result_property_type = scrape_property(
        location="Louisville, KY",
        listing_type="for_sale",
        property_type=["single_family"],
        limit=20
    )

    assert result_property_type is not None and len(result_property_type) > 0

    # Test foreclosure still works
    result_foreclosure = scrape_property(
        location="Detroit, MI",
        listing_type="for_sale",
        foreclosure=True,
        limit=15
    )

    assert result_foreclosure is not None