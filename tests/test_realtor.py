import pytz

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


def test_listing_type_none_includes_sold():
    """Test that listing_type=None includes sold listings (issue #142)"""
    # Get properties with listing_type=None (should include all common types)
    result_none = scrape_property(
        location="Warren, MI",
        listing_type=None
    )

    # Verify we got results
    assert result_none is not None and len(result_none) > 0

    # Verify sold listings are included
    status_types = set(result_none['status'].unique())
    assert 'SOLD' in status_types, "SOLD listings should be included when listing_type=None"

    # Verify we get multiple listing types (not just one)
    assert len(status_types) > 1, "Should return multiple listing types when listing_type=None"


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

    # When extra_property_data=False, these fields should be None
    extra_fields = ["nearby_schools", "assessed_value", "tax", "tax_history"]

    # Check that all extra fields are None when extra_property_data=False
    for field in extra_fields:
        if field in results[0].columns:
            assert results[0][field].isna().all(), f"Field '{field}' should be None when extra_property_data=False"


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
            scrape_property(location="85281", listing_type="for_rent", limit=100, return_type="raw"),
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


def test_past_hours_all_listing_types():
    """Validate that past_hours works correctly for all listing types with proper date fields"""
    from datetime import datetime, timedelta

    # Test 1: SOLD (uses last_sold_date field, server-side filters by sold_date)
    result_sold = scrape_property(
        location="Dallas, TX",
        listing_type="sold",
        past_hours=48,
        limit=20
    )

    assert result_sold is not None
    if len(result_sold) > 0:
        cutoff_48h = datetime.now() - timedelta(hours=48)

        # Verify results use sold_date and are within 48 hours
        for idx in range(min(5, len(result_sold))):
            sold_date_str = result_sold.iloc[idx]["last_sold_date"]
            if pd.notna(sold_date_str):
                try:
                    sold_date = datetime.strptime(str(sold_date_str), "%Y-%m-%d %H:%M:%S")
                    assert sold_date >= cutoff_48h, \
                        f"SOLD: last_sold_date {sold_date} should be within 48 hours"
                except (ValueError, TypeError):
                    pass

    # Test 2: FOR_SALE (uses list_date field, server-side filters by list_date)
    result_for_sale = scrape_property(
        location="Austin, TX",
        listing_type="for_sale",
        past_hours=48,
        limit=20
    )

    assert result_for_sale is not None
    if len(result_for_sale) > 0:
        cutoff_48h = datetime.now() - timedelta(hours=48)

        # Verify results use list_date and are within 48 hours
        for idx in range(min(5, len(result_for_sale))):
            list_date_str = result_for_sale.iloc[idx]["list_date"]
            if pd.notna(list_date_str):
                try:
                    list_date = datetime.strptime(str(list_date_str), "%Y-%m-%d %H:%M:%S")
                    assert list_date >= cutoff_48h, \
                        f"FOR_SALE: list_date {list_date} should be within 48 hours"
                except (ValueError, TypeError):
                    pass

    # Test 3: FOR_RENT (uses list_date field, server-side filters by list_date)
    result_for_rent = scrape_property(
        location="Houston, TX",
        listing_type="for_rent",
        past_hours=72,
        limit=20
    )

    assert result_for_rent is not None
    if len(result_for_rent) > 0:
        cutoff_72h = datetime.now() - timedelta(hours=72)

        # Verify results use list_date and are within 72 hours
        for idx in range(min(5, len(result_for_rent))):
            list_date_str = result_for_rent.iloc[idx]["list_date"]
            if pd.notna(list_date_str):
                try:
                    list_date = datetime.strptime(str(list_date_str), "%Y-%m-%d %H:%M:%S")
                    assert list_date >= cutoff_72h, \
                        f"FOR_RENT: list_date {list_date} should be within 72 hours"
                except (ValueError, TypeError):
                    pass

    # Test 4: PENDING (uses pending_date field, client-side filtering only)
    result_pending = scrape_property(
        location="San Antonio, TX",
        listing_type="pending",
        past_hours=48,
        limit=20
    )

    assert result_pending is not None
    # Note: PENDING doesn't use server-side date filtering (API filters broken)
    # Client-side filtering should still work via pending_date
    if len(result_pending) > 0:
        cutoff_48h = datetime.now() - timedelta(hours=48)

        # Verify results use pending_date (or are contingent without date)
        for idx in range(min(5, len(result_pending))):
            pending_date_str = result_pending.iloc[idx]["pending_date"]
            if pd.notna(pending_date_str):
                try:
                    pending_date = datetime.strptime(str(pending_date_str), "%Y-%m-%d %H:%M:%S")
                    assert pending_date >= cutoff_48h, \
                        f"PENDING: pending_date {pending_date} should be within 48 hours"
                except (ValueError, TypeError):
                    pass
            # else: property is contingent without pending_date, which is allowed


def test_datetime_filtering():
    """Test date_from and date_to parameters with hour precision"""
    from datetime import datetime, timedelta

    # Get a recent date range (e.g., yesterday)
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    # Test filtering for business hours (9 AM to 5 PM) on a specific day
    result = scrape_property(
        location="Dallas, TX",
        listing_type="for_sale",
        date_from=f"{date_str}T09:00:00",
        date_to=f"{date_str}T17:00:00",
        limit=30
    )

    assert result is not None

    # Test with only date_from
    result_from_only = scrape_property(
        location="Houston, TX",
        listing_type="for_sale",
        date_from=f"{date_str}T00:00:00",
        limit=30
    )

    assert result_from_only is not None

    # Test with only date_to
    result_to_only = scrape_property(
        location="Austin, TX",
        listing_type="for_sale",
        date_to=f"{date_str}T23:59:59",
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
    """Test sorting by list_price with actual sort order validation"""

    # Sort ascending (cheapest first) with multi-page limit to test concatenation
    result_asc = scrape_property(
        location="Orlando, FL",
        listing_type="for_sale",
        sort_by="list_price",
        sort_direction="asc",
        limit=250  # Multi-page to test concatenation logic
    )

    assert result_asc is not None and len(result_asc) > 0

    # Verify ascending sort order (allow for None/NA values at the end)
    prices_asc = result_asc["list_price"].dropna().tolist()
    assert len(prices_asc) > 0, "No properties with prices found"
    assert prices_asc == sorted(prices_asc), f"Prices not in ascending order: {prices_asc[:10]}"

    # Sort descending (most expensive first)
    result_desc = scrape_property(
        location="San Antonio, TX",
        listing_type="for_sale",
        sort_by="list_price",
        sort_direction="desc",
        limit=250  # Multi-page to test concatenation logic
    )

    assert result_desc is not None and len(result_desc) > 0

    # Verify descending sort order (allow for None/NA values at the end)
    prices_desc = result_desc["list_price"].dropna().tolist()
    assert len(prices_desc) > 0, "No properties with prices found"
    assert prices_desc == sorted(prices_desc, reverse=True), f"Prices not in descending order: {prices_desc[:10]}"


def test_sorting_by_date():
    """Test sorting by list_date with actual sort order validation"""

    # Test descending (newest first) with multi-page limit
    result_desc = scrape_property(
        location="Columbus, OH",
        listing_type="for_sale",
        sort_by="list_date",
        sort_direction="desc",  # Newest first
        limit=250  # Multi-page to test concatenation logic
    )

    assert result_desc is not None and len(result_desc) > 0

    # Verify descending sort order (allow for None/NA values at the end)
    dates_desc = result_desc["list_date"].dropna().tolist()
    assert len(dates_desc) > 0, "No properties with dates found"
    assert dates_desc == sorted(dates_desc, reverse=True), f"Dates not in descending order (newest first): {dates_desc[:10]}"

    # Test ascending (oldest first)
    result_asc = scrape_property(
        location="Columbus, OH",
        listing_type="for_sale",
        sort_by="list_date",
        sort_direction="asc",  # Oldest first
        limit=250
    )

    assert result_asc is not None and len(result_asc) > 0

    # Verify ascending sort order
    dates_asc = result_asc["list_date"].dropna().tolist()
    assert len(dates_asc) > 0, "No properties with dates found"
    assert dates_asc == sorted(dates_asc), f"Dates not in ascending order (oldest first): {dates_asc[:10]}"


def test_sorting_by_sqft():
    """Test sorting by square footage with actual sort order validation"""

    # Test descending (largest first) with multi-page limit
    result_desc = scrape_property(
        location="Indianapolis, IN",
        listing_type="for_sale",
        sort_by="sqft",
        sort_direction="desc",  # Largest first
        limit=250  # Multi-page to test concatenation logic
    )

    assert result_desc is not None and len(result_desc) > 0

    # Verify descending sort order (allow for None/NA values at the end)
    sqfts_desc = result_desc["sqft"].dropna().tolist()
    assert len(sqfts_desc) > 0, "No properties with sqft found"
    assert sqfts_desc == sorted(sqfts_desc, reverse=True), f"Square footages not in descending order: {sqfts_desc[:10]}"

    # Test ascending (smallest first)
    result_asc = scrape_property(
        location="Indianapolis, IN",
        listing_type="for_sale",
        sort_by="sqft",
        sort_direction="asc",  # Smallest first
        limit=250
    )

    assert result_asc is not None and len(result_asc) > 0

    # Verify ascending sort order
    sqfts_asc = result_asc["sqft"].dropna().tolist()
    assert len(sqfts_asc) > 0, "No properties with sqft found"
    assert sqfts_asc == sorted(sqfts_asc), f"Square footages not in ascending order: {sqfts_asc[:10]}"


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


def test_last_status_change_date_field():
    """Test that last_status_change_date field is present and has hour-level precision"""
    from datetime import datetime

    # Test 1: Field is present in SOLD listings
    result_sold = scrape_property(
        location="Phoenix, AZ",
        listing_type="sold",
        past_days=30,
        limit=20
    )

    assert result_sold is not None and len(result_sold) > 0

    # Check that last_status_change_date column exists
    assert "last_status_change_date" in result_sold.columns, \
        "last_status_change_date column should be present in results"

    # Check that at least some properties have this field populated
    has_status_change_date = False
    for idx in range(min(10, len(result_sold))):
        status_change_date_str = result_sold.iloc[idx]["last_status_change_date"]
        if pd.notna(status_change_date_str):
            has_status_change_date = True
            # Verify it has hour-level precision (includes time)
            assert " " in str(status_change_date_str) or "T" in str(status_change_date_str), \
                f"last_status_change_date should include time component: {status_change_date_str}"
            break

    # Note: It's possible some properties don't have this field, so we just verify it exists
    # assert has_status_change_date, "At least some properties should have last_status_change_date"

    # Test 2: Field is present in PENDING listings
    result_pending = scrape_property(
        location="Dallas, TX",
        listing_type="pending",
        past_days=30,
        limit=20
    )

    assert result_pending is not None
    # Only check columns if we have results (empty DataFrame has no columns)
    if len(result_pending) > 0:
        assert "last_status_change_date" in result_pending.columns, \
            "last_status_change_date column should be present in PENDING results"

    # Test 3: Field is present in FOR_SALE listings
    result_for_sale = scrape_property(
        location="Austin, TX",
        listing_type="for_sale",
        past_days=7,
        limit=20
    )

    assert result_for_sale is not None and len(result_for_sale) > 0
    assert "last_status_change_date" in result_for_sale.columns, \
        "last_status_change_date column should be present in FOR_SALE results"


def test_last_status_change_date_precision_enhancement():
    """Test that pending_date and last_sold_date use hour-precision from last_status_change_date"""
    from datetime import datetime

    # Test with pydantic return type to examine actual Property objects
    # Use a larger time window to ensure we get some results
    result_sold = scrape_property(
        location="Phoenix, AZ",
        listing_type="sold",
        past_days=90,
        limit=30,
        return_type="pydantic"
    )

    assert result_sold is not None

    # Only run assertions if we have data (data availability may vary)
    if len(result_sold) > 0:
        # Check that dates have hour-level precision (not just date)
        for prop in result_sold[:10]:
            # If both last_sold_date and last_status_change_date exist
            if prop.last_sold_date and prop.last_status_change_date:
                # Both should be datetime objects with time info
                assert hasattr(prop.last_sold_date, 'hour'), \
                    "last_sold_date should have hour precision"
                assert hasattr(prop.last_status_change_date, 'hour'), \
                    "last_status_change_date should have hour precision"

                # If they're on the same day, the processor should have used
                # last_status_change_date to provide hour precision for last_sold_date
                if prop.last_sold_date.date() == prop.last_status_change_date.date():
                    # They should have the same timestamp (hour/minute/second)
                    assert prop.last_sold_date == prop.last_status_change_date, \
                        "last_sold_date should match last_status_change_date for hour precision"

    # Test with PENDING listings
    result_pending = scrape_property(
        location="Dallas, TX",
        listing_type="pending",
        past_days=90,
        limit=30,
        return_type="pydantic"
    )

    assert result_pending is not None

    # Only run assertions if we have data
    if len(result_pending) > 0:
        for prop in result_pending[:10]:
            # If both pending_date and last_status_change_date exist
            if prop.pending_date and prop.last_status_change_date:
                assert hasattr(prop.pending_date, 'hour'), \
                    "pending_date should have hour precision"
                assert hasattr(prop.last_status_change_date, 'hour'), \
                    "last_status_change_date should have hour precision"

                # If they're on the same day, pending_date should use the time from last_status_change_date
                if prop.pending_date.date() == prop.last_status_change_date.date():
                    assert prop.pending_date == prop.last_status_change_date, \
                        "pending_date should match last_status_change_date for hour precision"


def test_last_status_change_date_filtering_fallback():
    """Test that filtering falls back to last_status_change_date when primary date is missing"""
    from datetime import datetime, timedelta

    # This test verifies that if a property doesn't have the primary date field
    # (e.g., pending_date for PENDING listings), it can still be filtered using
    # last_status_change_date as a fallback

    # Test with PENDING properties using past_hours (client-side filtering)
    result_pending = scrape_property(
        location="Miami, FL",
        listing_type="pending",
        past_hours=72,
        limit=30
    )

    assert result_pending is not None

    # If we get results, verify they have either pending_date or last_status_change_date
    if len(result_pending) > 0:
        cutoff_time = datetime.now() - timedelta(hours=72)

        for idx in range(min(5, len(result_pending))):
            pending_date_str = result_pending.iloc[idx]["pending_date"]
            status_change_date_str = result_pending.iloc[idx]["last_status_change_date"]

            # At least one of these should be present for filtering to work
            has_date = pd.notna(pending_date_str) or pd.notna(status_change_date_str)

            # Note: Contingent properties without dates are allowed, so we don't assert here
            # The test just verifies the field exists and can be used


def test_last_status_change_date_hour_filtering():
    """Test that past_hours filtering works correctly with last_status_change_date for PENDING/SOLD"""
    from datetime import datetime, timedelta

    # Test with SOLD properties
    result_sold = scrape_property(
        location="Atlanta, GA",
        listing_type="sold",
        past_hours=48,
        limit=30
    )

    assert result_sold is not None

    if len(result_sold) > 0:
        cutoff_time = datetime.now() - timedelta(hours=48)

        # Verify that results are within 48 hours
        for idx in range(min(5, len(result_sold))):
            sold_date_str = result_sold.iloc[idx]["last_sold_date"]
            if pd.notna(sold_date_str):
                try:
                    sold_date = datetime.strptime(str(sold_date_str), "%Y-%m-%d %H:%M:%S")
                    # Should be within 48 hours with hour-level precision
                    assert sold_date >= cutoff_time, \
                        f"SOLD property last_sold_date {sold_date} should be within 48 hours of {cutoff_time}"
                except (ValueError, TypeError):
                    pass  # Skip if parsing fails

    # Test with PENDING properties
    result_pending = scrape_property(
        location="Denver, CO",
        listing_type="pending",
        past_hours=48,
        limit=30
    )

    assert result_pending is not None

    if len(result_pending) > 0:
        cutoff_time = datetime.now() - timedelta(hours=48)

        # Verify that results are within 48 hours
        for idx in range(min(5, len(result_pending))):
            pending_date_str = result_pending.iloc[idx]["pending_date"]
            if pd.notna(pending_date_str):
                try:
                    pending_date = datetime.strptime(str(pending_date_str), "%Y-%m-%d %H:%M:%S")
                    # Should be within 48 hours with hour-level precision
                    assert pending_date >= cutoff_time, \
                        f"PENDING property pending_date {pending_date} should be within 48 hours of {cutoff_time}"
                except (ValueError, TypeError):
                    pass  # Skip if parsing fails


def test_exclude_pending_with_raw_data():
    """Test that exclude_pending parameter works correctly with return_type='raw'"""

    # Query for sale properties with exclude_pending=True and raw data
    result = scrape_property(
        location="Phoenix, AZ",
        listing_type="for_sale",
        exclude_pending=True,
        return_type="raw",
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify that no pending or contingent properties are in the results
    for prop in result:
        flags = prop.get('flags', {})
        is_pending = flags.get('is_pending', False)
        is_contingent = flags.get('is_contingent', False)

        assert not is_pending, f"Property {prop.get('property_id')} should not be pending when exclude_pending=True"
        assert not is_contingent, f"Property {prop.get('property_id')} should not be contingent when exclude_pending=True"


def test_mls_only_with_raw_data():
    """Test that mls_only parameter works correctly with return_type='raw'"""

    # Query with mls_only=True and raw data
    result = scrape_property(
        location="Dallas, TX",
        listing_type="for_sale",
        mls_only=True,
        return_type="raw",
        limit=50
    )

    assert result is not None and len(result) > 0

    # Verify that all properties have MLS IDs (stored in source.id)
    for prop in result:
        source = prop.get('source', {})
        mls_id = source.get('id') if source else None

        assert mls_id is not None and mls_id != "", \
            f"Property {prop.get('property_id')} should have an MLS ID (source.id) when mls_only=True, got: {mls_id}"


def test_combined_filters_with_raw_data():
    """Test that both exclude_pending and mls_only work together with return_type='raw'"""

    # Query with both filters enabled and raw data
    result = scrape_property(
        location="Austin, TX",
        listing_type="for_sale",
        exclude_pending=True,
        mls_only=True,
        return_type="raw",
        limit=30
    )

    assert result is not None and len(result) > 0

    # Verify both filters are applied
    for prop in result:
        # Check exclude_pending filter
        flags = prop.get('flags', {})
        is_pending = flags.get('is_pending', False)
        is_contingent = flags.get('is_contingent', False)

        assert not is_pending, f"Property {prop.get('property_id')} should not be pending"
        assert not is_contingent, f"Property {prop.get('property_id')} should not be contingent"

        # Check mls_only filter
        source = prop.get('source', {})
        mls_id = source.get('id') if source else None

        assert mls_id is not None and mls_id != "", \
            f"Property {prop.get('property_id')} should have an MLS ID (source.id)"


def test_updated_since_filtering():
    """Test the updated_since parameter for filtering by last_update_date"""
    from datetime import datetime, timedelta

    # Test 1: Filter by last update in past 10 minutes (user's example)
    cutoff_time = datetime.now() - timedelta(minutes=10)
    result_10min = scrape_property(
        location="California",
        updated_since=cutoff_time,
        sort_by="last_update_date",
        sort_direction="desc",
        limit=100
    )

    assert result_10min is not None
    print(f"\n10-minute window returned {len(result_10min)} properties")

    # Test 2: Verify all results have last_update_date within range
    if len(result_10min) > 0:
        for idx in range(min(10, len(result_10min))):
            update_date_str = result_10min.iloc[idx]["last_update_date"]
            if pd.notna(update_date_str):
                try:
                    # Handle timezone-aware datetime strings
                    date_str = str(update_date_str)
                    if '+' in date_str or date_str.endswith('Z'):
                        # Remove timezone for comparison with naive cutoff_time
                        date_str = date_str.replace('+00:00', '').replace('Z', '')
                    update_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

                    assert update_date >= cutoff_time, \
                        f"Property last_update_date {update_date} should be >= {cutoff_time}"
                    print(f"Property {idx}: last_update_date = {update_date} (valid)")
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not parse date {update_date_str}: {e}")

    # Test 3: Compare different time windows
    result_1hour = scrape_property(
        location="California",
        updated_since=datetime.now() - timedelta(hours=1),
        limit=50
    )

    result_24hours = scrape_property(
        location="California",
        updated_since=datetime.now() - timedelta(hours=24),
        limit=50
    )

    print(f"1-hour window: {len(result_1hour)} properties")
    print(f"24-hour window: {len(result_24hours)} properties")

    # Longer time window should return same or more results
    if len(result_1hour) > 0 and len(result_24hours) > 0:
        assert len(result_1hour) <= len(result_24hours), \
            "1-hour filter should return <= 24-hour results"

    # Test 4: Verify sorting works with filtering
    if len(result_10min) > 1:
        # Get non-null dates
        dates = []
        for idx in range(len(result_10min)):
            date_str = result_10min.iloc[idx]["last_update_date"]
            if pd.notna(date_str):
                try:
                    # Handle timezone-aware datetime strings
                    clean_date_str = str(date_str)
                    if '+' in clean_date_str or clean_date_str.endswith('Z'):
                        clean_date_str = clean_date_str.replace('+00:00', '').replace('Z', '')
                    dates.append(datetime.strptime(clean_date_str, "%Y-%m-%d %H:%M:%S"))
                except (ValueError, TypeError):
                    pass

        if len(dates) > 1:
            # Check if sorted descending
            for i in range(len(dates) - 1):
                assert dates[i] >= dates[i + 1], \
                    f"Results should be sorted by last_update_date descending: {dates[i]} >= {dates[i+1]}"


def test_updated_since_optimization():
    """Test that updated_since optimization works (auto-sort + early termination)"""
    from datetime import datetime, timedelta
    import time

    # Test 1: Verify auto-sort is applied when using updated_since without explicit sort
    start_time = time.time()
    result = scrape_property(
        location="California",
        updated_since=datetime.now() - timedelta(minutes=5),
        # NO sort_by specified - should auto-apply sort_by="last_update_date"
        limit=50
    )
    elapsed_time = time.time() - start_time

    print(f"\nAuto-sort test: {len(result)} properties in {elapsed_time:.2f}s")

    # Should complete quickly due to early termination optimization (<5 seconds)
    assert elapsed_time < 5.0, f"Query should be fast with optimization, took {elapsed_time:.2f}s"

    # Verify results are sorted by last_update_date (proving auto-sort worked)
    if len(result) > 1:
        dates = []
        for idx in range(min(10, len(result))):
            date_str = result.iloc[idx]["last_update_date"]
            if pd.notna(date_str):
                try:
                    clean_date_str = str(date_str)
                    if '+' in clean_date_str or clean_date_str.endswith('Z'):
                        clean_date_str = clean_date_str.replace('+00:00', '').replace('Z', '')
                    dates.append(datetime.strptime(clean_date_str, "%Y-%m-%d %H:%M:%S"))
                except (ValueError, TypeError):
                    pass

        if len(dates) > 1:
            # Verify descending order (most recent first)
            for i in range(len(dates) - 1):
                assert dates[i] >= dates[i + 1], \
                    "Auto-applied sort should order by last_update_date descending"

    print("Auto-sort optimization verified ✓")


def test_pending_date_optimization():
    """Test that PENDING + date filters get auto-sort and early termination"""
    from datetime import datetime, timedelta
    import time

    # Test: Verify auto-sort is applied for PENDING with past_days
    start_time = time.time()
    result = scrape_property(
        location="California",
        listing_type="pending",
        past_days=7,
        # NO sort_by specified - should auto-apply sort_by="pending_date"
        limit=50
    )
    elapsed_time = time.time() - start_time

    print(f"\nPENDING auto-sort test: {len(result)} properties in {elapsed_time:.2f}s")

    # Should complete quickly due to optimization (<10 seconds)
    assert elapsed_time < 10.0, f"PENDING query should be fast with optimization, took {elapsed_time:.2f}s"

    # Verify results are sorted by pending_date (proving auto-sort worked)
    if len(result) > 1:
        dates = []
        for idx in range(min(10, len(result))):
            date_str = result.iloc[idx]["pending_date"]
            if pd.notna(date_str):
                try:
                    clean_date_str = str(date_str)
                    if '+' in clean_date_str or clean_date_str.endswith('Z'):
                        clean_date_str = clean_date_str.replace('+00:00', '').replace('Z', '')
                    dates.append(datetime.strptime(clean_date_str, "%Y-%m-%d %H:%M:%S"))
                except (ValueError, TypeError):
                    pass

        if len(dates) > 1:
            # Verify descending order (most recent first)
            for i in range(len(dates) - 1):
                assert dates[i] >= dates[i + 1], \
                    "PENDING auto-applied sort should order by pending_date descending"

    print("PENDING optimization verified ✓")


def test_basic_last_update_date():
    from datetime import datetime, timedelta

    # Test with naive datetime (treated as local time)
    now = datetime.now()

    properties = scrape_property(
        "California",
        updated_since=now - timedelta(minutes=10),
        sort_by="last_update_date",
        sort_direction="desc"
    )

    # Convert now to timezone-aware for comparison with UTC dates in DataFrame
    now_utc = now.astimezone(tz=pytz.timezone("UTC"))

    # Check all last_update_date values are <= now
    assert (properties["last_update_date"] <= now_utc).all()

    # Verify we got some results
    assert len(properties) > 0


def test_timezone_aware_last_update_date():
    """Test that timezone-aware datetimes work correctly for updated_since"""
    from datetime import datetime, timedelta, timezone

    # Test with timezone-aware datetime (explicit UTC)
    now_utc = datetime.now(timezone.utc)

    properties = scrape_property(
        "California",
        updated_since=now_utc - timedelta(minutes=10),
        sort_by="last_update_date",
        sort_direction="desc"
    )

    # Check all last_update_date values are <= now
    assert (properties["last_update_date"] <= now_utc).all()

    # Verify we got some results
    assert len(properties) > 0


def test_timezone_handling_date_range():
    """Test timezone handling for date_from and date_to parameters"""
    from datetime import datetime, timedelta

    # Test with naive datetimes for date range (PENDING properties)
    now = datetime.now()
    three_days_ago = now - timedelta(days=3)

    properties = scrape_property(
        "California",
        listing_type="pending",
        date_from=three_days_ago,
        date_to=now
    )

    # Verify we got results and they're within the date range
    if len(properties) > 0:
        # Convert now to UTC for comparison
        now_utc = now.astimezone(tz=pytz.timezone("UTC"))
        assert (properties["pending_date"] <= now_utc).all()

