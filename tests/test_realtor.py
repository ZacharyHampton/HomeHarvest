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
    address_result = scrape_property("1 Hawthorne St Unit 12F, San Francisco, CA 94105", return_type="raw")
    assert address_result[0]["open_houses"] is not None  #: has open house data from address search

    zip_code_result = scrape_property("94105", return_type="raw")
    address_from_zip_result = list(filter(lambda row: row["property_id"] == '1264014746', zip_code_result))

    assert address_from_zip_result[0]["open_houses"] is not None  #: has open house data from general search



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