"""
Offline tests: no network. These run in CI on every pull request.

The live API tests are in test_realtor.py (marked `live`).
"""

import re
import warnings

import pytest
import requests

from homeharvest.core.scrapers import ScraperInput
from homeharvest.core.scrapers.models import ListingType, ReturnType
from homeharvest.core.scrapers.realtor import RealtorScraper
from homeharvest.core.scrapers.realtor.processors import process_property, process_extra_property_details, get_key
from homeharvest.exceptions import AuthenticationError, RateLimitError, HomeHarvestWarning
from homeharvest.utils import process_result


AREA = {"text": "Warren, MI", "area_type": "city", "city": "Warren", "state_code": "MI", "centroid": None}


def make_home(property_id, status="for_sale", **extra):
    home = {
        "property_id": str(property_id),
        "listing_id": "1",
        "href": f"https://www.realtor.com/realestateandhomes-detail/M{property_id}",
        "permalink": None,
        "status": status,
        "list_price": 300000,
        "list_price_min": None,
        "list_price_max": None,
        "list_date": "2026-11-20T10:00:00Z",
        "last_sold_date": None,
        "pending_date": None,
        "last_status_change_date": None,
        "last_update_date": None,
        "flags": {"is_pending": None, "is_contingent": None, "is_new_construction": None},
        "source": {"id": "MLS", "listing_id": "A1"},
        "description": {"beds": 3, "baths_full": 2, "sqft": 1500, "type": "single_family"},
        "location": {
            "address": {
                "line": "1 Main St",
                "street_number": "1",
                "street_name": "Main",
                "street_suffix": "St",
                "street_direction": None,
                "unit": None,
                "city": "Warren",
                "state_code": "MI",
                "postal_code": "48089",
                "coordinate": {"lat": 42.5, "lon": -83.0},
            },
            "county": {"name": "Macomb", "fips_code": "26099"},
            "neighborhoods": None,
        },
    }
    home.update(extra)
    return home


def make_scraper(**kwargs):
    defaults = dict(location="Warren, MI", listing_type=None, return_type=ReturnType.raw, extra_property_data=False)
    defaults.update(kwargs)
    scraper = RealtorScraper(ScraperInput(**defaults))
    scraper._sleep = lambda seconds: None
    scraper.handle_location = lambda: AREA
    return scraper


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")


# ---------------------------------------------------------------- #143 past_days with several listing types


def test_split_only_when_date_filter_and_several_types():
    assert make_scraper(listing_type=None)._split_listing_types() is None
    assert make_scraper(listing_type=None, last_x_days=1)._split_listing_types() == [
        ListingType.FOR_SALE, ListingType.FOR_RENT, ListingType.SOLD, ListingType.PENDING,
    ]
    assert make_scraper(
        listing_type=[ListingType.SOLD, ListingType.FOR_SALE], date_from="2026-01-01", date_to="2026-02-01"
    )._split_listing_types() == [ListingType.SOLD, ListingType.FOR_SALE]
    assert make_scraper(listing_type=ListingType.SOLD, last_x_days=1)._split_listing_types() is None


def test_each_listing_type_uses_its_own_date_field_and_results_are_deduped():
    scraper = make_scraper(listing_type=None, last_x_days=7)
    queries = []

    def fake_post(query, variables, operation_name):
        queries.append(query)
        if "status: sold" in query:
            results = [make_home(1, "sold", last_sold_date="2026-11-21")]
        elif "or_filters" in query:  #: pending
            results = [make_home(2, "for_sale", pending_date="2026-11-21", flags={"is_pending": True})]
        elif "status: for_rent" in query:
            results = [make_home(3, "for_rent")]
        else:  #: for_sale, also returns the pending home
            results = [make_home(4), make_home(2, "for_sale", pending_date="2026-11-21", flags={"is_pending": True})]
        return {"data": {"homeSearch": {"total": len(results), "count": len(results), "results": results}}}

    scraper._graphql_post = fake_post
    homes = scraper.search()

    assert len(queries) == 4
    by_status = {re.search(r"status: (\w+)", q).group(1) + ("+pending" if "or_filters" in q else ""): q for q in queries}
    assert 'sold_date: { min: "$today-7D" }' in by_status["sold"]
    assert 'list_date: { min: "$today-7D" }' in by_status["for_sale"]
    assert 'list_date: { min: "$today-7D" }' in by_status["for_rent"]
    #: pending dates are filtered client-side, never server-side
    assert "pending_date: {" not in by_status["for_sale+pending"]
    assert "list_date: {" not in by_status["for_sale+pending"]

    ids = [h["property_id"] for h in homes]
    assert sorted(ids) == ["1", "2", "3", "4"]  #: property 2 came back twice, kept once


def test_blocked_listing_type_keeps_earlier_types():
    scraper = make_scraper(listing_type=None, last_x_days=7)

    def fake_post(query, variables, operation_name):
        if "status: sold" in query:
            raise AuthenticationError("Received 403 Forbidden", response=None)
        results = [make_home(len(query) % 1000)]
        return {"data": {"homeSearch": {"total": 1, "count": 1, "results": results}}}

    scraper._graphql_post = fake_post
    with pytest.warns(HomeHarvestWarning, match="sold search failed"):
        homes = scraper.search()
    assert len(homes) >= 1  #: for_sale / for_rent results kept


def test_blocked_first_listing_type_raises():
    scraper = make_scraper(listing_type=None, last_x_days=7)

    def fake_post(query, variables, operation_name):
        raise AuthenticationError("Received 403 Forbidden", response=None)

    scraper._graphql_post = fake_post
    with pytest.raises(AuthenticationError):
        scraper.search()


def test_pending_with_dates_filters_before_fetching_details():
    """The API rejects sort/filter on pending_date: filter client-side, then enrich only the matches."""
    scraper = make_scraper(listing_type=ListingType.PENDING, last_x_days=7, sort_by="pending_date",
                           extra_property_data=True, return_type=ReturnType.pydantic)
    searches, detail_ids = [], []
    recent = "2099-01-01T00:00:00Z"  #: always inside the window
    results = [
        make_home(1, pending_date=recent, flags={"is_pending": True}),
        make_home(2, pending_date="2000-01-01T00:00:00Z", flags={"is_pending": True}),
        make_home(3, last_status_change_date=recent, flags={"is_contingent": True}),
        make_home(4, last_status_change_date="2000-01-01T00:00:00Z", flags={"is_contingent": True}),
    ]

    def fake_post(query, variables, operation_name):
        if operation_name == "GetHomeDetails":
            ids = re.findall(r"home_(\d+):", query)
            detail_ids.extend(ids)
            return {"data": {f"home_{pid}": {"property_id": pid} for pid in ids}}
        searches.append(query)
        return {"data": {"homeSearch": {"total": len(results), "count": len(results),
                                        "results": [dict(r) for r in results]}}}

    scraper._graphql_post = fake_post
    homes = scraper.search()

    assert "field: pending_date" not in searches[0]
    assert sorted(h.property_id for h in homes) == ["1", "3"]
    assert sorted(detail_ids) == ["1", "3"]  #: no details fetched for filtered-out homes


def test_one_item_list_is_a_single_type():
    scraper = make_scraper(listing_type=[ListingType.SOLD], last_x_days=3)
    queries = []

    def fake_post(query, variables, operation_name):
        queries.append(query)
        return {"data": {"homeSearch": {"total": 0, "count": 0, "results": []}}}

    scraper._graphql_post = fake_post
    scraper.search()
    assert len(queries) == 1
    assert 'sold_date: { min: "$today-3D" }' in queries[0]


# ---------------------------------------------------------------- extra property details


def test_extra_details_are_fetched_in_small_batches():
    scraper = make_scraper(extra_property_data=True)
    calls = []

    def fake_post(query, variables, operation_name):
        ids = re.findall(r"home_(\d+): home\(property_id: (\d+)\)", query)
        calls.append(len(ids))
        assert "...HomeExtraData" in query and "fragment HomeExtraData on Home" in query
        return {"data": {f"home_{pid}": {"property_id": pid, "taxHistory": [{"year": 2025, "tax": 100}]} for pid, _ in ids}}

    scraper._graphql_post = fake_post
    details = scraper.get_bulk_prop_details([str(i) for i in range(45)])

    assert calls == [20, 20, 5]
    assert len(details) == 45


def test_extra_details_errors_are_not_silent():
    scraper = make_scraper(extra_property_data=True)
    scraper._graphql_post = lambda *a: {"errors": [{"message": "parsing error: token limit reached"}]}

    with pytest.warns(HomeHarvestWarning, match="token limit reached"):
        details = scraper.get_bulk_prop_details([str(i) for i in range(30)])
    assert details == {}


def test_extra_details_stop_when_blocked():
    scraper = make_scraper(extra_property_data=True)
    calls = []

    def fake_post(query, variables, operation_name):
        calls.append(1)
        raise AuthenticationError("403", response=None)

    scraper._graphql_post = fake_post
    with pytest.warns(HomeHarvestWarning, match="missing for 60 of 60"):
        scraper.get_bulk_prop_details([str(i) for i in range(60)])
    assert len(calls) == 1  #: no hammering after a 403


def test_extra_details_partial_field_errors_do_not_warn():
    scraper = make_scraper(extra_property_data=True)
    scraper._graphql_post = lambda *a: {
        "data": {"home_1": {"property_id": "1"}},
        "errors": [{"message": "The user aborted a request."}],
    }
    with warnings.catch_warnings():
        warnings.simplefilter("error", HomeHarvestWarning)
        assert scraper.get_bulk_prop_details(["1"]) == {"1": {"property_id": "1"}}


def test_extra_property_data_parameter_is_honored():
    assert make_scraper(extra_property_data=True).extra_property_data is True
    assert make_scraper(extra_property_data=False).extra_property_data is False


# ---------------------------------------------------------------- 429 / 403 resilience


def test_429_is_retried_with_backoff(monkeypatch):
    scraper = make_scraper()
    responses = [
        FakeResponse(429, headers={"Retry-After": "2"}),
        FakeResponse(503),
        FakeResponse(200, {"data": {"ok": True}}),
    ]
    sleeps = []
    scraper._sleep = sleeps.append
    monkeypatch.setattr(requests, "post", lambda *a, **k: responses.pop(0))

    assert scraper._graphql_post("query X { x }", {}, "X") == {"data": {"ok": True}}
    assert len(sleeps) == 2
    assert 2 <= sleeps[0] <= 3  #: honors Retry-After (+ jitter)


def test_429_gives_up_with_rate_limit_error(monkeypatch):
    scraper = make_scraper()
    monkeypatch.setattr(requests, "post", lambda *a, **k: FakeResponse(429))
    with pytest.raises(RateLimitError):
        scraper._graphql_post("query X { x }", {}, "X")


def test_backoff_grows_and_is_capped():
    scraper = make_scraper()
    for attempt in range(10):
        delay = scraper._backoff_seconds(attempt)
        cap = min(scraper.BACKOFF_CAP, scraper.BACKOFF_BASE * 2 ** attempt)
        assert cap / 2 <= delay <= cap


@pytest.mark.parametrize("parallel", [True, False])
def test_403_mid_search_keeps_fetched_pages(parallel):
    scraper = make_scraper(listing_type=ListingType.FOR_SALE, parallel=parallel)

    def fake_general_search(variables, search_type):
        offset = variables["offset"]
        if offset >= 400:
            raise AuthenticationError("Received 403 Forbidden", response=None)
        return {"total": 1000, "properties": [make_home(offset + i) for i in range(200)]}

    scraper.general_search = fake_general_search
    with pytest.warns(HomeHarvestWarning, match="403"):
        homes = scraper.search()

    assert len(homes) == 400
    assert [h["property_id"] for h in homes[:3]] == ["0", "1", "2"]


def test_page_fan_out_is_polite():
    assert RealtorScraper.MAX_PAGE_WORKERS <= 4


# ---------------------------------------------------------------- open houses


def test_open_house_filter():
    assert make_scraper()._open_house_param() == ""
    assert make_scraper(open_house=True)._open_house_param() == 'open_house: { min: "$today" }'
    assert (
        make_scraper(open_house_from="2026-11-28", open_house_to="2026-11-29")._open_house_param()
        == 'open_house: { min: "2026-11-28", max: "2026-11-29" }'
    )
    assert make_scraper(open_house_to="2026-11-29")._open_house_param() == 'open_house: { min: "$today", max: "2026-11-29" }'


# ---------------------------------------------------------------- parsing of new fields


def _process(home, extra=True):
    return process_property(home, False, extra, False, ListingType.SOLD, get_key, process_extra_property_details)


def test_sold_list_date_after_sold_date_is_recovered_from_history():
    """#159: the sold record's list_date is the record creation time, after the sale."""
    home = make_home(
        1, "sold",
        list_date="2026-09-04T19:02:31Z",
        last_sold_date="2026-06-30",
        last_status_change_date="2026-09-04T19:02:31Z",
        source={"id": "BEFL", "listing_id": "F10543474"},
        property_history=[
            {"date": "2026-06-30", "event_name": "Sold", "price": 540000, "source_listing_id": "F10543474"},
            {"date": "2026-02-17", "event_name": "Price Changed", "price": 609000, "source_listing_id": "F10543474"},
            {"date": "2026-01-26", "event_name": "Listed", "price": 649000, "source_listing_id": "F10543474"},
            {"date": "2024-09-24", "event_name": "Listed for rent", "price": 3800, "source_listing_id": "F1"},
        ],
    )
    prop = _process(home)
    assert prop.list_date.date().isoformat() == "2026-01-26"
    assert prop.days_on_mls == (prop.last_sold_date.date() - prop.list_date.date()).days
    assert prop.original_list_price == 649000
    assert len(prop.property_history) == 4


def test_sold_list_date_after_sold_date_without_history_is_dropped():
    home = make_home(1, "sold", list_date="2026-08-12T07:12:10Z", last_sold_date="2025-12-31")
    prop = _process(home, extra=False)
    assert prop.list_date is None
    assert prop.days_on_mls is None


def test_price_change_fields():
    home = make_home(1, "for_sale", list_price=650000, last_price_change_amount=-29000.0,
                     last_price_change_date="2026-09-11T18:17:53.607Z", price_reduced_amount=29000.0)
    prop = _process(home, extra=False)
    assert prop.previous_list_price == 679000
    assert prop.last_price_change_amount == -29000
    assert prop.price_reduced_amount == 29000


def test_buyer_agent_median_rent_and_insurance():
    home = make_home(
        1, "sold", last_sold_date="2026-11-10", list_date="2026-07-23T00:00:00Z",
        buyers=[{
            "type": "buyer", "name": "Pat Buyer", "email": "pat@example.com", "fulfillment_id": "123",
            "phones": [{"number": "5550100", "type": "Mobile", "primary": True}],
            "office": {"name": "Buyer Office"}, "broker": {"name": "Buyer Broker"},
        }],
        mortgage={
            "insurance_rate": 0.0099, "property_tax_rate": 0.0155,
            "estimate": {"monthly_payment_details": [{"type": "home_insurance", "amount": 606}]},
        },
    )
    home["location"]["neighborhoods"] = [
        {"name": "Far North Dallas", "geo_statistics": {"housing_market": {"median_rent_price": 1390}}}
    ]
    home["location"]["postal_code"] = {"geo_statistics": {"housing_market": {"median_rent_price": 1210.0}}}

    prop = _process(home)
    assert prop.advertisers.buyer_agent.name == "Pat Buyer"
    assert prop.advertisers.buyer_office.name == "Buyer Office"
    assert prop.neighborhood_median_rent == 1390
    assert prop.zip_median_rent == 1210
    assert prop.insurance_rate == 0.0099
    assert prop.monthly_home_insurance == 606

    row = process_result(prop).iloc[0]
    assert row["buyer_agent_email"] == "pat@example.com"
    assert row["buyer_broker_name"] == "Buyer Broker"
    assert row["zip_median_rent"] == 1210
