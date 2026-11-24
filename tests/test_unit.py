"""
Offline tests: no network. These run in CI on every pull request.

The live API tests are in test_realtor.py (marked `live`).
"""

import asyncio
import json
import re
import threading
import time
import warnings

import pytest

from homeharvest.core.scrapers import ScraperInput
from homeharvest.core.scrapers.models import ListingType, ReturnType
from homeharvest.core.scrapers.http import HttpClient, HttpResponse
from homeharvest.core.scrapers.realtor import RealtorScraper
from homeharvest.core.scrapers.realtor.processors import process_property, process_extra_property_details, get_key
from homeharvest.exceptions import AuthenticationError, RateLimitError, HomeHarvestWarning, NetworkError, HTTPStatusError
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
            raise HTTPStatusError(f"{self.status_code} error", response=self)


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

    assert sorted(calls) == [5, 20, 20]
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
    monkeypatch.setattr(scraper.http, "post", lambda *a, **k: responses.pop(0))

    assert scraper._graphql_post("query X { x }", {}, "X") == {"data": {"ok": True}}
    assert len(sleeps) == 2
    assert 2 <= sleeps[0] <= 3  #: honors Retry-After (+ jitter)


def test_429_gives_up_with_rate_limit_error(monkeypatch):
    scraper = make_scraper()
    monkeypatch.setattr(scraper.http, "post", lambda *a, **k: FakeResponse(429))
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

    def fake_general_search(variables, search_type, finish=True):
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
    #: pages and detail batches together never have more than 4 requests in flight (same as 0.9.0)
    assert HttpClient().max_in_flight <= 4


def test_network_errors_are_retried_then_raised(monkeypatch):
    scraper = make_scraper()
    calls = []

    def flaky(*a, **k):
        calls.append(1)
        if len(calls) <= 2:
            raise NetworkError("ReadError: connection reset")
        return FakeResponse(200, {"data": {"ok": True}})

    monkeypatch.setattr(scraper.http, "post", flaky)
    assert scraper._graphql_post("query X { x }", {}, "X") == {"data": {"ok": True}}
    assert len(calls) == 3

    def down(*a, **k):
        calls.append(1)
        raise NetworkError("ConnectError: down")

    calls.clear()
    monkeypatch.setattr(scraper.http, "post", down)
    with pytest.raises(NetworkError):
        scraper._graphql_post("query X { x }", {}, "X")
    assert len(calls) == 1 + RealtorScraper.NETWORK_RETRIES


@pytest.mark.parametrize("parallel", [True, False])
def test_network_error_mid_search_keeps_fetched_pages(parallel):
    scraper = make_scraper(listing_type=ListingType.FOR_SALE, parallel=parallel)

    def fake_general_search(variables, search_type, finish=True):
        offset = variables["offset"]
        if offset >= 400:
            raise NetworkError("ReadTimeout")
        return {"total": 1000, "properties": [make_home(offset + i) for i in range(200)]}

    scraper.general_search = fake_general_search
    with pytest.warns(HomeHarvestWarning, match="NetworkError"):
        homes = scraper.search()
    assert len(homes) == 400


def _detail_fake(log, delay=0.02):
    """Fake _graphql_post for detail batches that records how many run at once."""
    state = {"now": 0, "max": 0}
    lock = threading.Lock()

    def fake_post(query, variables, operation_name):
        ids = [pid for pid, _ in re.findall(r"home_(\d+): home\(property_id: (\d+)\)", query)]
        with lock:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
            log.append(("start", ids[0]))
        time.sleep(delay)
        with lock:
            state["now"] -= 1
            log.append(("end", ids[0]))
        return {"data": {f"home_{pid}": {"property_id": pid, "taxHistory": [{"year": 2025, "tax": 1}]} for pid in ids}}

    return fake_post, state


def test_detail_batches_run_concurrently_but_bounded():
    scraper = make_scraper(extra_property_data=True)
    log = []
    scraper._graphql_post, state = _detail_fake(log)
    details = scraper.get_bulk_prop_details([str(i) for i in range(200)])

    assert len(details) == 200
    assert 1 < state["max"] <= RealtorScraper.DETAIL_WORKERS
    assert log[:2] == [("start", "0"), ("end", "0")]  #: the first batch goes alone (a 403 costs 1 request)


def test_detail_batches_are_sequential_when_parallel_is_off():
    scraper = make_scraper(extra_property_data=True, parallel=False)
    scraper._graphql_post, state = _detail_fake([], delay=0.001)
    assert len(scraper.get_bulk_prop_details([str(i) for i in range(100)])) == 100
    assert state["max"] == 1


def test_first_page_details_overlap_later_pages():
    scraper = make_scraper(listing_type=ListingType.FOR_SALE, extra_property_data=True, limit=400)
    events = []
    detail_post, _ = _detail_fake(events, delay=0.05)

    def fake_post(query, variables, operation_name):
        if operation_name == "GetHomeSearch":
            offset = variables["offset"]
            events.append(("page", offset))
            results = [make_home(offset + i) for i in range(200)]
            return {"data": {"homeSearch": {"total": 400, "count": 200, "results": results}}}
        return detail_post(query, variables, operation_name)

    scraper._graphql_post = fake_post
    homes = scraper.search()

    assert [h["property_id"] for h in homes] == [str(i) for i in range(400)]  #: API order kept
    assert all(h.get("taxHistory") for h in homes)  #: every home got its details
    #: page 2 was requested before page 1's last detail batch finished
    assert events.index(("page", 200)) < events.index(("end", "180"))


def test_http_client_caps_requests_in_flight():
    client = HttpClient(max_in_flight=3)
    state = {"now": 0, "max": 0}
    lock = threading.Lock()

    async def fake_post(self, http, url, data, headers, fresh):
        with lock:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
        await asyncio.sleep(0.02)
        with lock:
            state["now"] -= 1
        return HttpResponse(200, {}, b'{"ok": true}')

    client._post = fake_post.__get__(client)
    try:
        threads = [threading.Thread(target=client.post, args=("https://x",), kwargs={"data": "{}"}) for _ in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        client.close()
    assert state["max"] == 3


def test_http_client_with_proxy_opens_a_connection_per_request():
    assert HttpClient().reuse_connections is True
    assert HttpClient(proxy="http://user:pass@127.0.0.1:8080").reuse_connections is False


def test_proxy_without_scheme_is_http():
    assert HttpClient(proxy="127.0.0.1:8080").proxy == "http://127.0.0.1:8080"
    assert HttpClient(proxy="user:pass@127.0.0.1:8080").proxy == "http://user:pass@127.0.0.1:8080"
    assert HttpClient(proxy="socks5://127.0.0.1:1080").proxy == "socks5://127.0.0.1:1080"


def test_http_errors_become_network_errors():
    import httpx2

    client = HttpClient()

    async def broken(self, http, url, data, headers, fresh):
        raise httpx2.DecodingError("bad gzip")

    client._post = broken.__get__(client)
    try:
        with pytest.raises(NetworkError, match="DecodingError"):
            client.post("https://x", data="{}")
    finally:
        client.close()


def test_429_pauses_other_requests(monkeypatch):
    scraper = make_scraper()
    responses = [FakeResponse(429, headers={"Retry-After": "5"}), FakeResponse(200, {"data": {}})]
    monkeypatch.setattr(scraper.http, "post", lambda *a, **k: responses.pop(0))
    scraper._graphql_post("query X { x }", {}, "X")
    assert scraper.http.pause_remaining() > 4  #: other threads wait before sending


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


def test_one_dataframe_matches_concatenating_one_frame_per_home():
    """properties_to_dataframe must give the same values and dtypes as the 0.9.0 per-row concat."""
    import pandas as pd
    from homeharvest.utils import properties_to_dataframe, ordered_properties

    homes = [
        make_home(1, "sold", last_sold_date="2026-11-10", list_date="2026-07-23T00:00:00Z", hoa={"fee": 120}),
        make_home(2, "for_sale", description={"beds": None, "baths_full": 1, "sqft": 900, "type": "condos"}),
        make_home(3, "for_sale", last_price_change_amount=-1000.0, list_price=None),
        make_home(4, "for_rent", open_houses=[{"start_date": "2026-11-28T15:00:00Z", "end_date": "2026-11-28T17:00:00Z"}]),
    ]
    props = [_process(home, extra=False) for home in homes]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        old = pd.concat([process_result(p) for p in props], ignore_index=True)[ordered_properties]
    new = properties_to_dataframe(props)

    pd.testing.assert_frame_equal(new, old)
    assert properties_to_dataframe([]).empty


def test_homes_with_field_errors_are_fetched_again_once():
    """'The user aborted a request.' leaves a field empty for some homes: ask again for just those."""
    scraper = make_scraper(extra_property_data=True)
    calls = []

    def fake_post(query, variables, operation_name):
        ids = [pid for pid, _ in re.findall(r"home_(\d+): home\(property_id: (\d+)\)", query)]
        calls.append(ids)
        if len(calls) == 1:  #: first pass: homes 2 and 3 lose property_history
            return {
                "data": {f"home_{pid}": {"property_id": pid, "property_history": None if pid in "23" else [{"event_name": "Sold"}]} for pid in ids},
                "errors": [
                    {"message": "The user aborted a request.", "path": ["home_2", "property_history"]},
                    {"message": "The user aborted a request.", "path": ["home_3", "property_history"]},
                ],
            }
        #: retry: home 2 comes back whole, home 3 fails again
        return {
            "data": {f"home_{pid}": {"property_id": pid, "property_history": None if pid == "3" else [{"event_name": "Sold"}]} for pid in ids},
            "errors": [{"message": "The user aborted a request.", "path": ["home_3", "property_history"]}],
        }

    scraper._graphql_post = fake_post
    details = scraper.get_bulk_prop_details(["1", "2", "3"])

    assert calls == [["1", "2", "3"], ["2", "3"]]  #: one retry, only for the homes with errors
    assert details["2"]["property_history"] == [{"event_name": "Sold"}]
    assert details["3"]["property_history"] is None  #: still bad: the first copy is kept


# ---------------------------------------------------------------- Pyodide / Cloudflare Python Workers (fetch, no threads)


def test_headers_are_case_insensitive():
    from homeharvest.core.scrapers.http import Headers

    headers = Headers({"retry-after": "3", "Content-Type": "application/json"})
    assert headers.get("Retry-After") == "3"
    assert headers["CONTENT-TYPE"] == "application/json"
    assert "Content-Type" in headers and headers.get("missing") is None


def test_inline_executor_runs_in_order_without_threads():
    from concurrent.futures import as_completed
    from homeharvest.core.scrapers.runtime import InlineExecutor

    calls = []
    with InlineExecutor() as executor:
        futures = [executor.submit(calls.append, n) for n in range(3)]
        failed = executor.submit(lambda: 1 / 0)
        assert list(executor.map(lambda n: n * 2, [1, 2])) == [2, 4]
    assert calls == [0, 1, 2]
    assert len(list(as_completed(futures))) == 3
    with pytest.raises(ZeroDivisionError):
        failed.result()


def test_fetch_transport_skips_the_http_thread():
    from homeharvest.core.scrapers.http import FetchTransport

    sent = []

    async def fake_fetch(url, *, method, headers, body, timeout):
        sent.append((url, method, body, timeout))
        return HttpResponse(200, {}, b'{"ok": true}', url=url)

    client = HttpClient(timeout=7, transport=FetchTransport(timeout=7, fetch=fake_fetch, run_sync=asyncio.run))
    assert client.post("https://x", data="{}").json() == {"ok": True}
    assert client._thread is None and client._client is None
    assert sent == [("https://x", "POST", "{}", 7)]
    client.close()


@pytest.fixture
def pyodide(monkeypatch):
    """Pretend to be Pyodide: no threads, no httpx2, fetch + run_sync (JSPI) for HTTP."""
    import sys
    import types

    import homeharvest.core.scrapers as scrapers
    from homeharvest.core.scrapers import http, runtime

    for module in (runtime, http, scrapers):
        monkeypatch.setattr(module, "IN_PYODIDE", True)

    ffi = types.ModuleType("pyodide.ffi")
    ffi.can_run_sync = lambda: True
    ffi.run_sync = asyncio.run
    monkeypatch.setitem(sys.modules, "pyodide", types.ModuleType("pyodide"))
    monkeypatch.setitem(sys.modules, "pyodide.ffi", ffi)

    def no_threads(self):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", no_threads)

    requests = []
    replies = []

    async def fake_fetch(url, *, method, headers, body, timeout):
        requests.append(json.loads(body))
        status, payload = replies.pop(0)
        return HttpResponse(status, http.Headers({"retry-after": "0"}), json.dumps(payload).encode(), url=url)

    monkeypatch.setattr(http, "js_fetch", fake_fetch)
    return types.SimpleNamespace(requests=requests, replies=replies)


def test_scrape_property_under_pyodide_uses_fetch_and_no_threads(pyodide):
    from homeharvest import scrape_property

    geo = {"text": "Austin, TX", "geo": {"area_type": "city", "city": "Austin", "state_code": "TX"}}
    homes = [
        make_home(n, open_houses=[{"start_date": "2026-11-28T11:00:00", "end_date": "2026-11-28T13:00:00"}])
        for n in (1, 2)
    ]
    pyodide.replies.extend([
        (429, {}),  #: retried after a (JS timer) sleep
        (200, {"data": {"search_suggestions": {"geo_results": [geo]}}}),
        (200, {"data": {"homeSearch": {"total": 2, "count": 2, "results": homes}}}),
    ])

    results = scrape_property(
        "Austin, TX", listing_type="for_sale", open_house_from="2026-11-28", open_house_to="2026-11-29",
        return_type="pydantic", extra_property_data=False,
    )

    assert [home.property_id for home in results] == ["1", "2"]
    assert results[0].open_houses
    assert [r["operationName"] for r in pyodide.requests] == ["Search_suggestions", "Search_suggestions", "GetHomeSearch"]
    assert 'open_house: { min: "2026-11-28", max: "2026-11-29" }' in pyodide.requests[-1]["query"]


def test_proxies_are_refused_under_pyodide(pyodide):
    with pytest.raises(ValueError, match="Pyodide"):
        HttpClient(proxy="http://127.0.0.1:8080")
