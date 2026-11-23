# Changelog

## 0.9.1 (unreleased)

### Faster
Median wall time from 3 live runs each, 0.9.0 against 0.9.1 (`scripts/bench.py`, results in `scripts/bench_results/`). The request count is the same, and so is the peak of 4 requests in flight.

| Search | 0.9.0 | 0.9.1 |
|---|---|---|
| Dallas sold, `past_days=30`, 200 homes | 2.1s | 2.3s (one page: the server's time) |
| same, `extra_property_data=True` | 11.7s | 4.8s (6.7s in a slower hour) |
| Phoenix for sale, 200 homes, extra data | 13.6s | 4.6s |
| Dallas for rent, 200 homes, extra data | 10.9 to 12.5s (2 runs) | 5.6s |
| Dallas pending, `past_days=7`, extra data | 12.3s | 6.9s |
| Phoenix for sale, 1,000 homes | 6.2s | 4.4s |
| Houston for sale, 2,000 homes | 12.9s | 7.2s |
| Austin open houses | 0.9s | 0.8s |

- **Extra property details are fetched 4 batches at a time** (they were fetched one at a time). With several pages, page 1's details download while the later pages do. The first batch still goes alone, so a 403 or 429 costs one request, and a block stops every batch that hasn't started. With `parallel=False`, details are still fetched one at a time.
- **The DataFrame is built once** instead of one frame per home and then concatenated. This cuts about 2 ms of CPU per home to about 0.25 ms (about 4s off a 2,000-row search). The values and dtypes are the same.
- **Smaller queries.** Fields HomeHarvest doesn't map were removed: `__typename`, the always-empty advertiser `corporation` / `rental_corporation` / `rental_management` objects, `price_reduced_date`, `median_listing_price` and the school district id. The location lookup no longer asks for typeahead results. A detail batch is now 105 KB instead of 137 KB.

### HTTP library: requests → httpx (HTTP/2)
- HomeHarvest now depends on `httpx[http2]` (BSD-3, pure Python, wheels for every OS and Python 3.9 to 3.14) instead of `requests`. Without a proxy, one search uses one pooled HTTP/2 connection. Before, every request opened a new TCP + TLS connection.
- Measured live: with 5 detail batches in parallel, HTTP/2 took 1.2 to 1.3s against 1.8 to 2.0s for a pooled `requests.Session`. In end-to-end pairs it was faster in 5 of 8. We saw no 403s or 429s with any library.
- Not chosen: **niquests** (installing it replaces the `urllib3` that `requests`, boto and others import with urllib3-future). **curl_cffi** (a session can't be shared between threads, so no multiplexing, and it was not faster; its releases after 0.13/0.14 drop Python 3.9; the TLS impersonation made no difference to 403s from our IP). **httpx's sync HTTP/2 client** (it shares one socket between threads unsafely, and 6 of 48 parallel requests failed with "Resource temporarily unavailable"). HomeHarvest uses httpx's async client on a private background thread instead.
- All HTTP goes through `homeharvest/core/scrapers/http.py`, so the library can change again without touching the scrapers.

### Reliability
- Connection, timeout and proxy errors are retried twice. After that they raise `homeharvest.NetworkError`, and a 5xx that is still failing after retries raises `homeharvest.HTTPStatusError`. On a later page or a detail batch, both still give partial results with a `HomeHarvestWarning`.
- A 429 now pauses every request of the search, not only the one that got it.
- Homes whose extra details come back with a field error ("The user aborted a request.", which empties that field) are fetched once more.

### Behavior notes (check before upgrading)
- `requests` is no longer installed with HomeHarvest. If your code catches `requests.RequestException` around `scrape_property`, catch `homeharvest.NetworkError` (an `OSError`) instead. `AuthenticationError` and `RateLimitError` are unchanged.
- Proxies: the `proxy` parameter works as before (`http://user:pass@host:port`). With a proxy, each request still opens its own connection, so rotating proxies still give each request a new exit IP. SOCKS proxies need `pip install "httpx[socks]"` (before, `requests[socks]`).
- `return_type="raw"` no longer has the removed fields listed above.
- Up to 4 requests are in flight per search, the same peak as 0.9.0's 4 parallel pages. Extra-data searches now reach that peak too, so they send their requests faster (the same number in less time). If you're rate limited, use `parallel=False`.

### Packaging
- CI: the Python 3.9 job runs on `ubuntu-24.04`, because `ubuntu-latest` (26.04) has no Python 3.9 build.
- New `scripts/bench.py` (not part of the package): live timings, request counts, bytes and a fill-rate report for every column, compared on the same homes.

## 0.9.0 (2026-11-23)

### Fixes
- **`past_days` / `past_hours` / `date_from`+`date_to` now work when `listing_type` is `None` or a list** (#143). HomeHarvest runs one search per listing type with the right date field (sold date for `sold`, list date for `for_sale` / `for_rent`, pending date filtered client-side for `pending`), then merges the results and drops duplicates by `property_id`. Before, the date filter was silently dropped and searches returned up to 10,000 homes.
- **Extra property details work again at the default page size.** The bulk detail query asked for 200 homes at once and the API rejected it ("token limit reached"), so schools, tax history and assessed value came back empty with no error. Details are now fetched 20 homes per request using a shared GraphQL fragment, and `extra_property_data` is honored again (0.8.18 had it forced off).
- **Errors are no longer silent.** If extra details or a later results page can't be fetched, you get a `HomeHarvestWarning` that says what failed and how many homes are affected.
- **Sold `list_date` later than the sold date** (#159). The data source sometimes re-creates a sold listing after closing, so `list_date` became that record's creation time (about 46% of Miami sales from April to June 2026). HomeHarvest now gets the real list date from property history when it can (with `extra_property_data=True`). If it can't, it sets `list_date` and `days_on_mls` to empty instead of returning a wrong date.
- **`listing_type="pending"` with `past_days` / dates returned 0 homes.** The search asked the API to sort by `pending_date`, which it now rejects. The error was swallowed, so you got empty results. Pending searches are now sorted client-side. Pending and contingent listings are filtered on `pending_date`, or on `last_status_change_date` when a contingent listing has no pending date (before, those were always included). Extra details are fetched only for the homes that pass the filter. Dallas pending with `past_days=7` now returns 160 homes (0 before).
- **`offset` returned nothing.** `offset=200, limit=200` returned 0 rows because the first page was cut to `limit - offset`. It now returns rows 200 to 399.
- **Python 3.9 really works.** `pyproject.toml` said `>=3.9`, but `str | list[str]` type hints crashed on import. Added `from __future__ import annotations` and the `eval-type-backport` dependency (installed only on Python < 3.10).

### Features
- **Open house filter:** `open_house=True` returns only listings with an upcoming open house. `open_house_from` / `open_house_to` (date, datetime, or `"YYYY-MM-DD"`) limit it to a date window. Both use the server-side filter.
- **Open house times in the default output:** new columns `next_open_house_start`, `next_open_house_end` and `open_houses` (every scheduled time, e.g. `2026-11-28 15:00-17:00 MST`). Before, open house times were only in `pydantic` / `raw` output.
- **Buyer's agent on sold homes** (#134): `buyer_agent_name`, `buyer_agent_email`, `buyer_agent_phones`, `buyer_agent_id`, `buyer_agent_mls_set`, `buyer_agent_nrds_id`, `buyer_office_name`, `buyer_broker_name` (Pydantic: `advertisers.buyer_agent`, `advertisers.buyer_office`, `advertisers.buyer_broker`). Needs `extra_property_data`.
- **Median rent** (#137): `neighborhood_median_rent` and `zip_median_rent`. Needs `extra_property_data`.
- **Insurance and tax rates** (#121): `insurance_rate` and `property_tax_rate` (yearly, as a share of price) and `monthly_home_insurance` (dollars). Usually only on for-sale homes. Needs `extra_property_data`.
- **Price changes** (#120, partial): `last_price_change_amount`, `last_price_change_date`, `price_reduced_amount`, and `previous_list_price` (from search results, no extra request). `original_list_price` (where the current listing started) comes from property history and needs `extra_property_data`.
- **`property_history`:** listed, price changed and sold events (date, event, price, change, source). Needs `extra_property_data`.
- **Resilience:** 429 and 5xx responses are retried with exponential backoff and jitter (honors `Retry-After`, up to 5 retries), then `RateLimitError` is raised. If a later page fails (403 / 429 / network), you keep the pages already fetched and get a `HomeHarvestWarning` instead of losing the whole search.
- New exports: `RateLimitError`, `HomeHarvestWarning`, `homeharvest.__version__`.

### Behavior changes (check before upgrading)
- **`extra_property_data` now defaults to `False`.** Schools, tax history, assessed value, property history, buyer's agent, median rent and insurance/tax rates need `extra_property_data=True`, which adds about one request per 20 homes (200 sold homes: ~2s without, ~12s with). In 0.8.18 it was forced off; in 0.8.10 and earlier it defaulted on.
- `listing_type=None` together with a date filter now covers `for_sale`, `for_rent`, `sold` and `pending`. `off_market` is left out because it has no date to filter on. Without a date filter, `None` works as before.
- When several listing types are searched one by one, `limit` caps the merged result.
- `parallel=True` now fetches at most 4 pages at a time (before, it used the thread pool default of up to 32).
- A 403 on a later page now returns partial results with a warning instead of raising. A 403 on the first request still raises `AuthenticationError`, and location lookups now raise it directly instead of tenacity's `RetryError`.
- Contingent listings without a `pending_date` are now date-filtered on `last_status_change_date` in pending searches, instead of always being included.
- Sold homes whose `list_date` was after the sold date now have the recovered date or an empty one (see #159 above). `return_type="raw"` is unchanged.

### Packaging
- `pyproject.toml` now uses standard `[project]` metadata (built with poetry-core 2.1 or newer), with a new description, keywords, classifiers and project URLs.
- Dev dependencies now include `pytz` (needed by the tests).
- New CI workflow (`.github/workflows/ci.yml`): offline tests on Python 3.9 to 3.13 plus a package build check for every PR. Live API tests can be started by hand.
- README rewritten.
