# Changelog

## 2.1.0 (2026-11-23)

HomeHarvest now runs in Cloudflare Python Workers (Pyodide), with the same `scrape_property()` call. On CPython nothing else changes, except one fix that changes results: `open_house_to` now includes its last day.

### Upgrading from 2.0 (behavior change)
- **`open_house_to` now includes its last day.** The API reads the end of the window as midnight at the start of that day, so in 2.0 `open_house_from="2026-11-28", open_house_to="2026-11-29"` dropped every Sunday open house (Austin: 10 homes instead of 21). HomeHarvest now asks for the day after. If you passed the day after `open_house_to` to work around this, you now get one extra day: pass the last day you want.

### Cloudflare Python Workers (Pyodide)
- **The same call works in a Worker.** Under Pyodide (`sys.platform == "emscripten"`) each request goes through the JavaScript `fetch` API, and HomeHarvest waits for it with `pyodide.ffi.run_sync` (JSPI), so `scrape_property` stays synchronous. Call it inside a request handler, not at import time.
- **How to use it.** Add `homeharvest` to `dependencies` in the Worker's `pyproject.toml`, set `compatibility_flags = ["python_workers"]` with a `compatibility_date` of 2026-09-08 or later (Python 3.14) in the wrangler config, and deploy with `uv run pywrangler deploy` (pywrangler needs uv 0.13.4 or newer). Use `return_type="pydantic"` or `"raw"`. The README has a minimal Worker. The free [open house list](https://homeharvestdata.com/free-tools/open-house-list) runs this way.
- **No threads there.** Pages, detail batches and parsing run one at a time on the calling thread (`parallel` is treated as `False`). Retries wait on a JS timer instead of spinning.
- **Smaller install there.** `httpx2` and `pandas` are not installed under Pyodide (9 MB of packages instead of 39 MB). For `return_type="pandas"`, install pandas yourself. A missing pandas fails before the search, with a clear message.
- **Proxies are refused** there (`ValueError`): fetch can't use one.

### Everywhere else
- Nothing else changes on CPython: same dependencies, threads, HTTP/2 client and results. pandas is imported when a search first needs it, not on `import homeharvest`.
- `HttpClient(transport=FetchTransport(fetch=..., run_sync=...))` drives the fetch path with your own function (the offline tests use it to run the Pyodide path on CPython).

## 2.0.1 (2026-11-23)

README only: back to the original layout, so the PyPI page matches GitHub. No code changes.

## 2.0.0 (2026-11-23)

A faster release that needs Python 3.11 or newer. 0.9.1 was never published; its changes are part of 2.0.0.

### Upgrading from 0.9 (breaking changes)
- **Python 3.11 or newer.** Python 3.9 and 3.10 are no longer supported. `httpx2` needs 3.11. On 3.9 or 3.10, pip keeps installing 0.9.0.
- **`requests` is gone.** HomeHarvest uses `httpx2` (pydantic's maintained fork of httpx). If your code catches `requests.RequestException` around `scrape_property`, catch `homeharvest.NetworkError` instead (an `OSError`; raised after 2 retries on a connection, timeout, proxy or decoding error). A 5xx that keeps failing raises `homeharvest.HTTPStatusError`, a kind of `NetworkError`. `AuthenticationError` and `RateLimitError` are unchanged, but their `.response` is now a small HomeHarvest object (`status_code`, `headers`, `content`, `text`, `json()`), not a `requests.Response`.
- **SOCKS proxies** need `pip install "httpx2[socks]"` (before, `requests[socks]`). HTTP proxies work as before (`http://user:pass@host:port`). A proxy without a scheme (`host:port` or `user:pass@host:port`) is treated as `http://`.
- **TLS certificates** are checked against your operating system's trust store (httpx2 uses `truststore`), not the `certifi` bundle. Company CAs installed on the machine are now trusted. `certifi` is no longer installed with HomeHarvest.
- **`return_type="raw"`** no longer has fields HomeHarvest never mapped: `__typename`, the advertiser `corporation` / `rental_corporation` / `rental_management` objects (always empty), `price_reduced_date`, `median_listing_price` and the school district id.
- **Request rate:** a search still has at most 4 requests in flight, the same peak as 0.9.0's 4 parallel pages. Extra-data searches now reach that peak too, so they send their requests faster (the same number in less time). If you get rate limited, use `parallel=False`, which fetches pages and details one at a time.

### Faster
Median wall time, 2 live runs each, 0.9.0 and 2.0.0 run back to back (`scripts/bench.py`; results in `scripts/bench_results/`). The request count is the same.

| Search | 0.9.0 | 2.0.0 |
|---|---|---|
| Dallas sold, `past_days=30`, 200 homes | 2.8s | 1.8s |
| same, `extra_property_data=True` | 9.8s | 4.6s |
| Phoenix for sale, 200 homes, extra data | 11.0s | 4.2s |
| Dallas for rent, 200 homes, extra data | 12.7s | 6.2s |
| Dallas pending, `past_days=7`, extra data | 10.1s | 5.7s |
| Phoenix for sale, 1,000 homes | 7.4s | 4.3s |
| Houston for sale, 2,000 homes | 13.3s | 6.8s |
| Austin open houses (36 homes) | 0.8s | 0.8s |

- **Extra property details are fetched 4 batches at a time** (they were fetched one at a time). With several pages, page 1's details download while the later pages do. The first batch still goes alone, so a 403 or 429 costs one request, and a block stops every batch that hasn't started. With `parallel=False`, details are still fetched one at a time.
- **The DataFrame is built once** instead of one frame per home and then concatenated. This cuts about 2 ms of CPU per home to about 0.25 ms (about 4s off a 2,000-row search). The values and dtypes are the same.
- **Smaller queries.** Fields HomeHarvest doesn't map were removed: `__typename`, the always-empty advertiser `corporation` / `rental_corporation` / `rental_management` objects, `price_reduced_date`, `median_listing_price` and the school district id. The location lookup no longer asks for typeahead results. A detail batch is now 105 KB instead of 137 KB.

### HTTP library: requests → httpx2 (HTTP/2)
- HomeHarvest depends on `httpx2[http2]` (BSD-3, pure Python) instead of `requests`. Without a proxy, one search uses one pooled HTTP/2 connection. Before, every request opened a new TCP + TLS connection. With a proxy, each request still opens its own connection, so rotating proxies still give each request a new exit IP.
- Measured live: with 5 detail batches in parallel, HTTP/2 took 1.2 to 1.3s against 1.8 to 2.0s for a pooled `requests.Session`. We saw no 403s or 429s with any library.
- Not chosen: **niquests** (installing it replaces the `urllib3` that `requests`, boto and others import with urllib3-future). **curl_cffi** (a session can't be shared between threads, so no multiplexing, and it was not faster; the TLS impersonation made no difference to 403s from our IP).
- The sync HTTP/2 client of both httpx 0.28 and httpx2 2.16 shares one socket between threads unsafely. Against a local HTTP/2 server, 20 of 4,000 requests from 8 threads failed with "Resource temporarily unavailable" (httpx: 27 of 4,000). HomeHarvest runs httpx2's async client on a private background thread instead, which had no such errors.
- All HTTP goes through `homeharvest/core/scrapers/http.py`, so the library can change again without touching the scrapers.

### Reliability
- Connection, timeout, proxy and decoding errors are retried twice before `NetworkError` is raised. On a later page or a detail batch, they still give partial results with a `HomeHarvestWarning`.
- A 429 now pauses every request of the search, not only the one that got it.
- Homes whose extra details come back with a field error ("The user aborted a request.", which empties that field) are fetched once more. 0.9.0 gets these errors too. In testing, `property_history` came back empty once for 6 of 199 rentals. It didn't happen again in later runs, and we haven't confirmed the cause.

### Packaging
- `requires-python = ">=3.11"`, classifiers for 3.11 to 3.14. Dependencies: `httpx2[http2]`, `pandas`, `pydantic`, `tenacity`. `requests` and `eval-type-backport` are no longer dependencies.
- CI runs the offline tests on Python 3.11 to 3.14 on `ubuntu-latest`.
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
