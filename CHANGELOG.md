# Changelog

## 0.9.0 (2026-11-23)

### Fixes
- **`past_days` / `past_hours` / `date_from`+`date_to` now work when `listing_type` is `None` or a list** (#143). HomeHarvest runs one search per listing type with the right date field (sold date for `sold`, list date for `for_sale` / `for_rent`, pending date filtered client-side for `pending`), then merges the results and drops duplicates by `property_id`. Before, the date filter was silently dropped and searches returned up to 10,000 homes.
- **Extra property details work again at the default page size.** The bulk detail query asked for 200 homes at once and the API rejected it ("token limit reached"), so schools, tax history and assessed value came back empty with no error. Details are now fetched 20 homes per request using a shared GraphQL fragment, and `extra_property_data` is honored again (0.8.18 had it forced off).
- **Errors are no longer silent.** If extra details or a later results page can't be fetched, you get a `HomeHarvestWarning` that says what failed and how many homes are affected.
- **Sold `list_date` later than the sold date** (#159). Realtor feeds sometimes re-create a sold listing after closing, so `list_date` became that record's creation time (about 46% of Miami sales from April to June 2026). HomeHarvest now gets the real list date from property history when it can (with `extra_property_data=True`). If it can't, it sets `list_date` and `days_on_mls` to empty instead of returning a wrong date.
- **`offset` returned nothing.** `offset=200, limit=200` returned 0 rows because the first page was cut to `limit - offset`. It now returns rows 200 to 399.
- **Python 3.9 really works.** `pyproject.toml` said `>=3.9`, but `str | list[str]` type hints crashed on import. Added `from __future__ import annotations` and the `eval-type-backport` dependency (installed only on Python < 3.10).

### Features
- **Open house filter:** `open_house=True` returns only listings with an upcoming open house. `open_house_from` / `open_house_to` (date, datetime, or `"YYYY-MM-DD"`) limit it to a date window. Both use the server-side filter.
- **Buyer's agent on sold homes** (#134): `buyer_agent_name`, `buyer_agent_email`, `buyer_agent_phones`, `buyer_agent_id`, `buyer_agent_mls_set`, `buyer_agent_nrds_id`, `buyer_office_name`, `buyer_broker_name` (Pydantic: `advertisers.buyer_agent`, `advertisers.buyer_office`, `advertisers.buyer_broker`). Needs `extra_property_data`.
- **Median rent** (#137): `neighborhood_median_rent` and `zip_median_rent`. Needs `extra_property_data`.
- **Insurance and tax rates** (#121): `insurance_rate` and `property_tax_rate` (yearly, as a share of price) and `monthly_home_insurance` (dollars). Usually only on for-sale homes. Needs `extra_property_data`.
- **Price changes** (#120, partial): `last_price_change_amount`, `last_price_change_date`, `price_reduced_amount`, and `previous_list_price` (from search results, no extra request). `original_list_price` (where the current listing started) comes from property history and needs `extra_property_data`.
- **`property_history`:** listed, price changed and sold events (date, event, price, change, source). Needs `extra_property_data`.
- **Resilience:** 429 and 5xx responses are retried with exponential backoff and jitter (honors `Retry-After`, up to 5 retries), then `RateLimitError` is raised. If a later page fails (403 / 429 / network), you keep the pages already fetched and get a `HomeHarvestWarning` instead of losing the whole search.
- New exports: `RateLimitError`, `HomeHarvestWarning`, `homeharvest.__version__`.

### Behavior changes (check before upgrading)
- `extra_property_data=True` (the default) is honored again, so a search adds about one request per 20 homes. Pass `extra_property_data=False` for large, fast pulls.
- `listing_type=None` together with a date filter now covers `for_sale`, `for_rent`, `sold` and `pending`. `off_market` is left out because it has no date to filter on. Without a date filter, `None` works as before.
- When several listing types are searched one by one, `limit` caps the merged result.
- `parallel=True` now fetches at most 4 pages at a time (before, it used the thread pool default of up to 32).
- A 403 on a later page now returns partial results with a warning instead of raising. A 403 on the first request still raises `AuthenticationError`, and location lookups now raise it directly instead of tenacity's `RetryError`.
- Sold homes whose `list_date` was after the sold date now have the recovered date or an empty one (see #159 above). `return_type="raw"` is unchanged.

### Packaging
- `pyproject.toml` now uses standard `[project]` metadata (built with poetry-core 2.1 or newer), with a new description, keywords, classifiers and project URLs.
- Dev dependencies now include `pytz` (needed by the tests).
- New CI workflow (`.github/workflows/ci.yml`): offline tests on Python 3.9 to 3.13 plus a package build check for every PR. Live API tests can be started by hand.
- README rewritten.
