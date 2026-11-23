<img src="https://github.com/ZacharyHampton/HomeHarvest/assets/78247585/d1a2bf8b-09f5-4c57-b33a-0ada8a34f12d" width="400">

**HomeHarvest is a Python library for US property data: homes for sale, sold, pending, rentals, open houses, and listing agent contacts.**

Give it a city, ZIP code, or address. Get back a clean table (pandas), typed models (Pydantic), or raw JSON.

> **Real estate agent?** Modern Realty writes your lead texts → https://www.modernrealty.io/?utm_source=github&utm_medium=readme&utm_campaign=homeharvest

> **MCP:** a new HomeHarvest MCP server (property data inside your AI agent) is coming soon → [tryhomeharvest.com/mcp](https://tryhomeharvest.com/mcp)

## Install

```bash
pip install -U homeharvest
```
_Python 3.9 or newer._

## Quick start

```py
from homeharvest import scrape_property

homes = scrape_property(location="San Diego, CA", listing_type="sold", past_days=30)
homes.to_csv("sold_last_30_days.csv", index=False)
print(len(homes), "homes sold in the last 30 days")
```

## What you can get

| You want | How |
|---|---|
| **Sold comps** | `listing_type="sold"`, `past_days=90`, or `location="<address>", radius=0.5` for comps around a home |
| **Open houses** | `open_house=True` (upcoming), or `open_house_from` / `open_house_to` for a date window |
| **Listing agent contacts** | every row has `agent_name`, `agent_email`, `agent_phones`, `office_name`, `broker_name` |
| **Buyer's agent (sold homes)** | `buyer_agent_name`, `buyer_agent_email`, `buyer_agent_phones`, `buyer_office_name` |
| **Rentals** | `listing_type="for_rent"`, plus `neighborhood_median_rent` and `zip_median_rent` |
| **Pending / contingent** | `listing_type="pending"` |
| **Price history and tax** | `property_history`, `previous_list_price`, `original_list_price`, `tax`, `tax_history`, `assessed_value` |
| **Costs** | `hoa_fee`, `insurance_rate`, `monthly_home_insurance`, `property_tax_rate`, `estimated_value` |

Export to CSV / Excel, or use `return_type="pydantic"` / `"raw"`. Filter by beds, baths, price, sqft, lot size, year built, and date (hours, days, or a range). Sort by price, date, size, or last update.

![homeharvest](https://github.com/ZacharyHampton/HomeHarvest/assets/78247585/b3d5d727-e67b-4a9f-85d8-1e65fd18620a)

## New in 0.9.0

### Date filters work with several listing types
`past_days`, `past_hours` and `date_from` / `date_to` now work when `listing_type` is `None` or a list. Each type is filtered on its own date (sold date for sold, list date for for sale / for rent, pending date for pending) and the results are merged with no duplicates.

```py
# everything listed, sold, or gone pending in Warren, MI yesterday
homes = scrape_property(location="Warren, MI", past_days=1)
```
With `listing_type=None` and a date filter, HomeHarvest searches for_sale, for_rent, sold and pending (off_market has no date to filter on).

### Open houses
```py
# listings with an open house this weekend
homes = scrape_property(
    location="Austin, TX",
    listing_type="for_sale",
    open_house_from="2026-11-28",
    open_house_to="2026-11-29",
)
print(homes[["formatted_address", "list_price", "agent_name", "agent_phones"]])
```
Use `open_house=True` for any upcoming open house. Open house times are in the `open_houses` field (`return_type="pydantic"` or `"raw"`).

### Buyer's agent, price history, median rent, insurance
These come with `extra_property_data=True` (the default). They cost one extra request per 20 homes; set `extra_property_data=False` for big, fast pulls.

```py
sold = scrape_property(location="Dallas, TX", listing_type="sold", past_days=30)
print(sold[["formatted_address", "sold_price", "original_list_price", "buyer_agent_name", "buyer_office_name"]])

rentals = scrape_property(location="Huntsville, AL", listing_type="for_rent")
print(rentals[["formatted_address", "list_price", "neighborhood_median_rent", "zip_median_rent"]])
```

- `previous_list_price`, `last_price_change_amount`, `price_reduced_amount`: spot price cuts (no extra request needed).
- `original_list_price`: the price the current listing started at.
- `insurance_rate` / `property_tax_rate`: yearly estimate as a share of price (e.g. `0.0099` = 0.99%); `monthly_home_insurance` in dollars. Usually only on for-sale homes.

### Fewer failed runs
- HTTP 429 (rate limited) is retried with exponential backoff and jitter.
- If one page fails mid-search (403 or 429), you get the pages already fetched plus a `HomeHarvestWarning`, instead of an exception that loses everything.
- `parallel=True` fetches at most 4 pages at a time.

## More examples

### Locations
```py
# Accepts: zip code, city, "city, state", full address, neighborhood, county, state
homes = scrape_property(location="92104")
comps = scrape_property(location="1234 Main St, San Diego, CA 92104", radius=0.5, listing_type="sold", past_days=180)
```

### Time-based filtering
```py
from datetime import datetime, timedelta

homes = scrape_property(
    location="Austin, TX",
    listing_type="for_sale",
    past_hours=24,  # or timedelta(hours=24)
    # date_from=datetime.now() - timedelta(days=7),  # alternative: datetime objects
    # date_to=datetime.now(),                         # hour precision is detected automatically
)
```

### Property filters
```py
homes = scrape_property(
    location="San Francisco, CA",
    listing_type="for_sale",
    beds_min=3, beds_max=5,
    baths_min=2.0,
    sqft_min=1500, sqft_max=3000,
    price_min=300000, price_max=800000,
    year_built_min=2000,
    lot_sqft_min=5000,
)
```

### Sorting and listing types
```py
# Sort options: list_price, list_date, sqft, beds, baths, last_update_date
# Listing types: "for_sale", "for_rent", "sold", "pending", "off_market", a list, or None
homes = scrape_property(
    location="Miami, FL",
    listing_type=["for_sale", "pending"],
    sort_by="list_price",
    sort_direction="asc",
    limit=100,
)
```

### Pagination
```py
# Sequential mode stops early once time filters stop matching (and is gentler on rate limits)
homes = scrape_property(
    location="Los Angeles, CA",
    listing_type="for_sale",
    updated_in_past_hours=2,
    parallel=False,
)

# Chunks: rows 200-399
page_2 = scrape_property(location="Los Angeles, CA", listing_type="for_sale", offset=200, limit=200)
```

### Pydantic models
```py
homes = scrape_property(location="San Diego, CA", listing_type="for_sale", return_type="pydantic")

for home in homes[:5]:
    print(home.address.formatted_address, home.list_price)
    if home.advertisers and home.advertisers.agent:
        print("  agent:", home.advertisers.agent.name, home.advertisers.agent.email)
```

## Output
```plaintext
>>> homes.head()
    MLS       MLS # Status          Style  ...     COEDate LotSFApx PrcSqft Stories
0  SDCA   230018348   SOLD         CONDOS  ...  2023-10-03   290110     803       2
1  SDCA   230016614   SOLD      TOWNHOMES  ...  2023-10-03     None     838       3
2  SDCA   230016367   SOLD         CONDOS  ...  2023-10-03    30056     649       1
3  MRCA  NDP2306335   SOLD  SINGLE_FAMILY  ...  2023-10-03     7519     661       2
4  SDCA   230014532   SOLD         CONDOS  ...  2023-10-03     None     752       1
[5 rows x 22 columns]
```

### Parameters for `scrape_property()`
```
Required
├── location (str): Flexible location search - accepts any of these formats:
│    - ZIP code: "92104"
│    - City: "San Diego" or "San Francisco"
│    - City, State (abbreviated or full): "San Diego, CA" or "San Diego, California"
│    - Full address: "1234 Main St, San Diego, CA 92104"
│    - Neighborhood: "Downtown San Diego"
│    - County: "San Diego County"
│    - State (no support for abbreviated): "California"
│
├── listing_type (str | list[str] | None): Choose the type of listing.
│    - 'for_sale'
│    - 'for_rent'
│    - 'sold'
│    - 'pending'
│    - 'off_market'
│    - 'new_community'
│    - 'other'
│    - 'ready_to_build'
│    - List of strings returns properties matching ANY status: ['for_sale', 'pending']
│    - None returns common listing types (for_sale, for_rent, sold, pending, off_market)
│
Optional
├── property_type (list): Choose the type of properties.
│    - 'single_family'
│    - 'multi_family'
│    - 'condos'
│    - 'condo_townhome_rowhome_coop'
│    - 'condo_townhome'
│    - 'townhomes'
│    - 'duplex_triplex'
│    - 'farm'
│    - 'land'
│    - 'mobile'
│
├── return_type (option): Choose the return type.
│    - 'pandas' (default)
│    - 'pydantic'
│    - 'raw' (json)
│
├── radius (decimal): Radius in miles to find comparable properties based on individual addresses.
│    Example: 5.5 (fetches properties within a 5.5-mile radius if location is set to a specific address; otherwise, ignored)
│
├── past_days (integer | timedelta): Number of past days to filter properties. Uses the sold date for 'sold',
│    the list date for 'for_sale' / 'for_rent', and the pending date for 'pending'.
│    Works with a list of listing types or None: each type is searched with its own date and results are merged.
│    Example: 30 (fetches properties listed/sold in the last 30 days)
│
├── past_hours (integer | timedelta): Number of past hours to filter properties (more precise than past_days). Uses client-side filtering.
│    Example: 24 or timedelta(hours=24) (fetches properties from the last 24 hours)
│    Note: Cannot be used together with past_days or date_from/date_to
│
├── date_from, date_to (string): Start and end dates to filter properties listed or sold, both dates are required.
│    (use this to get properties in chunks as there's a 10k result limit)
│    Accepts multiple formats with automatic precision detection:
│    - Date strings: "YYYY-MM-DD" (day precision)
│    - Datetime strings: "YYYY-MM-DDTHH:MM:SS" (hour precision, uses client-side filtering)
│    - date objects: date(2025, 1, 20) (day precision)
│    - datetime objects: datetime(2025, 1, 20, 9, 0) (hour precision)
│    Examples:
│      Day precision: "2023-05-01", "2023-05-15"
│      Hour precision: "2025-01-20T09:00:00", "2025-01-20T17:00:00"
│
├── updated_since (datetime | str): Filter properties updated since a specific date/time (based on last_update_date field)
│    Accepts datetime objects or ISO 8601 strings
│    Example: updated_since=datetime(2025, 11, 10, 9, 0) or "2025-11-10T09:00:00"
│
├── updated_in_past_hours (integer | timedelta): Filter properties updated in the past X hours (based on last_update_date field)
│    Accepts integer (hours) or timedelta object
│    Example: updated_in_past_hours=24 or timedelta(hours=24)
│
├── beds_min, beds_max (integer): Filter by number of bedrooms
│    Example: beds_min=2, beds_max=4 (2-4 bedrooms)
│
├── baths_min, baths_max (float): Filter by number of bathrooms
│    Example: baths_min=2.0, baths_max=3.5 (2-3.5 bathrooms)
│
├── sqft_min, sqft_max (integer): Filter by square footage
│    Example: sqft_min=1000, sqft_max=2500 (1,000-2,500 sq ft)
│
├── price_min, price_max (integer): Filter by listing price
│    Example: price_min=200000, price_max=500000 ($200k-$500k)
│
├── lot_sqft_min, lot_sqft_max (integer): Filter by lot size in square feet
│    Example: lot_sqft_min=5000, lot_sqft_max=10000 (5,000-10,000 sq ft lot)
│
├── year_built_min, year_built_max (integer): Filter by year built
│    Example: year_built_min=2000, year_built_max=2024 (built between 2000-2024)
│
├── sort_by (string): Sort results by field
│    Options: 'list_date', 'sold_date', 'list_price', 'sqft', 'beds', 'baths', 'last_update_date'
│    Example: sort_by='list_price'
│
├── sort_direction (string): Sort direction, default is 'desc'
│    Options: 'asc' (ascending), 'desc' (descending)
│    Example: sort_direction='asc' (cheapest first)
│
├── mls_only (True/False): If set, fetches only MLS listings (mainly applicable to 'sold' listings)
│
├── foreclosure (True/False): If set, fetches only foreclosures
│
├── proxy (string): In format 'http://user:pass@host:port'
│
├── extra_property_data (True/False): Default True. One extra request per 20 homes. Adds schools, tax history,
│    assessed value, price history, original list price, buyer's agent (sold), median rent, insurance / tax rates.
│    Set to False for faster, larger pulls.
│
├── exclude_pending (True/False): If set, excludes 'pending' properties from the 'for_sale' results unless listing_type is 'pending'
│
├── limit (integer): Limit the number of properties to fetch. Max & default is 10000.
│
├── offset (integer): Starting position for pagination within the 10k limit. Use with limit to fetch results in chunks.
│
├── open_house (True/False): Only return listings with an upcoming open house
│
├── open_house_from, open_house_to (date | datetime | "YYYY-MM-DD"): Only return listings with an open house in this window
│    Example: open_house_from="2026-11-28", open_house_to="2026-11-29"
│
└── parallel (True/False): Controls pagination strategy. Default is True (up to 4 pages at a time). Set to False for sequential fetching with early termination (gentler on rate limits, good for narrow time windows).
```

### Property Schema
```plaintext
Property
├── Basic Information:
│ ├── property_url
│ ├── property_id
│ ├── listing_id
│ ├── mls
│ ├── mls_id
│ ├── mls_status
│ ├── status
│ └── permalink

├── Address Details (Pydantic/Raw):
│ ├── street
│ ├── unit
│ ├── city
│ ├── state
│ ├── zip_code
│ └── formatted_address*  # Computed field

├── Property Description:
│ ├── style
│ ├── beds
│ ├── full_baths
│ ├── half_baths
│ ├── sqft
│ ├── year_built
│ ├── stories
│ ├── garage
│ ├── lot_sqft
│ ├── text  # Full description text
│ └── type

├── Property Listing Details:
│ ├── days_on_mls
│ ├── list_price
│ ├── list_price_min
│ ├── list_price_max
│ ├── list_date  # datetime (full timestamp: YYYY-MM-DD HH:MM:SS)
│ ├── pending_date  # datetime (full timestamp: YYYY-MM-DD HH:MM:SS)
│ ├── sold_price
│ ├── last_sold_date  # datetime (full timestamp: YYYY-MM-DD HH:MM:SS)
│ ├── last_status_change_date  # datetime (full timestamp: YYYY-MM-DD HH:MM:SS)
│ ├── last_update_date  # datetime (full timestamp: YYYY-MM-DD HH:MM:SS)
│ ├── last_sold_price
│ ├── price_per_sqft
│ ├── new_construction
│ ├── hoa_fee
│ ├── monthly_fees  # List of fees
│ ├── one_time_fees  # List of fees
│ └── estimated_value

├── Price Changes:
│ ├── previous_list_price  # list price before the last change
│ ├── original_list_price  # price the current listing started at (extra_property_data)
│ ├── last_price_change_amount  # negative = price cut
│ ├── last_price_change_date
│ ├── price_reduced_amount
│ └── property_history  # listed / price changed / sold events (extra_property_data)

├── Costs and Area Stats (extra_property_data):
│ ├── insurance_rate  # yearly, share of price (0.0099 = 0.99%)
│ ├── monthly_home_insurance
│ ├── property_tax_rate
│ ├── neighborhood_median_rent
│ └── zip_median_rent

├── Tax Information:
│ ├── tax_assessed_value
│ └── tax_history  # List with years, amounts, assessments

├── Location Details:
│ ├── latitude
│ ├── longitude
│ ├── neighborhoods
│ ├── county
│ ├── fips_code
│ ├── parcel_number
│ └── nearby_schools

├── Agent/Broker/Office Info (Pydantic/Raw):
│ ├── agent_uuid
│ ├── agent_name  
│ ├── agent_email
│ ├── agent_phone
│ ├── agent_state_license
│ ├── broker_uuid
│ ├── broker_name
│ ├── office_uuid
│ ├── office_name
│ ├── office_email
│ ├── office_phones
│ ├── buyer_agent_id / buyer_agent_name / buyer_agent_email / buyer_agent_phones  # sold homes (extra_property_data)
│ ├── buyer_agent_mls_set / buyer_agent_nrds_id
│ ├── buyer_broker_name
│ └── buyer_office_name

├── Additional Fields (Pydantic/Raw only):
│ ├── estimated_monthly_rental
│ ├── tags  # Property tags/features
│ ├── flags  # Status flags (foreclosure, etc)
│ ├── photos  # All property photos
│ ├── primary_photo
│ ├── alt_photos
│ ├── open_houses  # List of open house events
│ ├── units  # For multi-family properties
│ ├── pet_policy
│ ├── parking
│ ├── terms  # Listing terms
│ ├── current_estimates  # Platform estimates with sources
│ └── estimates  # Historical estimates

* Only available when using return_type='pydantic'
```

### Exceptions and warnings
The following may be raised when using HomeHarvest:

- `InvalidListingType`: valid options are `for_sale`, `for_rent`, `sold`, `pending`, `off_market`, `new_community`, `other`, `ready_to_build`.
- `InvalidDate`: a date is not in ISO format (e.g. `2025-01-20` or `2025-01-20T14:30:00`), or the end is before the start.
- `AuthenticationError`: the data source answered 403 Forbidden on the first request (you may be blocked; wait, or use a proxy).
- `RateLimitError`: still rate limited (429) after retries.

`HomeHarvestWarning` (a `UserWarning`) is shown when results are incomplete, for example when a later page or some extra property details could not be fetched. You still get everything that was fetched. To turn warnings into errors:

```py
import warnings
from homeharvest import HomeHarvestWarning
warnings.simplefilter("error", HomeHarvestWarning)
```
