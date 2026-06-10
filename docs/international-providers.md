# International Providers

HomeHarvest currently defaults to `realtor.com`. International support should be added as provider adapters that normalize each source into the existing `Property` model.

## MVP Order

1. UK: Rightmove first, then Zoopla or OnTheMarket. Best public underwriting backstop is HM Land Registry Price Paid Data.
2. Australia: Domain first if API access is available, then realestate.com.au only with permission/licensed data.
3. Dubai/UAE: Bayut or Property Finder listings, paired with Dubai Land Department open transaction and rent data.
4. Spain: idealista first if API access is approved, then Fotocasa.
5. Germany: ImmoScout24 first, then immowelt.

## Provider Rules

- Keep `scrape_property()` backwards compatible.
- Add new portals behind the `site=` parameter.
- Normalize price with `currency` and location with `country`.
- Prefer official APIs or public open data where available.
- Keep live network tests out of CI; use fixtures for parsers and URL builders.
- Treat sold/transaction data as a separate underwriting input where portals do not expose it directly.

## Provider Registry

The package exposes `homeharvest.list_international_sources()` so Realist can inspect the supported and planned international sources without scraping docs.

Current registry:

| Market | Site | Status | Underwriting backstop |
| --- | --- | --- | --- |
| United Kingdom | `rightmove` | MVP | HM Land Registry Price Paid Data |
| Australia | `domain.com.au` | planned | state title and valuer-general datasets where available |
| Dubai / UAE | `bayut` | planned | Dubai Land Department transaction and rent data |
| Spain | `idealista` | planned | TBD / licensed data |
| Germany | `immoscout24` | planned | TBD / licensed data |

## First Target

Rightmove is the first adapter because the UK has both strong portal coverage and public sold-price data. The first implementation extracts embedded search-page JSON and maps it into `Property` with `source_site="rightmove"`, `country="GB"`, and `currency="GBP"`.

Known Rightmove city seeds:

- London: `REGION^87490`
- Manchester: `REGION^904`
- Birmingham: `REGION^162`
- Leeds: `REGION^787`
- Liverpool: `REGION^813`

You can pass either one of those city names or a raw Rightmove `locationIdentifier`.
