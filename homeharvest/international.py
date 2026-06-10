from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class InternationalSource:
    market: str
    country: str
    primary_site: str
    site: str
    status: str
    transaction_data: str | None = None
    notes: str | None = None


INTERNATIONAL_SOURCES: tuple[InternationalSource, ...] = (
    InternationalSource(
        market="United Kingdom",
        country="GB",
        primary_site="Rightmove",
        site="rightmove",
        status="mvp",
        transaction_data="HM Land Registry Price Paid Data",
        notes="First live adapter. Strong fit for comp-backed underwriting.",
    ),
    InternationalSource(
        market="Australia",
        country="AU",
        primary_site="Domain",
        site="domain.com.au",
        status="planned",
        transaction_data="state land title and valuer-general datasets where available",
        notes="Use official/licensed access before scraping realestate.com.au.",
    ),
    InternationalSource(
        market="Dubai / UAE",
        country="AE",
        primary_site="Bayut",
        site="bayut",
        status="planned",
        transaction_data="Dubai Land Department open transaction and rent data",
        notes="Good underwriting market because listing data can be paired with DLD transactions.",
    ),
    InternationalSource(
        market="Spain",
        country="ES",
        primary_site="idealista",
        site="idealista",
        status="planned",
        transaction_data=None,
        notes="Large portal coverage, but API/licensing should be checked before live scraping.",
    ),
    InternationalSource(
        market="Germany",
        country="DE",
        primary_site="ImmoScout24",
        site="immoscout24",
        status="planned",
        transaction_data=None,
        notes="High listing volume, stricter access constraints. Build after UK/AU/UAE.",
    ),
)


def list_international_sources() -> list[dict]:
    return [asdict(source) for source in INTERNATIONAL_SOURCES]
