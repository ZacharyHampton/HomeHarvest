from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ReturnType(Enum):
    pydantic = "pydantic"
    pandas = "pandas"
    raw = "raw"


class SiteName(Enum):
    ZILLOW = "zillow"
    REDFIN = "redfin"
    REALTOR = "realtor.com"

    @classmethod
    def get_by_value(cls, value):
        for item in cls:
            if item.value == value:
                return item
        raise ValueError(f"{value} not found in {cls}")


class SearchPropertyType(Enum):
    SINGLE_FAMILY = "single_family"
    APARTMENT = "apartment"
    CONDOS = "condos"
    CONDO_TOWNHOME_ROWHOME_COOP = "condo_townhome_rowhome_coop"
    CONDO_TOWNHOME = "condo_townhome"
    TOWNHOMES = "townhomes"
    DUPLEX_TRIPLEX = "duplex_triplex"
    FARM = "farm"
    LAND = "land"
    MULTI_FAMILY = "multi_family"
    MOBILE = "mobile"


class ListingType(Enum):
    FOR_SALE = "FOR_SALE"
    FOR_RENT = "FOR_RENT"
    PENDING = "PENDING"
    SOLD = "SOLD"


@dataclass
class Agent:
    name: str | None = None
    phone: str | None = None


class PropertyType(Enum):
    APARTMENT = "APARTMENT"
    BUILDING = "BUILDING"
    COMMERCIAL = "COMMERCIAL"
    GOVERNMENT = "GOVERNMENT"
    INDUSTRIAL = "INDUSTRIAL"
    CONDO_TOWNHOME = "CONDO_TOWNHOME"
    CONDO_TOWNHOME_ROWHOME_COOP = "CONDO_TOWNHOME_ROWHOME_COOP"
    CONDO = "CONDO"
    CONDOP = "CONDOP"
    CONDOS = "CONDOS"
    COOP = "COOP"
    DUPLEX_TRIPLEX = "DUPLEX_TRIPLEX"
    FARM = "FARM"
    INVESTMENT = "INVESTMENT"
    LAND = "LAND"
    MOBILE = "MOBILE"
    MULTI_FAMILY = "MULTI_FAMILY"
    RENTAL = "RENTAL"
    SINGLE_FAMILY = "SINGLE_FAMILY"
    TOWNHOMES = "TOWNHOMES"
    OTHER = "OTHER"


@dataclass
class Address:
    full_line: str | None = None
    street: str | None = None
    unit: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None


@dataclass
class Description:
    primary_photo: str | None = None
    alt_photos: list[str] | None = None
    style: PropertyType | None = None
    beds: int | None = None
    baths_full: int | None = None
    baths_half: int | None = None
    sqft: int | None = None
    lot_sqft: int | None = None
    sold_price: int | None = None
    year_built: int | None = None
    garage: float | None = None
    stories: int | None = None
    text: str | None = None


@dataclass
class AgentPhone:  #: For documentation purposes only (at the moment)
    number: str | None = None
    type: str | None = None
    primary: bool | None = None
    ext: str | None = None


@dataclass
class Entity:
    name: str
    uuid: str | None = None


@dataclass
class Agent(Entity):
    mls_set: str | None = None
    nrds_id: str | None = None
    phones: list[dict] | AgentPhone | None = None
    email: str | None = None
    href: str | None = None


@dataclass
class Office(Entity):
    mls_set: str | None = None
    email: str | None = None
    href: str | None = None
    phones: list[dict] | AgentPhone | None = None


@dataclass
class Broker(Entity):
    pass


@dataclass
class Builder(Entity):
    pass


@dataclass
class Advertisers:
    agent: Agent | None = None
    broker: Broker | None = None
    builder: Builder | None = None
    office: Office | None = None


@dataclass
class Property:
    property_url: str
    property_id: str

    listing_id: str | None = None
    
    mls: str | None = None
    mls_id: str | None = None
    status: str | None = None
    address: Address | None = None

    # Enhanced address components (new fields)
    street_number: str | None = None
    street_direction: str | None = None
    street_suffix: str | None = None

    list_price: int | None = None
    list_price_min: int | None = None
    list_price_max: int | None = None

    list_date: str | None = None
    pending_date: str | None = None
    last_sold_date: str | None = None
    prc_sqft: int | None = None
    new_construction: bool | None = None
    hoa_fee: int | None = None
    days_on_mls: int | None = None
    description: Description | None = None
    tags: list[str] | None = None
    property_tags: str | None = None
    property_details: str | None = None
    details: list[dict] | None = None

    latitude: float | None = None
    longitude: float | None = None
    neighborhoods: Optional[str] = None
    county: Optional[str] = None
    fips_code: Optional[str] = None
    nearby_schools: list[str] = None
    assessed_value: int | None = None
    estimated_value: int | None = None
    
    # Enhanced estimate data (new fields)
    estimate_high: int | None = None
    estimate_low: int | None = None
    estimate_source: str | None = None
    estimate_date: str | None = None
    
    tax: int | None = None
    tax_history: list[dict] | None = None
    
    # Tax record details (new fields)
    apn: str | None = None
    public_record_id: str | None = None
    tax_parcel_id: str | None = None
    tax_record_last_update: str | None = None
    
    # Property fees (new fields)
    monthly_fees: str | None = None
    one_time_fees: str | None = None
    parking_details: str | None = None
    
    # Rental-specific info (new fields)
    pet_friendly: str | None = None
    lease_terms: str | None = None
    unit_count: int | None = None
    available_units: int | None = None
    
    # Rental management (new fields)
    rental_management_name: str | None = None
    rental_management_href: str | None = None

    advertisers: Advertisers | None = None
