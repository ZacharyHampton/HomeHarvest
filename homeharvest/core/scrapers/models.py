from dataclasses import dataclass
from enum import Enum
from typing import Optional


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
    CONDO_TOWNHOME = "CONDO_TOWNHOME"
    CONDO_TOWNHOME_ROWHOME_COOP = "CONDO_TOWNHOME_ROWHOME_COOP"
    CONDO = "CONDO"
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


@dataclass
class Property:
    property_url: str
    mls: str | None = None
    mls_id: str | None = None
    status: str | None = None
    address: Address | None = None

    list_price: int | None = None
    list_date: str | None = None
    pending_date: str | None = None
    last_sold_date: str | None = None
    prc_sqft: int | None = None
    hoa_fee: int | None = None
    days_on_mls: int | None = None
    description: Description | None = None

    latitude: float | None = None
    longitude: float | None = None
    neighborhoods: Optional[str] = None

    agents: list[Agent] = None
    nearby_schools: list[str] = None
