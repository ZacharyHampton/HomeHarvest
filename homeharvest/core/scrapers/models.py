from __future__ import annotations
from enum import Enum
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, computed_field, HttpUrl, Field


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
    OFF_MARKET = "OFF_MARKET"
    NEW_COMMUNITY = "NEW_COMMUNITY"
    OTHER = "OTHER"
    READY_TO_BUILD = "READY_TO_BUILD"


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


class Address(BaseModel):
    full_line: str | None = None
    street: str | None = None
    unit: str | None = None
    city: str | None = Field(None, description="The name of the city")
    state: str | None = Field(None, description="The name of the state")
    zip: str | None = Field(None, description="zip code")
    
    # Additional address fields from GraphQL
    street_direction: str | None = None
    street_number: str | None = None
    street_name: str | None = None
    street_suffix: str | None = None
    
    @computed_field
    @property
    def formatted_address(self) -> str | None:
        """Computed property that combines full_line, city, state, and zip into a formatted address."""
        parts = []
        
        if self.full_line:
            parts.append(self.full_line)
        
        city_state_zip = []
        if self.city:
            city_state_zip.append(self.city)
        if self.state:
            city_state_zip.append(self.state)
        if self.zip:
            city_state_zip.append(self.zip)
        
        if city_state_zip:
            parts.append(", ".join(city_state_zip))
        
        return ", ".join(parts) if parts else None




class Description(BaseModel):
    primary_photo: HttpUrl | None = None
    alt_photos: list[HttpUrl] | None = None
    style: PropertyType | None = None
    beds: int | None = Field(None, description="Total number of bedrooms")
    baths_full: int | None = Field(None, description="Total number of full bathrooms (4 parts: Sink, Shower, Bathtub and Toilet)")
    baths_half: int | None = Field(None, description="Total number of 1/2 bathrooms (2 parts: Usually Sink and Toilet)")
    sqft: int | None = Field(None, description="Square footage of the Home")
    lot_sqft: int | None = Field(None, description="Lot square footage")
    sold_price: int | None = Field(None, description="Sold price of home")
    year_built: int | None = Field(None, description="The year the building/home was built")
    garage: float | None = Field(None, description="Number of garage spaces")
    stories: int | None = Field(None, description="Number of stories in the building")
    text: str | None = None
    
    # Additional description fields
    name: str | None = None
    type: str | None = None


class AgentPhone(BaseModel):
    number: str | None = None
    type: str | None = None
    primary: bool | None = None
    ext: str | None = None


class Entity(BaseModel):
    name: str | None = None  # Make name optional since it can be None
    uuid: str | None = None


class Agent(Entity):
    mls_set: str | None = None
    nrds_id: str | None = None
    phones: list[dict] | AgentPhone | None = None
    email: str | None = None
    href: str | None = None
    state_license: str | None = Field(None, description="Advertiser agent state license number")


class Office(Entity):
    mls_set: str | None = None
    email: str | None = None
    href: str | None = None
    phones: list[dict] | AgentPhone | None = None


class Broker(Entity):
    pass


class Builder(Entity):
    pass


class Advertisers(BaseModel):
    agent: Agent | None = None
    broker: Broker | None = None
    builder: Builder | None = None
    office: Office | None = None


class Property(BaseModel):
    property_url: HttpUrl
    property_id: str = Field(..., description="Unique Home identifier also known as property id")
    #: allows_cats: bool
    #: allows_dogs: bool

    listing_id: str | None = None
    permalink: str | None = None

    mls: str | None = None
    mls_id: str | None = None
    status: str | None = Field(None, description="Listing status: for_sale, for_rent, sold, off_market, active (New Home Subdivisions), other (if none of the above conditions were met)")
    address: Address | None = None

    list_price: int | None = Field(None, description="The current price of the Home")
    list_price_min: int | None = None
    list_price_max: int | None = None

    list_date: datetime | None = Field(None, description="The time this Home entered Move system")
    pending_date: datetime | None = Field(None, description="The date listing went into pending state")
    last_sold_date: datetime | None = Field(None, description="Last time the Home was sold")
    last_status_change_date: datetime | None = Field(None, description="Last time the status of the listing changed")
    last_update_date: datetime | None = Field(None, description="Last time the home was updated")
    prc_sqft: int | None = None
    new_construction: bool | None = Field(None, description="Search for new construction homes")
    hoa_fee: int | None = Field(None, description="Search for homes where HOA fee is known and falls within specified range")
    days_on_mls: int | None = Field(None, description="An integer value determined by the MLS to calculate days on market")
    description: Description | None = None
    tags: list[str] | None = None
    details: list[HomeDetails] | None = None

    latitude: float | None = None
    longitude: float | None = None
    neighborhoods: Optional[str] = None
    county: Optional[str] = Field(None, description="County associated with home")
    fips_code: Optional[str] = Field(None, description="The FIPS (Federal Information Processing Standard) code for the county")
    nearby_schools: list[str] | None = None
    assessed_value: int | None = None
    estimated_value: int | None = None
    tax: int | None = None
    tax_history: list[TaxHistory] | None = None

    advertisers: Advertisers | None = None
    
    # Additional fields from GraphQL that aren't currently parsed
    mls_status: str | None = None
    last_sold_price: int | None = None
    
    # Structured data from GraphQL
    open_houses: list[OpenHouse] | None = None
    pet_policy: PetPolicy | None = None
    units: list[Unit] | None = None
    monthly_fees: HomeMonthlyFee | None = Field(None, description="Monthly fees. Currently only some rental data will have them.")
    one_time_fees: list[HomeOneTimeFee] | None = Field(None, description="One time fees. Currently only some rental data will have them.")
    parking: HomeParkingDetails | None = Field(None, description="Parking information. Currently only some rental data will have it.")
    terms: list[PropertyDetails] | None = None
    popularity: Popularity | None = None
    tax_record: TaxRecord | None = None
    parcel_info: dict | None = None  # Keep as dict for flexibility
    current_estimates: list[PropertyEstimate] | None = None
    estimates: HomeEstimates | None = None
    photos: list[dict] | None = None  # Keep as dict for photo structure
    flags: HomeFlags | None = Field(None, description="Home flags for Listing/Property")


# Specialized models for GraphQL types

class HomeMonthlyFee(BaseModel):
    description: str | None = None
    display_amount: str | None = None


class HomeOneTimeFee(BaseModel):
    description: str | None = None
    display_amount: str | None = None


class HomeParkingDetails(BaseModel):
    unassigned_space_rent: int | None = None
    assigned_spaces_available: int | None = None
    description: str | None = Field(None, description="Parking information. Currently only some rental data will have it.")
    assigned_space_rent: int | None = None


class PetPolicy(BaseModel):
    cats: bool | None = Field(None, description="Search for homes which allow cats")
    dogs: bool | None = Field(None, description="Search for homes which allow dogs")
    dogs_small: bool | None = Field(None, description="Search for homes with allow small dogs")
    dogs_large: bool | None = Field(None, description="Search for homes which allow large dogs")


class OpenHouse(BaseModel):
    start_date: datetime | None = None
    end_date: datetime | None = None
    description: str | None = None
    time_zone: str | None = None
    dst: bool | None = None
    href: HttpUrl | None = None
    methods: list[str] | None = None


class HomeFlags(BaseModel):
    is_pending: bool | None = None
    is_contingent: bool | None = None
    is_new_construction: bool | None = None
    is_coming_soon: bool | None = None
    is_new_listing: bool | None = None
    is_price_reduced: bool | None = None
    is_foreclosure: bool | None = None


class PopularityPeriod(BaseModel):
    clicks_total: int | None = None
    views_total: int | None = None
    dwell_time_mean: float | None = None
    dwell_time_median: float | None = None
    leads_total: int | None = None
    shares_total: int | None = None
    saves_total: int | None = None
    last_n_days: int | None = None


class Popularity(BaseModel):
    periods: list[PopularityPeriod] | None = None


class Assessment(BaseModel):
    building: int | None = None
    land: int | None = None
    total: int | None = None


class TaxHistory(BaseModel):
    assessment: Assessment | None = None
    market: Assessment | None = Field(None, description="Market values as provided by the county or local taxing/assessment authority")
    appraisal: Assessment | None = Field(None, description="Appraised value given by taxing authority")
    value: Assessment | None = Field(None, description="Value closest to current market value used for assessment by county or local taxing authorities")
    tax: int | None = None
    year: int | None = None
    assessed_year: int | None = Field(None, description="Assessment year for which taxes were billed")


class TaxRecord(BaseModel):
    cl_id: str | None = None
    public_record_id: str | None = None
    last_update_date: datetime | None = None
    apn: str | None = None
    tax_parcel_id: str | None = None


class EstimateSource(BaseModel):
    type: str | None = Field(None, description="Type of the avm vendor, list of values: corelogic, collateral, quantarium")
    name: str | None = Field(None, description="Name of the avm vendor")


class PropertyEstimate(BaseModel):
    estimate: int | None = Field(None, description="Estimated value of a property")
    estimate_high: int | None = Field(None, description="Estimated high value of a property")
    estimate_low: int | None = Field(None, description="Estimated low value of a property")
    date: datetime | None = Field(None, description="Date of estimation")
    is_best_home_value: bool | None = None
    source: EstimateSource | None = Field(None, description="Source of the latest estimate value")


class HomeEstimates(BaseModel):
    current_values: list[PropertyEstimate] | None = Field(None, description="Current valuation and best value for home from multiple AVM vendors")


class PropertyDetails(BaseModel):
    category: str | None = None
    text: list[str] | None = None
    parent_category: str | None = None


class HomeDetails(BaseModel):
    category: str | None = None
    text: list[str] | None = None
    parent_category: str | None = None


class UnitDescription(BaseModel):
    baths_consolidated: str | None = None
    baths: float | None = None  # Changed to float to handle values like 2.5
    beds: int | None = None
    sqft: int | None = None


class UnitAvailability(BaseModel):
    date: datetime | None = None


class Unit(BaseModel):
    availability: UnitAvailability | None = None
    description: UnitDescription | None = None
    photos: list[dict] | None = None  # Keep as dict for photo structure
    list_price: int | None = None
