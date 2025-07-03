from __future__ import annotations
import pandas as pd
from datetime import datetime
from .core.scrapers.models import Property, ListingType, Advertisers
from .exceptions import InvalidListingType, InvalidDate

ordered_properties = [
    "property_url",
    "property_id",
    "listing_id",
    "mls",
    "mls_id",
    "status",
    "text",
    "style",
    "full_street_line",
    "street",
    "unit",
    "city",
    "state",
    "zip_code",
    "beds",
    "full_baths",
    "half_baths",
    "sqft",
    "year_built",
    "days_on_mls",
    "list_price",
    "list_price_min",
    "list_price_max",
    "list_date",
    "pending_date",
    "sold_price",
    "last_sold_date",
    "assessed_value",
    "estimated_value",
    "estimate_high",
    "estimate_low",
    "estimate_source",
    "estimate_date",
    "tax",
    "tax_history",
    "apn",
    "public_record_id",
    "tax_record_last_update",
    "tax_parcel_id",
    "new_construction",
    "lot_sqft",
    "price_per_sqft",
    "latitude",
    "longitude",
    "street_number",
    "street_direction",
    "street_suffix",
    "neighborhoods",
    "county",
    "fips_code",
    "stories",
    "hoa_fee",
    "monthly_fees",
    "one_time_fees",
    "parking_garage",
    "parking_details",
    "property_tags",
    "property_details",
    "pet_friendly",
    "lease_terms",
    "unit_count",
    "available_units",
    "agent_id",
    "agent_name",
    "agent_email",
    "agent_phones",
    "agent_mls_set",
    "agent_nrds_id",
    "agent_href",
    "broker_id",
    "broker_name",
    "builder_id",
    "builder_name",
    "office_id",
    "office_mls_set",
    "office_name",
    "office_email",
    "office_phones",
    "office_href",
    "rental_management_name",
    "rental_management_href",
    "nearby_schools",
    "primary_photo",
    "alt_photos",
]


def process_result(result: Property) -> pd.DataFrame:
    prop_data = {prop: None for prop in ordered_properties}
    prop_data.update(result.__dict__)

    # Handle address
    if hasattr(result, "address") and result.address:
        address_data = result.address
        prop_data["full_street_line"] = address_data.full_line
        prop_data["street"] = address_data.street
        prop_data["unit"] = address_data.unit
        prop_data["city"] = address_data.city
        prop_data["state"] = address_data.state
        prop_data["zip_code"] = address_data.zip

    # Handle advertisers
    if hasattr(result, "advertisers") and result.advertisers:
        advertiser_data: Advertisers = result.advertisers
        if advertiser_data.agent:
            agent_data = advertiser_data.agent
            prop_data["agent_id"] = agent_data.uuid
            prop_data["agent_name"] = agent_data.name
            prop_data["agent_email"] = agent_data.email
            prop_data["agent_phones"] = agent_data.phones
            prop_data["agent_mls_set"] = agent_data.mls_set
            prop_data["agent_nrds_id"] = agent_data.nrds_id
            prop_data["agent_href"] = agent_data.href

        if advertiser_data.broker:
            broker_data = advertiser_data.broker
            prop_data["broker_id"] = broker_data.uuid
            prop_data["broker_name"] = broker_data.name

        if advertiser_data.builder:
            builder_data = advertiser_data.builder
            prop_data["builder_id"] = builder_data.uuid
            prop_data["builder_name"] = builder_data.name

        if advertiser_data.office:
            office_data = advertiser_data.office
            prop_data["office_id"] = office_data.uuid
            prop_data["office_name"] = office_data.name
            prop_data["office_email"] = office_data.email
            prop_data["office_phones"] = office_data.phones
            prop_data["office_mls_set"] = office_data.mls_set
            prop_data["office_href"] = office_data.href

    # Handle existing fields
    prop_data["price_per_sqft"] = prop_data["prc_sqft"]
    prop_data["nearby_schools"] = filter(None, prop_data["nearby_schools"]) if prop_data["nearby_schools"] else None
    prop_data["nearby_schools"] = ", ".join(set(prop_data["nearby_schools"])) if prop_data["nearby_schools"] else None

    # Handle description
    description = result.description
    if description:
        prop_data["primary_photo"] = description.primary_photo
        prop_data["alt_photos"] = ", ".join(description.alt_photos) if description.alt_photos else None
        prop_data["style"] = (
            description.style
            if isinstance(description.style, str)
            else description.style.value if description.style else None
        )
        prop_data["beds"] = description.beds
        prop_data["full_baths"] = description.baths_full
        prop_data["half_baths"] = description.baths_half
        prop_data["sqft"] = description.sqft
        prop_data["lot_sqft"] = description.lot_sqft
        prop_data["sold_price"] = description.sold_price
        prop_data["year_built"] = description.year_built
        prop_data["parking_garage"] = description.garage
        prop_data["stories"] = description.stories
        prop_data["text"] = description.text

    properties_df = pd.DataFrame([prop_data])
    properties_df = properties_df.reindex(columns=ordered_properties)

    return properties_df[ordered_properties]


def validate_input(listing_type: str) -> None:
    if listing_type.upper() not in ListingType.__members__:
        raise InvalidListingType(f"Provided listing type, '{listing_type}', does not exist.")


def validate_dates(date_from: str | None, date_to: str | None) -> None:
    if isinstance(date_from, str) != isinstance(date_to, str):
        raise InvalidDate("Both date_from and date_to must be provided.")

    if date_from and date_to:
        try:
            date_from_obj = datetime.strptime(date_from, "%Y-%m-%d")
            date_to_obj = datetime.strptime(date_to, "%Y-%m-%d")

            if date_to_obj < date_from_obj:
                raise InvalidDate("date_to must be after date_from.")
        except ValueError:
            raise InvalidDate(f"Invalid date format or range")


def validate_limit(limit: int) -> None:
    #: 1 -> 10000 limit

    if limit is not None and (limit < 1 or limit > 10000):
        raise ValueError("Property limit must be between 1 and 10,000.")
    