from __future__ import annotations
import pandas as pd
from datetime import datetime
from .core.scrapers.models import Property, ListingType, Advertisers
from .exceptions import InvalidListingType, InvalidDate

ordered_properties = [
    "property_url",
    "property_id",
    "listing_id",
    "permalink",
    "mls",
    "mls_id",
    "status",
    "mls_status",
    "text",
    "style",
    "formatted_address",
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
    "last_sold_price",
    "last_status_change_date",
    "assessed_value",
    "estimated_value",
    "tax",
    "tax_history",
    "new_construction",
    "lot_sqft",
    "price_per_sqft",
    "latitude",
    "longitude",
    "neighborhoods",
    "county",
    "fips_code",
    "stories",
    "hoa_fee",
    "parking_garage",
    "agent_id",
    "agent_name",
    "agent_email",
    "agent_phones",
    "agent_mls_set",
    "agent_nrds_id",
    "broker_id",
    "broker_name",
    "builder_id",
    "builder_name",
    "office_id",
    "office_mls_set",
    "office_name",
    "office_email",
    "office_phones",
    "nearby_schools",
    "primary_photo",
    "alt_photos"
]


def process_result(result: Property) -> pd.DataFrame:
    prop_data = {prop: None for prop in ordered_properties}
    prop_data.update(result.model_dump())

    if "address" in prop_data and prop_data["address"]:
        address_data = prop_data["address"]
        prop_data["full_street_line"] = address_data.get("full_line")
        prop_data["street"] = address_data.get("street")
        prop_data["unit"] = address_data.get("unit")
        prop_data["city"] = address_data.get("city")
        prop_data["state"] = address_data.get("state")
        prop_data["zip_code"] = address_data.get("zip")
        prop_data["formatted_address"] = address_data.get("formatted_address")

    if "advertisers" in prop_data and prop_data.get("advertisers"):
        advertiser_data = prop_data["advertisers"]
        if advertiser_data.get("agent"):
            agent_data = advertiser_data["agent"]
            prop_data["agent_id"] = agent_data.get("uuid")
            prop_data["agent_name"] = agent_data.get("name")
            prop_data["agent_email"] = agent_data.get("email")
            prop_data["agent_phones"] = agent_data.get("phones")
            prop_data["agent_mls_set"] = agent_data.get("mls_set")
            prop_data["agent_nrds_id"] = agent_data.get("nrds_id")

        if advertiser_data.get("broker"):
            broker_data = advertiser_data["broker"]
            prop_data["broker_id"] = broker_data.get("uuid")
            prop_data["broker_name"] = broker_data.get("name")

        if advertiser_data.get("builder"):
            builder_data = advertiser_data["builder"]
            prop_data["builder_id"] = builder_data.get("uuid")
            prop_data["builder_name"] = builder_data.get("name")

        if advertiser_data.get("office"):
            office_data = advertiser_data["office"]
            prop_data["office_id"] = office_data.get("uuid")
            prop_data["office_name"] = office_data.get("name")
            prop_data["office_email"] = office_data.get("email")
            prop_data["office_phones"] = office_data.get("phones")
            prop_data["office_mls_set"] = office_data.get("mls_set")

    prop_data["price_per_sqft"] = prop_data["prc_sqft"]
    prop_data["nearby_schools"] = filter(None, prop_data["nearby_schools"]) if prop_data["nearby_schools"] else None
    prop_data["nearby_schools"] = ", ".join(set(prop_data["nearby_schools"])) if prop_data["nearby_schools"] else None
    
    # Convert datetime objects to strings for CSV (preserve full datetime including time)
    for date_field in ["list_date", "pending_date", "last_sold_date", "last_status_change_date"]:
        if prop_data.get(date_field):
            prop_data[date_field] = prop_data[date_field].strftime("%Y-%m-%d %H:%M:%S") if hasattr(prop_data[date_field], 'strftime') else prop_data[date_field]
    
    # Convert HttpUrl objects to strings for CSV
    if prop_data.get("property_url"):
        prop_data["property_url"] = str(prop_data["property_url"])

    description = result.description
    if description:
        prop_data["primary_photo"] = str(description.primary_photo) if description.primary_photo else None
        prop_data["alt_photos"] = ", ".join(str(url) for url in description.alt_photos) if description.alt_photos else None
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


def validate_datetime(datetime_str: str | None) -> None:
    """Validate ISO 8601 datetime format."""
    if not datetime_str:
        return

    try:
        # Try parsing as ISO 8601 datetime
        datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        raise InvalidDate(
            f"Invalid datetime format: '{datetime_str}'. "
            f"Expected ISO 8601 format (e.g., '2025-01-20T14:30:00' or '2025-01-20')."
        )


def validate_filters(
    beds_min: int | None = None,
    beds_max: int | None = None,
    baths_min: float | None = None,
    baths_max: float | None = None,
    sqft_min: int | None = None,
    sqft_max: int | None = None,
    price_min: int | None = None,
    price_max: int | None = None,
    lot_sqft_min: int | None = None,
    lot_sqft_max: int | None = None,
    year_built_min: int | None = None,
    year_built_max: int | None = None,
) -> None:
    """Validate that min values are less than max values for range filters."""
    ranges = [
        ("beds", beds_min, beds_max),
        ("baths", baths_min, baths_max),
        ("sqft", sqft_min, sqft_max),
        ("price", price_min, price_max),
        ("lot_sqft", lot_sqft_min, lot_sqft_max),
        ("year_built", year_built_min, year_built_max),
    ]

    for name, min_val, max_val in ranges:
        if min_val is not None and max_val is not None and min_val > max_val:
            raise ValueError(f"{name}_min ({min_val}) cannot be greater than {name}_max ({max_val}).")


def validate_sort(sort_by: str | None, sort_direction: str | None = "desc") -> None:
    """Validate sort parameters."""
    valid_sort_fields = ["list_date", "sold_date", "list_price", "sqft", "beds", "baths"]
    valid_directions = ["asc", "desc"]

    if sort_by and sort_by not in valid_sort_fields:
        raise ValueError(
            f"Invalid sort_by value: '{sort_by}'. "
            f"Valid options: {', '.join(valid_sort_fields)}"
        )

    if sort_direction and sort_direction not in valid_directions:
        raise ValueError(
            f"Invalid sort_direction value: '{sort_direction}'. "
            f"Valid options: {', '.join(valid_directions)}"
        )
