"""
Processors for realtor.com property data processing
"""

from datetime import datetime
from typing import Optional
from ..models import (
    Property,
    ListingType,
    Agent,
    Broker,
    Builder,
    Advertisers,
    Office,
    ReturnType
)
from .parsers import (
    parse_open_houses,
    parse_units,
    parse_tax_record,
    parse_current_estimates,
    parse_estimates,
    parse_neighborhoods,
    parse_address,
    parse_description,
    calculate_days_on_mls,
    process_alt_photos
)


def process_advertisers(advertisers: list[dict] | None) -> Advertisers | None:
    """Process advertisers data from GraphQL response"""
    if not advertisers:
        return None

    def _parse_fulfillment_id(fulfillment_id: str | None) -> str | None:
        return fulfillment_id if fulfillment_id and fulfillment_id != "0" else None

    processed_advertisers = Advertisers()

    for advertiser in advertisers:
        advertiser_type = advertiser.get("type")
        if advertiser_type == "seller":  #: agent
            processed_advertisers.agent = Agent(
                uuid=_parse_fulfillment_id(advertiser.get("fulfillment_id")),
                nrds_id=advertiser.get("nrds_id"),
                mls_set=advertiser.get("mls_set"),
                name=advertiser.get("name"),
                email=advertiser.get("email"),
                phones=advertiser.get("phones"),
                state_license=advertiser.get("state_license"),
            )

            if advertiser.get("broker") and advertiser["broker"].get("name"):  #: has a broker
                processed_advertisers.broker = Broker(
                    uuid=_parse_fulfillment_id(advertiser["broker"].get("fulfillment_id")),
                    name=advertiser["broker"].get("name"),
                )

            if advertiser.get("office"):  #: has an office
                processed_advertisers.office = Office(
                    uuid=_parse_fulfillment_id(advertiser["office"].get("fulfillment_id")),
                    mls_set=advertiser["office"].get("mls_set"),
                    name=advertiser["office"].get("name"),
                    email=advertiser["office"].get("email"),
                    phones=advertiser["office"].get("phones"),
                )

        if advertiser_type == "community":  #: could be builder
            if advertiser.get("builder"):
                processed_advertisers.builder = Builder(
                    uuid=_parse_fulfillment_id(advertiser["builder"].get("fulfillment_id")),
                    name=advertiser["builder"].get("name"),
                )

    return processed_advertisers


def process_property(result: dict, mls_only: bool = False, extra_property_data: bool = False, 
                    exclude_pending: bool = False, listing_type: ListingType = ListingType.FOR_SALE,
                    get_key_func=None, process_extra_property_details_func=None) -> Property | None:
    """Process property data from GraphQL response"""
    mls = result["source"].get("id") if "source" in result and isinstance(result["source"], dict) else None

    if not mls and mls_only:
        return None

    able_to_get_lat_long = (
        result
        and result.get("location")
        and result["location"].get("address")
        and result["location"]["address"].get("coordinate")
    )

    is_pending = result["flags"].get("is_pending")
    is_contingent = result["flags"].get("is_contingent")

    if (is_pending or is_contingent) and (exclude_pending and listing_type != ListingType.PENDING):
        return None

    property_id = result["property_id"]
    prop_details = process_extra_property_details_func(result) if extra_property_data and process_extra_property_details_func else {}

    property_estimates_root = result.get("current_estimates") or result.get("estimates", {}).get("currentValues")
    estimated_value = get_key_func(property_estimates_root, [0, "estimate"]) if get_key_func else None

    advertisers = process_advertisers(result.get("advertisers"))

    realty_property = Property(
        mls=mls,
        mls_id=(
            result["source"].get("listing_id")
            if "source" in result and isinstance(result["source"], dict)
            else None
        ),
        property_url=result["href"],
        property_id=property_id,
        listing_id=result.get("listing_id"),
        permalink=result.get("permalink"),
        status=("PENDING" if is_pending else "CONTINGENT" if is_contingent else result["status"].upper()),
        list_price=result["list_price"],
        list_price_min=result["list_price_min"],
        list_price_max=result["list_price_max"],
        list_date=(datetime.fromisoformat(result["list_date"].replace('Z', '+00:00') if result["list_date"].endswith('Z') else result["list_date"]) if result.get("list_date") else None),
        prc_sqft=result.get("price_per_sqft"),
        last_sold_date=(datetime.fromisoformat(result["last_sold_date"].replace('Z', '+00:00') if result["last_sold_date"].endswith('Z') else result["last_sold_date"]) if result.get("last_sold_date") else None),
        pending_date=(datetime.fromisoformat(result["pending_date"].replace('Z', '+00:00') if result["pending_date"].endswith('Z') else result["pending_date"]) if result.get("pending_date") else None),
        last_status_change_date=(datetime.fromisoformat(result["last_status_change_date"].replace('Z', '+00:00') if result["last_status_change_date"].endswith('Z') else result["last_status_change_date"]) if result.get("last_status_change_date") else None),
        last_update_date=(datetime.fromisoformat(result["last_update_date"].replace('Z', '+00:00') if result["last_update_date"].endswith('Z') else result["last_update_date"]) if result.get("last_update_date") else None),
        new_construction=result["flags"].get("is_new_construction") is True,
        hoa_fee=(result["hoa"]["fee"] if result.get("hoa") and isinstance(result["hoa"], dict) else None),
        latitude=(result["location"]["address"]["coordinate"].get("lat") if able_to_get_lat_long else None),
        longitude=(result["location"]["address"]["coordinate"].get("lon") if able_to_get_lat_long else None),
        address=parse_address(result, search_type="general_search"),
        description=parse_description(result),
        neighborhoods=parse_neighborhoods(result),
        county=(result["location"]["county"].get("name") if result["location"]["county"] else None),
        fips_code=(result["location"]["county"].get("fips_code") if result["location"]["county"] else None),
        days_on_mls=calculate_days_on_mls(result),
        nearby_schools=prop_details.get("schools"),
        assessed_value=prop_details.get("assessed_value"),
        estimated_value=estimated_value if estimated_value else None,
        advertisers=advertisers,
        tax=prop_details.get("tax"),
        tax_history=prop_details.get("tax_history"),
        
        # Additional fields from GraphQL
        mls_status=result.get("mls_status"),
        last_sold_price=result.get("last_sold_price"),
        tags=result.get("tags"),
        details=result.get("details"),
        open_houses=parse_open_houses(result.get("open_houses")),
        pet_policy=result.get("pet_policy"),
        units=parse_units(result.get("units")),
        monthly_fees=result.get("monthly_fees"),
        one_time_fees=result.get("one_time_fees"),
        parking=result.get("parking"),
        terms=result.get("terms"),
        popularity=result.get("popularity"),
        tax_record=parse_tax_record(result.get("tax_record")),
        parcel_info=result.get("location", {}).get("parcel"),
        current_estimates=parse_current_estimates(result.get("current_estimates")),
        estimates=parse_estimates(result.get("estimates")),
        photos=result.get("photos"),
        flags=result.get("flags"),
    )

    # Enhance date precision using last_status_change_date
    # pending_date and last_sold_date only have day-level precision
    # last_status_change_date has hour-level precision
    if realty_property.last_status_change_date:
        status = realty_property.status.upper() if realty_property.status else None

        # For PENDING/CONTINGENT properties, use last_status_change_date for hour-precision on pending_date
        if status in ["PENDING", "CONTINGENT"] and realty_property.pending_date:
            # Only replace if dates are on the same day
            if realty_property.pending_date.date() == realty_property.last_status_change_date.date():
                realty_property.pending_date = realty_property.last_status_change_date

        # For SOLD properties, use last_status_change_date for hour-precision on last_sold_date
        elif status == "SOLD" and realty_property.last_sold_date:
            # Only replace if dates are on the same day
            if realty_property.last_sold_date.date() == realty_property.last_status_change_date.date():
                realty_property.last_sold_date = realty_property.last_status_change_date

    return realty_property


def process_extra_property_details(result: dict, get_key_func=None) -> dict:
    """Process extra property details from GraphQL response"""
    if get_key_func:
        schools = get_key_func(result, ["nearbySchools", "schools"])
        assessed_value = get_key_func(result, ["taxHistory", 0, "assessment", "total"])
        tax_history = get_key_func(result, ["taxHistory"])
    else:
        nearby_schools = result.get("nearbySchools")
        schools = nearby_schools.get("schools", []) if nearby_schools else []
        tax_history_data = result.get("taxHistory", [])

        assessed_value = None
        if tax_history_data and tax_history_data[0] and tax_history_data[0].get("assessment"):
            assessed_value = tax_history_data[0]["assessment"].get("total")

        tax_history = tax_history_data

    if schools:
        schools = [school["district"]["name"] for school in schools if school["district"].get("name")]

    # Process tax history
    latest_tax = None
    processed_tax_history = None
    if tax_history and isinstance(tax_history, list):
        tax_history = sorted(tax_history, key=lambda x: x.get("year", 0), reverse=True)

        if tax_history and "tax" in tax_history[0]:
            latest_tax = tax_history[0]["tax"]

        processed_tax_history = []
        for entry in tax_history:
            if "year" in entry and "tax" in entry:
                processed_entry = {
                    "year": entry["year"],
                    "tax": entry["tax"],
                }
                if "assessment" in entry and isinstance(entry["assessment"], dict):
                    processed_entry["assessment"] = {
                        "building": entry["assessment"].get("building"),
                        "land": entry["assessment"].get("land"),
                        "total": entry["assessment"].get("total"),
                    }
                processed_tax_history.append(processed_entry)

    return {
        "schools": schools if schools else None,
        "assessed_value": assessed_value if assessed_value else None,
        "tax": latest_tax,
        "tax_history": processed_tax_history,
    }


def get_key(data: dict, keys: list):
    """Get nested key from dictionary safely"""
    try:
        value = data
        for key in keys:
            value = value[key]
        return value or {}
    except (KeyError, TypeError, IndexError):
        return {}