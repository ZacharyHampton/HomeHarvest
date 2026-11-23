"""
Processors for realtor.com property data processing
"""

from __future__ import annotations

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


def _parse_fulfillment_id(fulfillment_id: str | None) -> str | None:
    return fulfillment_id if fulfillment_id and fulfillment_id != "0" else None


def _to_int(value) -> int | None:
    """Round a numeric API value (often a float) to int, keeping None."""
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO date / datetime string from the API into a naive datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            return None


def process_buyers(buyers: list[dict] | None, advertisers: Advertisers | None) -> Advertisers | None:
    """Attach the buyer's agent (Home.buyers) to the Advertisers model."""
    if not buyers:
        return advertisers

    buyer = next((b for b in buyers if b and b.get("type") == "buyer"), None) or next((b for b in buyers if b), None)
    if not buyer:
        return advertisers

    advertisers = advertisers or Advertisers()
    advertisers.buyer_agent = Agent(
        uuid=_parse_fulfillment_id(buyer.get("fulfillment_id")),
        nrds_id=buyer.get("nrds_id"),
        mls_set=buyer.get("mls_set"),
        name=buyer.get("name"),
        email=buyer.get("email"),
        phones=buyer.get("phones"),
        state_license=buyer.get("state_license"),
    )
    if buyer.get("broker") and buyer["broker"].get("name"):
        advertisers.buyer_broker = Broker(
            uuid=_parse_fulfillment_id(buyer["broker"].get("fulfillment_id")),
            name=buyer["broker"].get("name"),
        )
    if buyer.get("office") and buyer["office"].get("name"):
        advertisers.buyer_office = Office(
            uuid=_parse_fulfillment_id(buyer["office"].get("fulfillment_id")),
            name=buyer["office"].get("name"),
        )
    return advertisers


def process_advertisers(advertisers: list[dict] | None) -> Advertisers | None:
    """Process advertisers data from GraphQL response"""
    if not advertisers:
        return None

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
    if extra_property_data:
        advertisers = process_buyers(result.get("buyers"), advertisers)

    list_price = result["list_price"]
    last_price_change_amount = _to_int(result.get("last_price_change_amount"))
    previous_list_price = (
        _to_int(list_price - last_price_change_amount)
        if list_price is not None and last_price_change_amount
        else None
    )

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

        # Price changes
        last_price_change_amount=last_price_change_amount,
        last_price_change_date=_parse_datetime(result.get("last_price_change_date")),
        price_reduced_amount=_to_int(result.get("price_reduced_amount")),
        previous_list_price=previous_list_price,
        original_list_price=prop_details.get("original_list_price"),
        property_history=prop_details.get("property_history"),

        # Area / cost estimates
        neighborhood_median_rent=prop_details.get("neighborhood_median_rent"),
        zip_median_rent=prop_details.get("zip_median_rent"),
        insurance_rate=prop_details.get("insurance_rate"),
        property_tax_rate=prop_details.get("property_tax_rate"),
        monthly_home_insurance=prop_details.get("monthly_home_insurance"),

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

    # Sold listings: realtor.com sometimes re-creates the listing record after the sale
    # (e.g. MLS feed migrations), so list_date becomes the record creation time and lands
    # *after* the sold date (issue #159). Recover the real list date from property history
    # when we have it, otherwise drop the misleading value.
    if (
        realty_property.status == "SOLD"
        and realty_property.list_date
        and realty_property.last_sold_date
        and realty_property.list_date.date() > realty_property.last_sold_date.date()
    ):
        recovered = prop_details.get("listing_start_date")
        if recovered and recovered.date() <= realty_property.last_sold_date.date():
            realty_property.list_date = recovered
            realty_property.days_on_mls = (realty_property.last_sold_date.date() - recovered.date()).days
        else:
            realty_property.list_date = None
            realty_property.days_on_mls = None

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

    listing_event = _find_listing_event(result)

    return {
        "schools": schools if schools else None,
        "assessed_value": assessed_value if assessed_value else None,
        "tax": latest_tax,
        "tax_history": processed_tax_history,
        "property_history": _process_property_history(result.get("property_history")),
        "original_list_price": _to_int(listing_event.get("price")) if listing_event and listing_event.get("price") else None,
        "listing_start_date": _parse_datetime(listing_event.get("date")) if listing_event else None,
        "neighborhood_median_rent": _neighborhood_median_rent(result),
        "zip_median_rent": _to_int(_housing_market(get_key(result, ["location", "postal_code"])).get("median_rent_price")),
        **_process_mortgage(result.get("mortgage")),
    }


def _process_property_history(history: list[dict] | None) -> list[dict] | None:
    if not history:
        return None
    events = []
    for event in history:
        if not event:
            continue
        events.append({
            "date": _parse_datetime(event.get("date")),
            "event_name": event.get("event_name"),
            "price": _to_int(event.get("price")),
            "price_change": _to_int(event.get("price_change")),
            "source_name": event.get("source_name"),
            "source_listing_id": event.get("source_listing_id"),
        })
    return events or None


def _find_listing_event(result: dict) -> dict | None:
    """Find the property_history event where the current listing went live.

    Prefers the earliest "Listed"/"Relisted" event that shares the current MLS listing id,
    otherwise the most recent listing event (before the sale, for sold homes).
    """
    history = [e for e in (result.get("property_history") or []) if e and e.get("date")]
    if not history:
        return None

    is_rental = (result.get("status") or "").lower() == "for_rent"

    def is_listing_event(name: str | None) -> bool:
        name = (name or "").lower()
        if not (name.startswith("listed") or name == "relisted"):
            return False
        return ("rent" in name) == is_rental

    candidates = [e for e in history if is_listing_event(e.get("event_name"))]

    sold_date = result.get("last_sold_date")
    if (result.get("status") or "").lower() == "sold" and sold_date:
        candidates = [e for e in candidates if e["date"][:10] <= sold_date[:10]]

    if not candidates:
        return None

    source = result.get("source") if isinstance(result.get("source"), dict) else {}
    mls_id = source.get("listing_id") if source else None
    same_listing = [e for e in candidates if mls_id and e.get("source_listing_id") == mls_id]
    if same_listing:
        return min(same_listing, key=lambda e: e["date"])

    return max(candidates, key=lambda e: e["date"])


def _housing_market(geo: dict | None) -> dict:
    if not geo or not isinstance(geo, dict):
        return {}
    return get_key(geo, ["geo_statistics", "housing_market"]) or {}


def _neighborhood_median_rent(result: dict) -> int | None:
    for neighborhood in get_key(result, ["location", "neighborhoods"]) or []:
        rent = _housing_market(neighborhood).get("median_rent_price")
        if rent:
            return _to_int(rent)
    return None


def _process_mortgage(mortgage: dict | None) -> dict:
    if not mortgage or not isinstance(mortgage, dict):
        return {"insurance_rate": None, "property_tax_rate": None, "monthly_home_insurance": None}

    monthly_home_insurance = None
    for detail in get_key(mortgage, ["estimate", "monthly_payment_details"]) or []:
        if detail and detail.get("type") == "home_insurance":
            monthly_home_insurance = _to_int(detail.get("amount"))

    return {
        "insurance_rate": mortgage.get("insurance_rate"),
        "property_tax_rate": mortgage.get("property_tax_rate"),
        "monthly_home_insurance": monthly_home_insurance,
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