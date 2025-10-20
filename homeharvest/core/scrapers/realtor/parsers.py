"""
Parsers for realtor.com data processing
"""

from datetime import datetime
from typing import Optional
from ..models import Address, Description, PropertyType


def parse_open_houses(open_houses_data: list[dict] | None) -> list[dict] | None:
    """Parse open houses data and convert date strings to datetime objects"""
    if not open_houses_data:
        return None
        
    parsed_open_houses = []
    for oh in open_houses_data:
        parsed_oh = oh.copy()
        
        # Parse start_date and end_date
        if parsed_oh.get("start_date"):
            try:
                parsed_oh["start_date"] = datetime.fromisoformat(parsed_oh["start_date"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                parsed_oh["start_date"] = None
                
        if parsed_oh.get("end_date"):
            try:
                parsed_oh["end_date"] = datetime.fromisoformat(parsed_oh["end_date"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                parsed_oh["end_date"] = None
                
        parsed_open_houses.append(parsed_oh)
        
    return parsed_open_houses


def parse_units(units_data: list[dict] | None) -> list[dict] | None:
    """Parse units data and convert date strings to datetime objects"""
    if not units_data:
        return None
        
    parsed_units = []
    for unit in units_data:
        parsed_unit = unit.copy()
        
        # Parse availability date
        if parsed_unit.get("availability") and parsed_unit["availability"].get("date"):
            try:
                parsed_unit["availability"]["date"] = datetime.fromisoformat(parsed_unit["availability"]["date"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                parsed_unit["availability"]["date"] = None
                
        parsed_units.append(parsed_unit)
        
    return parsed_units


def parse_tax_record(tax_record_data: dict | None) -> dict | None:
    """Parse tax record data and convert date strings to datetime objects"""
    if not tax_record_data:
        return None
        
    parsed_tax_record = tax_record_data.copy()
    
    # Parse last_update_date
    if parsed_tax_record.get("last_update_date"):
        try:
            parsed_tax_record["last_update_date"] = datetime.fromisoformat(parsed_tax_record["last_update_date"].replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            parsed_tax_record["last_update_date"] = None
            
    return parsed_tax_record


def parse_current_estimates(estimates_data: list[dict] | None) -> list[dict] | None:
    """Parse current estimates data and convert date strings to datetime objects"""
    if not estimates_data:
        return None
        
    parsed_estimates = []
    for estimate in estimates_data:
        parsed_estimate = estimate.copy()
        
        # Parse date
        if parsed_estimate.get("date"):
            try:
                parsed_estimate["date"] = datetime.fromisoformat(parsed_estimate["date"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                parsed_estimate["date"] = None
        
        # Parse source information
        if parsed_estimate.get("source"):
            source_data = parsed_estimate["source"]
            parsed_estimate["source"] = {
                "type": source_data.get("type"),
                "name": source_data.get("name")
            }
                
        parsed_estimates.append(parsed_estimate)
        
    return parsed_estimates


def parse_estimates(estimates_data: dict | None) -> dict | None:
    """Parse estimates data and convert date strings to datetime objects"""
    if not estimates_data:
        return None
        
    parsed_estimates = estimates_data.copy()
    
    # Parse current_values (which is aliased as currentValues in GraphQL)
    current_values = parsed_estimates.get("currentValues") or parsed_estimates.get("current_values")
    if current_values:
        parsed_current_values = []
        for estimate in current_values:
            parsed_estimate = estimate.copy()
            
            # Parse date
            if parsed_estimate.get("date"):
                try:
                    parsed_estimate["date"] = datetime.fromisoformat(parsed_estimate["date"].replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    parsed_estimate["date"] = None
            
            # Parse source information
            if parsed_estimate.get("source"):
                source_data = parsed_estimate["source"]
                parsed_estimate["source"] = {
                    "type": source_data.get("type"),
                    "name": source_data.get("name")
                }
            
            # Convert GraphQL aliases to Pydantic field names
            if "estimateHigh" in parsed_estimate:
                parsed_estimate["estimate_high"] = parsed_estimate.pop("estimateHigh")
            if "estimateLow" in parsed_estimate:
                parsed_estimate["estimate_low"] = parsed_estimate.pop("estimateLow")
            if "isBestHomeValue" in parsed_estimate:
                parsed_estimate["is_best_home_value"] = parsed_estimate.pop("isBestHomeValue")
                    
            parsed_current_values.append(parsed_estimate)
        
        parsed_estimates["current_values"] = parsed_current_values
        
        # Remove the GraphQL alias if it exists
        if "currentValues" in parsed_estimates:
            del parsed_estimates["currentValues"]
    
    return parsed_estimates


def parse_neighborhoods(result: dict) -> Optional[str]:
    """Parse neighborhoods from location data"""
    neighborhoods_list = []
    neighborhoods = result["location"].get("neighborhoods", [])

    if neighborhoods:
        for neighborhood in neighborhoods:
            name = neighborhood.get("name")
            if name:
                neighborhoods_list.append(name)

    return ", ".join(neighborhoods_list) if neighborhoods_list else None


def handle_none_safely(address_part):
    """Handle None values safely for address parts"""
    if address_part is None:
        return ""
    return address_part


def parse_address(result: dict, search_type: str) -> Address:
    """Parse address data from result"""
    if search_type == "general_search":
        address = result["location"]["address"]
    else:
        address = result["address"]

    return Address(
        full_line=address.get("line"),
        street=" ".join(
            part
            for part in [
                address.get("street_number"),
                address.get("street_direction"),
                address.get("street_name"),
                address.get("street_suffix"),
            ]
            if part is not None
        ).strip(),
        unit=address["unit"],
        city=address["city"],
        state=address["state_code"],
        zip=address["postal_code"],
        
        # Additional address fields
        street_direction=address.get("street_direction"),
        street_number=address.get("street_number"),
        street_name=address.get("street_name"),
        street_suffix=address.get("street_suffix"),
    )


def parse_description(result: dict) -> Description | None:
    """Parse description data from result"""
    if not result:
        return None

    description_data = result.get("description", {})

    if description_data is None or not isinstance(description_data, dict):
        description_data = {}

    style = description_data.get("type", "")
    if style is not None:
        style = style.upper()

    primary_photo = None
    if (primary_photo_info := result.get("primary_photo")) and (
        primary_photo_href := primary_photo_info.get("href")
    ):
        primary_photo = primary_photo_href.replace("s.jpg", "od-w480_h360_x2.webp?w=1080&q=75")

    return Description(
        primary_photo=primary_photo,
        alt_photos=process_alt_photos(result.get("photos", [])),
        style=(PropertyType.__getitem__(style) if style and style in PropertyType.__members__ else None),
        beds=description_data.get("beds"),
        baths_full=description_data.get("baths_full"),
        baths_half=description_data.get("baths_half"),
        sqft=description_data.get("sqft"),
        lot_sqft=description_data.get("lot_sqft"),
        sold_price=(
            result.get("last_sold_price") or description_data.get("sold_price")
            if result.get("last_sold_date") or result["list_price"] != description_data.get("sold_price")
            else None
        ),  #: has a sold date or list and sold price are different
        year_built=description_data.get("year_built"),
        garage=description_data.get("garage"),
        stories=description_data.get("stories"),
        text=description_data.get("text"),
        
        # Additional description fields
        name=description_data.get("name"),
        type=description_data.get("type"),
    )


def calculate_days_on_mls(result: dict) -> Optional[int]:
    """Calculate days on MLS from result data"""
    list_date_str = result.get("list_date")
    list_date = None
    if list_date_str:
        try:
            # Parse full datetime, then use date() for day calculation
            list_date_str_clean = list_date_str.replace('Z', '+00:00') if list_date_str.endswith('Z') else list_date_str
            list_date = datetime.fromisoformat(list_date_str_clean).replace(tzinfo=None)
        except (ValueError, AttributeError):
            # Fallback for date-only format
            list_date = datetime.strptime(list_date_str.split("T")[0], "%Y-%m-%d") if "T" in list_date_str else None

    last_sold_date_str = result.get("last_sold_date")
    last_sold_date = None
    if last_sold_date_str:
        try:
            last_sold_date_str_clean = last_sold_date_str.replace('Z', '+00:00') if last_sold_date_str.endswith('Z') else last_sold_date_str
            last_sold_date = datetime.fromisoformat(last_sold_date_str_clean).replace(tzinfo=None)
        except (ValueError, AttributeError):
            # Fallback for date-only format
            try:
                last_sold_date = datetime.strptime(last_sold_date_str, "%Y-%m-%d")
            except ValueError:
                last_sold_date = None
    today = datetime.now()

    if list_date:
        if result["status"] == "sold":
            if last_sold_date:
                days = (last_sold_date - list_date).days
                if days >= 0:
                    return days
        elif result["status"] in ("for_sale", "for_rent"):
            days = (today - list_date).days
            if days >= 0:
                return days


def process_alt_photos(photos_info: list[dict]) -> list[str] | None:
    """Process alternative photos from photos info"""
    if not photos_info:
        return None

    return [
        photo_info["href"].replace("s.jpg", "od-w480_h360_x2.webp?w=1080&q=75")
        for photo_info in photos_info
        if photo_info.get("href")
    ]