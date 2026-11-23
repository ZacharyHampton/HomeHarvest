SEARCH_RESULTS_FRAGMENT = """
fragment PropertyResult on SearchHome {
    __typename
    pending_date
    listing_id
    property_id
    href
    permalink
    list_date
    status
    mls_status
    last_sold_price
    last_sold_date
    last_status_change_date
    last_update_date
    list_price
    list_price_max
    list_price_min
    price_per_sqft
    tags
    open_houses {
        start_date
        end_date
        description
        time_zone
        dst
        href
        methods
    }
    details {
        category
        text
        parent_category
    }
    pet_policy {
        cats
        dogs
        dogs_small
        dogs_large
        __typename
    }
    units {
        availability {
          date
          __typename
        }
        description {
          baths_consolidated
          baths
          beds
          sqft
          __typename
        }
        photos(https: true) {
            title
            href
            tags {
                label
            }
        }
        list_price
        __typename
    }
    flags {
        is_contingent
        is_pending
        is_new_construction
    }
    description {
        type
        sqft
        beds
        baths_full
        baths_half
        lot_sqft
        year_built
        garage
        type
        name
        stories
        text
    }
    source {
        id
        listing_id
    }
    hoa {
        fee
    }
    location {
        address {
            street_direction
            street_number
            street_name
            street_suffix
            line
            unit
            city
            state_code
            postal_code
            coordinate {
                lon
                lat
            }
        }
        county {
            name
            fips_code
        }
        neighborhoods {
            name
        }
    }
    tax_record {
        cl_id
        public_record_id
        last_update_date
        apn
        tax_parcel_id
    }
    primary_photo(https: true) {
        href
    }
    advertisers {
        email
        broker {
            name
            fulfillment_id
        }
        type
        name
        fulfillment_id
        builder {
            name
            fulfillment_id
        }
        phones {
            ext
            primary
            type
            number
        }
        office {
            name
            email
            fulfillment_id
            href
            phones {
                number
                type
                primary
                ext
            }
            mls_set
        }
        corporation {
            specialties
            name
            bio
            href
            fulfillment_id
        }
        mls_set
        nrds_id
        state_license
        rental_corporation {
            fulfillment_id
        }
        rental_management {
            name
            href
            fulfillment_id
        }
    }
    current_estimates {
        __typename
        source {
            __typename
            type
            name
        }
        estimate
        estimateHigh: estimate_high
        estimateLow: estimate_low
        date
        isBestHomeValue: isbest_homevalue
    }
}
"""

_SEARCH_HOMES_DATA_BASE = """{
    pending_date
    listing_id
    property_id
    href
    permalink
    list_date
    status
    mls_status
    last_sold_price
    last_sold_date
    last_status_change_date
    last_update_date
    list_price
    list_price_max
    list_price_min
    price_per_sqft
    last_price_change_amount
    last_price_change_date
    tags
    open_houses {
        start_date
        end_date
        description
        time_zone
        dst
        href
        methods
    }
    details {
        category
        text
        parent_category
    }
    pet_policy {
        cats
        dogs
        dogs_small
        dogs_large
        __typename
    }
    units {
        availability {
          date
          __typename
        }
        description {
          baths_consolidated
          baths
          beds
          sqft
          __typename
        }
        photos(https: true) {
            title
            href
            tags {
                label
            }
        }
        list_price
        __typename
    }
    flags {
        is_contingent
        is_pending
        is_new_construction
    }
    description {
        type
        sqft
        beds
        baths_full
        baths_half
        lot_sqft
        year_built
        garage
        type
        name
        stories
        text
    }
    source {
        id
        listing_id
    }
    hoa {
        fee
    }
    location {
        address {
            street_direction
            street_number
            street_name
            street_suffix
            line
            unit
            city
            state_code
            postal_code
            coordinate {
                lon
                lat
            }
        }
        county {
            name
            fips_code
        }
        neighborhoods {
            name
        }
    }
    tax_record {
        cl_id
        public_record_id
        last_update_date
        apn
        tax_parcel_id
    }
    primary_photo(https: true) {
        href
    }
    photos(https: true) {
        title
        href
        tags {
            label
        }
    }
    advertisers {
        email
        broker {
            name
            fulfillment_id
        }
        type
        name
        fulfillment_id
        builder {
            name
            fulfillment_id
        }
        phones {
            ext
            primary
            type
            number
        }
        office {
            name
            email
            fulfillment_id
            href
            phones {
                number
                type
                primary
                ext
            }
            mls_set
        }
        corporation {
            specialties
            name
            bio
            href
            fulfillment_id
        }
        mls_set
        nrds_id
        state_license
        rental_corporation {
            fulfillment_id
        }
        rental_management {
            name
            href
            fulfillment_id
        }
    }
    """


HOME_FRAGMENT = """
fragment PropertyResult on Home {
    __typename
    pending_date
    listing_id
    property_id
    href
    permalink
    list_date
    status
    mls_status
    last_sold_price
    last_sold_date
    last_status_change_date
    last_update_date
    list_price
    list_price_max
    list_price_min
    price_per_sqft
    tags
    open_houses {
        start_date
        end_date
        description
        time_zone
        dst
        href
        methods
    }
    details {
        category
        text
        parent_category
    }
    pet_policy {
        cats
        dogs
        dogs_small
        dogs_large
        __typename
    }
    units {
        availability {
          date
          __typename
        }
        description {
          baths_consolidated
          baths
          beds
          sqft
          __typename
        }
        photos(https: true) {
            title
            href
            tags {
                label
            }
        }
        list_price
        __typename
    }
    flags {
        is_contingent
        is_pending
        is_new_construction
    }
    description {
        type
        sqft
        beds
        baths_full
        baths_half
        lot_sqft
        year_built
        garage
        type
        name
        stories
        text
    }
    source {
        id
        listing_id
    }
    hoa {
        fee
    }
    location {
        address {
            street_direction
            street_number
            street_name
            street_suffix
            line
            unit
            city
            state_code
            postal_code
            coordinate {
                lon
                lat
            }
        }
        county {
            name
            fips_code
        }
        neighborhoods {
            name
        }
        parcel {
            parcel_id
        }
    }
    tax_record {
        cl_id
        public_record_id
        last_update_date
        apn
        tax_parcel_id
    }
    primary_photo(https: true) {
        href
    }
    photos(https: true) {
        title
        href
        tags {
            label
        }
    }
    advertisers {
        email
        broker {
            name
            fulfillment_id
        }
        type
        name
        fulfillment_id
        builder {
            name
            fulfillment_id
        }
        phones {
            ext
            primary
            type
            number
        }
        office {
            name
            email
            fulfillment_id
            href
            phones {
                number
                type
                primary
                ext
            }
            mls_set
        }
        corporation {
            specialties
            name
            bio
            href
            fulfillment_id
        }
        mls_set
        nrds_id
        state_license
        rental_corporation {
            fulfillment_id
        }
        rental_management {
            name
            href
            fulfillment_id
        }
    }
    nearbySchools: nearby_schools(radius: 5.0, limit_per_level: 3) {
        __typename schools { district { __typename id name } }
    }
    popularity {
        periods {
            clicks_total
            views_total
            dwell_time_mean
            dwell_time_median
            leads_total
            shares_total
            saves_total
            last_n_days
        }
    }
    taxHistory: tax_history { __typename tax year assessment { __typename building land total } }
    property_history {
        date
        event_name
        price
    }
    monthly_fees {
        description
        display_amount
    }
    one_time_fees {
        description
        display_amount
    }
    parking {
        unassigned_space_rent
        assigned_spaces_available
        description
        assigned_space_rent
    }
    terms {
        text
        category
    }
    estimates {
        __typename
        currentValues: current_values {
            __typename
            source { __typename type name }
            estimate
            estimateHigh: estimate_high
            estimateLow: estimate_low
            date
            isBestHomeValue: isbest_homevalue
        }
    }
}
"""


# Fields that only exist on the Home (detail) type. Fetched for extra_property_data
# (in batches, see RealtorScraper.get_bulk_prop_details) and for single-address lookups.
EXTRA_PROPERTY_FIELDS = """
    property_id
    nearbySchools: nearby_schools(radius: 5.0, limit_per_level: 3) {
        __typename schools { district { __typename id name } }
    }
    monthly_fees {
        description
        display_amount
    }
    one_time_fees {
        description
        display_amount
    }
    popularity {
        periods {
            clicks_total
            views_total
            dwell_time_mean
            dwell_time_median
            leads_total
            shares_total
            saves_total
            last_n_days
        }
    }
    location {
        parcel {
            parcel_id
        }
        neighborhoods {
            name
            geo_statistics(group_by: property_type) {
                housing_market {
                    median_rent_price
                    median_listing_price
                }
            }
        }
        postal_code {
            postal_code
            geo_statistics(group_by: property_type) {
                housing_market {
                    median_rent_price
                    median_listing_price
                }
            }
        }
    }
    parking {
        unassigned_space_rent
        assigned_spaces_available
        description
        assigned_space_rent
    }
    terms {
        text
        category
    }
    taxHistory: tax_history { __typename tax year assessment { __typename building land total } }
    estimates {
        __typename
        currentValues: current_values {
            __typename
            source { __typename type name }
            estimate
            estimateHigh: estimate_high
            estimateLow: estimate_low
            date
            isBestHomeValue: isbest_homevalue
        }
    }
    property_history {
        date
        event_name
        price
        price_change
        source_name
        source_listing_id
    }
    buyers {
        name
        email
        nrds_id
        fulfillment_id
        state_license
        mls_set
        type
        phones {
            number
            type
            primary
            ext
        }
        office {
            name
            fulfillment_id
        }
        broker {
            name
            fulfillment_id
        }
    }
    mortgage {
        insurance_rate
        property_tax_rate
        estimate {
            monthly_payment_details {
                type
                amount
            }
        }
    }
"""

#: Named fragment used by the batched detail query (keeps each request small).
EXTRA_PROPERTY_FRAGMENT = "fragment HomeExtraData on Home {%s}" % EXTRA_PROPERTY_FIELDS

HOMES_DATA = """%s
%s
}""" % (_SEARCH_HOMES_DATA_BASE, EXTRA_PROPERTY_FIELDS)

SEARCH_HOMES_DATA = """%s
price_reduced_amount
price_reduced_date
current_estimates {
    __typename
    source {
        __typename
        type
        name
    }
    estimate
    estimateHigh: estimate_high
    estimateLow: estimate_low
    date
    isBestHomeValue: isbest_homevalue
}
}""" % _SEARCH_HOMES_DATA_BASE

# Query body using inline fields (kept for backward compatibility)
GENERAL_RESULTS_QUERY_BODY = """{
                            count
                            total
                            results %s
                        }""" % SEARCH_HOMES_DATA

GENERAL_RESULTS_QUERY = """{
                            __typename
                            count
                            total
                            results %s
                        }""" % SEARCH_HOMES_DATA

LISTING_PHOTOS_FRAGMENT = """
fragment ListingPhotosFragment on SearchHome {
    __typename
    photos(https: true) {
        __typename
        title
        href
        tags {
            __typename
            label
            probability
        }
    }
}
"""

SEARCH_SUGGESTIONS_QUERY = """query Search_suggestions($searchInput: SearchSuggestionsInput!) {
  search_suggestions(search_input: $searchInput) {
    raw_input_parser_result
    typeahead_results {
      display_string
      display_geo
      geo {
        _id
        _score
        mpr_id
        area_type
        city
        state_code
        state
        postal_code
        country
        lat
        lon
        county
        counties {
          name
          fips
          state_code
        }
        slug_id
        geo_id
        score
        name
        city_slug_id
        centroid {
          lat
          lon
        }
        county_needed_for_uniq
        street
        line
        school
        school_id
        school_district
        has_catchment
        university
        university_id
        neighborhood
        park
      }
      url
    }
    geo_results {
      type
      text
      geo {
        _id
        _score
        mpr_id
        area_type
        city
        state_code
        state
        postal_code
        country
        lat
        lon
        county
        counties {
          name
          fips
          state_code
        }
        slug_id
        geo_id
        score
        name
        city_slug_id
        centroid {
          lat
          lon
        }
        county_needed_for_uniq
        street
        line
        school
        school_id
        school_district
        has_catchment
        university
        university_id
        neighborhood
        park
      }
    }
    no_matches
    has_results
    original_string
  }
}"""
