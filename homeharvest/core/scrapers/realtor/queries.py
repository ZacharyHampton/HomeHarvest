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
fragment HomeData on Home {
    property_id
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
    location {
        parcel {
            parcel_id
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
}
"""

HOMES_DATA = """%s
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
}""" % _SEARCH_HOMES_DATA_BASE

SEARCH_HOMES_DATA = """%s
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

GENERAL_RESULTS_QUERY = """{
                            count
                            total
                            results %s
                        }""" % SEARCH_HOMES_DATA
