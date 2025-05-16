import os
import sys
import logging
from datetime import datetime, timedelta
from homeharvest import scrape_property
import pandas as pd
from multiprocessing import Pool, cpu_count
import argparse

# List of all US states (abbreviations)
US_STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

LISTING_TYPES = ['sold', 'for_sale', 'for_rent', 'pending']
DAYS_PER_CHUNK = 30
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime.now()
OUTPUT_DIR = 'state_exports'
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    filename='fetch_all_states.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch real estate data for US states using HomeHarvest.")
    parser.add_argument('--states', nargs='+', default=US_STATES, help='List of state abbreviations to fetch (default: all states)')
    parser.add_argument('--listing_types', nargs='+', default=LISTING_TYPES, help='Listing types to fetch (default: all types)')
    parser.add_argument('--start_date', type=str, default='2024-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, default=datetime.now().strftime('%Y-%m-%d'), help='End date (YYYY-MM-DD)')
    parser.add_argument('--output_dir', type=str, default='state_exports', help='Output directory')
    parser.add_argument('--max_rows', type=int, default=10000, help='Max properties per state')
    parser.add_argument('--output_format', choices=['csv', 'excel'], default='csv', help='Output file format')
    parser.add_argument('--processes', type=int, default=1, help='Number of parallel processes')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
    return parser.parse_args()

def fetch_state_10000(state, listing_type):
    """
    Fetch up to 10,000 properties for a state and listing type, and return as DataFrame.
    """
    try:
        properties = scrape_property(
            location=state,
            listing_type=listing_type,
            date_from=START_DATE.strftime('%Y-%m-%d'),
            date_to=END_DATE.strftime('%Y-%m-%d'),
            limit=10000,
            extra_property_data=True  # Fetch extra details for each property
        )
        if len(properties) > 0:
            return properties
        else:
            logging.info(f"No properties found for {state} {listing_type}")
            return None
    except Exception as e:
        logging.error(f"Error fetching {state} {listing_type}: {e}")
        return None

def fetch_state_custom(state, listing_types, start_date, end_date, max_rows):
    all_chunks = []
    for listing_type in listing_types:
        try:
            properties = scrape_property(
                location=state,
                listing_type=listing_type,
                date_from=start_date,
                date_to=end_date,
                limit=max_rows,
                extra_property_data=True
            )
            if len(properties) > 0:
                all_chunks.append(properties)
        except Exception as e:
            logging.error(f"Error fetching {state} {listing_type}: {e}")
    if all_chunks:
        df_state = pd.concat(all_chunks, ignore_index=True)
        return df_state.head(max_rows)
    return None

def process_state_cli(args_tuple):
    state, listing_types, start_date, end_date, max_rows, output_dir, output_format, overwrite, columns_map = args_tuple
    filename = os.path.join(output_dir, f"{state}_properties.{ 'xlsx' if output_format == 'excel' else 'csv' }")
    if os.path.exists(filename) and not overwrite:
        logging.info(f"Skipping {filename}, already exists.")
        print(f"Skipping {filename}, already exists.")
        return
    df_state = fetch_state_custom(state, listing_types, start_date, end_date, max_rows)
    if df_state is not None:
        available_cols = [col for col in columns_map.keys() if col in df_state.columns]
        df_export = df_state[available_cols].rename(columns={k: v for k, v in columns_map.items() if k in available_cols})
        if output_format == 'excel':
            df_export.to_excel(filename, index=False)
        else:
            df_export.to_csv(filename, index=False)
        logging.info(f"Saved {len(df_export)} properties for {state} to {filename}")
        print(f"Saved {len(df_export)} properties for {state} to {filename}")
    else:
        logging.info(f"No data for {state}")
        print(f"No data for {state}")

def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    columns_map = {
        # 1. Price
        'list_price': 'Listing Price',
        # 2. Images
        'photos': 'Images',
        'virtual_tour_url': 'Virtual Tour',
        # 3. Property Details
        'beds': 'Bedrooms',
        'full_baths': 'Full Baths',
        'half_baths': 'Half Baths',
        'sqft': 'Square Footage',
        'year_built': 'Year Built',
        'lot_sqft': 'Lot Size',
        'stories': 'Stories',
        # 4. Pricing Metrics
        'price_per_sqft': 'Price per Sqft',
        'estimated_value': 'Estimated Market Value',
        'price_history': 'Price History',
        # 5. Financial & Fees
        'hoa_fee': 'Monthly HOA Fee',
        'monthly_cost': 'Monthly Cost Calculator',
        'estimated_monthly_payment': 'Estimated Monthly Payments',
        # 6. Property Description & Highlights
        'description': "What's Special About This Property",
        'features': 'Features & Upgrades',
        'special_features': 'Unique Selling Points',
        # 7. Multimedia & Virtual Tours
        'tour_3d_url': '3D Tour',
        'video_tour_url': 'Video Tour',
        # 8. Source & Listing Data
        'agent_name': 'Listed By',
        'mls_id': 'MLS Number',
        'mls': 'Originating MLS',
        # 9. Legal & Tax Information
        'tax_history': 'Public Tax History',
        'tax': 'Tax Assessed Value',
        # 10. Facts & Features
        'property_type': 'Property Type',
        'interior_features': 'Interior Features',
        'exterior_features': 'Exterior Features',
        # 11. Historical Data
        'sale_history': 'Sale History',
        # 12. Environmental & Climate Risk
        'climate_risk': 'Climate Risk',
        # 13. Getting Around
        'commute': 'Getting Around',
        # 14. Nearby Amenities & Education
        'nearby_schools': 'Nearby Schools',
        'nearby_cities': 'Nearby Cities',
        'parks': 'Parks & Recreation',
    }
    tasks = [
        (state, args.listing_types, args.start_date, args.end_date, args.max_rows, args.output_dir, args.output_format, args.overwrite, columns_map)
        for state in args.states
    ]
    if args.processes > 1:
        with Pool(args.processes) as pool:
            pool.map(process_state_cli, tasks)
    else:
        for t in tasks:
            process_state_cli(t)
    print("Done. See fetch_all_states.log for details.")

if __name__ == "__main__":
    main()
