from homeharvest import scrape_property


properties = scrape_property(
    location="London",
    site="rightmove",
    listing_type="for_sale",
    beds_min=2,
    price_max=750000,
    limit=25,
)

properties.to_csv("rightmove_london_sample.csv", index=False)
print(properties[["source_site", "country", "currency", "formatted_address", "list_price", "beds", "sqft"]].head())
