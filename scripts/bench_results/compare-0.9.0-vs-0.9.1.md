# 0.9.0 baseline (3 runs) vs 0.9.1 full matrix (3 runs)

A = v0.9.0-baseline (0.9.0, 2026-11-23 12:40:41)   B = 0.9.1-candidate (0.9.1 pre-release, 2026-11-23 13:21:43)

| scenario | rows A/B | time A -> B (median s) | requests A -> B | body MB A -> B | tainted A/B |
|---|---|---|---|---|---|
| dallas_sold_200 | 200/200 | 2.13 -> 2.27 (0.9x) | 2 -> 2 | 2.34 -> 2.25 | 0/0 |
| dallas_sold_200_extras | 200/200 | 11.72 -> 6.73 (1.7x) | 12 -> 12 | 3.77 -> 3.39 | 0/0 |
| phoenix_for_sale_200_extras | 200/200 | 13.58 -> 4.60 (3.0x) | 12 -> 12 | 4.11 -> 3.73 | 0/0 |
| phoenix_for_sale_1000 | 1000/1000 | 6.21 -> 4.41 (1.4x) | 6 -> 6 | 14.10 -> 13.74 | 0/0 |
| dallas_pending_7d_extras | 162/157 | 12.34 -> 6.93 (1.8x) | 14 -> 14 | 10.71 -> 10.14 | 0/0 |
| austin_open_house | 37/36 | 0.88 -> 0.76 (1.2x) | 2 -> 2 | 0.62 -> 0.57 | 0/0 |
| houston_for_sale_2000 | 2000/2000 | 12.90 -> 7.21 (1.8x) | 11 -> 11 | 25.46 -> 24.88 | 0/0 |


# 0.9.0 vs 0.9.1 extras re-check, back to back, same homes

A = v0.9.0-rerun2 (0.9.0, 2026-11-23 13:28:54)   B = 0.9.1-refetch (0.9.1, 2026-11-23 13:27:56)

| scenario | rows A/B | time A -> B (median s) | requests A -> B | body MB A -> B | tainted A/B |
|---|---|---|---|---|---|
| dallas_rent_200_extras | 200/200 | 12.52 -> 5.56 (2.3x) | 12 -> 12 | 6.03 -> 5.52 | 0/0 |
| dallas_sold_200_extras | 200/200 | 10.10 -> 4.78 (2.1x) | 12 -> 12 | 3.78 -> 3.38 | 0/0 |
| phoenix_for_sale_200_extras | 200/200 | 10.49 -> 4.65 (2.3x) | 12 -> 12 | 4.11 -> 3.73 | 0/0 |

### dallas_rent_200_extras: fill rate % (all rows A -> B; same 199 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| agent_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| alt_photos | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| assessed_value | 55.0 | 55.0 | 54.8 | 54.8 | 0 | 0 |
| buyer_agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_office_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 6.5 | 6.5 | 6.5 | 6.5 | 0 | 0 |
| insurance_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| nearby_schools | 99.5 | 99.5 | 99.5 | 99.5 | 0 | 0 |
| neighborhood_median_rent | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 66.0 | 65.5 | 65.8 | 65.8 | 0 | 0 |
| primary_photo | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_history | 73.0 | 72.8 | 72.9 | 72.9 | 0 | 0 |
| property_tax_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| tax | 55.0 | 55.0 | 54.8 | 54.8 | 0 | 0 |
| tax_history | 55.0 | 55.0 | 54.8 | 54.8 | 0 | 0 |
| zip_median_rent | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |

### dallas_sold_200_extras: fill rate % (all rows A -> B; same 200 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| agent_phones | 95.0 | 95.0 | 95.0 | 95.0 | 0 | 0 |
| alt_photos | 98.5 | 98.5 | 98.5 | 98.5 | 0 | 0 |
| assessed_value | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| buyer_agent_email | 93.5 | 93.5 | 93.5 | 93.5 | 0 | 0 |
| buyer_agent_name | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| buyer_agent_phones | 88.5 | 88.5 | 88.5 | 88.5 | 0 | 0 |
| buyer_office_name | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 97.0 | 97.0 | 97.0 | 97.0 | 0 | 0 |
| insurance_rate | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| nearby_schools | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| neighborhood_median_rent | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| primary_photo | 98.5 | 98.5 | 98.5 | 98.5 | 0 | 0 |
| property_history | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_tax_rate | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| tax | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| tax_history | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| zip_median_rent | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |

### phoenix_for_sale_200_extras: fill rate % (all rows A -> B; same 199 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 97.0 | 97.0 | 97.0 | 97.0 | 0 | 0 |
| agent_phones | 96.0 | 96.0 | 96.0 | 96.0 | 0 | 0 |
| alt_photos | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| assessed_value | 87.5 | 87.5 | 87.4 | 87.4 | 0 | 0 |
| buyer_agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_office_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 95.0 | 95.0 | 95.0 | 95.0 | 0 | 0 |
| insurance_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| nearby_schools | 99.0 | 99.0 | 99.0 | 99.0 | 0 | 0 |
| neighborhood_median_rent | 93.0 | 93.0 | 93.0 | 93.0 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 99.5 | 99.5 | 99.5 | 99.5 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| primary_photo | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_history | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_tax_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| tax | 87.5 | 87.5 | 87.4 | 87.4 | 0 | 0 |
| tax_history | 87.5 | 87.5 | 87.4 | 87.4 | 0 | 0 |
| zip_median_rent | 99.0 | 99.0 | 99.0 | 99.0 | 0 | 0 |

Regressions (> 1 point drop on the same homes): none

# 0.9.0 vs 0.9.1 HEAD (final code), extras, back to back, same homes

A = v0.9.0-rerun3 (0.9.0, 2026-11-23 13:42:36)   B = 0.9.1-head (0.9.1, 2026-11-23 13:42:06)

| scenario | rows A/B | time A -> B (median s) | requests A -> B | body MB A -> B | tainted A/B |
|---|---|---|---|---|---|
| dallas_rent_200_extras | 200/200 | 13.83 -> 7.10 (1.9x) | 12 -> 12 | 5.98 -> 5.52 | 0/0 |
| dallas_sold_200_extras | 200/200 | 10.27 -> 6.32 (1.6x) | 12 -> 12 | 3.78 -> 3.38 | 0/0 |
| phoenix_for_sale_200_extras | 200/200 | 11.04 -> 5.87 (1.9x) | 12 -> 12 | 4.11 -> 3.73 | 0/0 |
| dallas_pending_7d_extras | 156/156 | 11.97 -> 6.22 (1.9x) | 14 -> 14 | 10.63 -> 10.13 | 0/0 |

### dallas_rent_200_extras: fill rate % (all rows A -> B; same 200 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| agent_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| alt_photos | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| assessed_value | 55.0 | 55.0 | 55.0 | 55.0 | 0 | 0 |
| buyer_agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_office_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 6.5 | 6.5 | 6.5 | 6.5 | 0 | 0 |
| insurance_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| nearby_schools | 99.5 | 99.5 | 99.5 | 99.5 | 0 | 0 |
| neighborhood_median_rent | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 65.5 | 65.5 | 65.5 | 65.5 | 0 | 0 |
| primary_photo | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_history | 73.0 | 73.0 | 73.0 | 73.0 | 0 | 0 |
| property_tax_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| tax | 55.0 | 55.0 | 55.0 | 55.0 | 0 | 0 |
| tax_history | 55.0 | 55.0 | 55.0 | 55.0 | 0 | 0 |
| zip_median_rent | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |

### dallas_sold_200_extras: fill rate % (all rows A -> B; same 200 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| agent_phones | 95.0 | 95.0 | 95.0 | 95.0 | 0 | 0 |
| alt_photos | 98.5 | 98.5 | 98.5 | 98.5 | 0 | 0 |
| assessed_value | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| buyer_agent_email | 93.5 | 93.5 | 93.5 | 93.5 | 0 | 0 |
| buyer_agent_name | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| buyer_agent_phones | 88.5 | 88.5 | 88.5 | 88.5 | 0 | 0 |
| buyer_office_name | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 97.0 | 97.0 | 97.0 | 97.0 | 0 | 0 |
| insurance_rate | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| nearby_schools | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| neighborhood_median_rent | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| primary_photo | 98.5 | 98.5 | 98.5 | 98.5 | 0 | 0 |
| property_history | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_tax_rate | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| tax | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| tax_history | 96.5 | 96.5 | 96.5 | 96.5 | 0 | 0 |
| zip_median_rent | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |

### phoenix_for_sale_200_extras: fill rate % (all rows A -> B; same 200 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 97.0 | 97.0 | 97.0 | 97.0 | 0 | 0 |
| agent_phones | 96.0 | 96.0 | 96.0 | 96.0 | 0 | 0 |
| alt_photos | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| assessed_value | 87.5 | 87.5 | 87.5 | 87.5 | 0 | 0 |
| buyer_agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_phones | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_office_name | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 95.0 | 95.0 | 95.0 | 95.0 | 0 | 0 |
| insurance_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| nearby_schools | 99.0 | 99.0 | 99.0 | 99.0 | 0 | 0 |
| neighborhood_median_rent | 93.0 | 93.0 | 93.0 | 93.0 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 99.5 | 99.5 | 99.5 | 99.5 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| primary_photo | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_history | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_tax_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| tax | 87.5 | 87.5 | 87.5 | 87.5 | 0 | 0 |
| tax_history | 87.5 | 87.5 | 87.5 | 87.5 | 0 | 0 |
| zip_median_rent | 99.0 | 99.0 | 99.0 | 99.0 | 0 | 0 |

### dallas_pending_7d_extras: fill rate % (all rows A -> B; same 152 homes A -> B; emptied/changed on same homes)
| column | all A | all B | same A | same B | emptied | changed |
|---|---|---|---|---|---|---|
| agent_email | 94.9 | 94.9 | 95.4 | 95.4 | 0 | 0 |
| agent_phones | 94.9 | 94.9 | 95.4 | 95.4 | 0 | 0 |
| alt_photos | 97.4 | 97.4 | 97.4 | 97.4 | 0 | 0 |
| assessed_value | 96.2 | 96.2 | 96.1 | 96.1 | 0 | 0 |
| buyer_agent_email | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| buyer_agent_name | 0.6 | 0.6 | 0.7 | 0.7 | 0 | 0 |
| buyer_agent_phones | 0.6 | 0.6 | 0.7 | 0.7 | 0 | 0 |
| buyer_office_name | 0.6 | 0.6 | 0.7 | 0.7 | 0 | 0 |
| days_on_mls | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| estimated_value | 98.1 | 98.1 | 98.0 | 98.0 | 0 | 0 |
| insurance_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| list_date | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| monthly_home_insurance | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| nearby_schools | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| neighborhood_median_rent | 98.7 | 99.4 | 98.7 | 99.3 | 0 | 0 |
| next_open_house_start | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| office_phones | 99.4 | 99.4 | 99.3 | 99.3 | 0 | 0 |
| open_houses | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 |
| original_list_price | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| primary_photo | 97.4 | 97.4 | 97.4 | 97.4 | 0 | 0 |
| property_history | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| property_tax_rate | 100.0 | 100.0 | 100.0 | 100.0 | 0 | 0 |
| tax | 96.2 | 96.2 | 96.1 | 96.1 | 0 | 0 |
| tax_history | 96.2 | 96.2 | 96.1 | 96.1 | 0 | 0 |
| zip_median_rent | 99.4 | 100.0 | 99.3 | 100.0 | 0 | 0 |

Regressions (> 1 point drop on the same homes): none
