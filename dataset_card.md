---
license: other
task_categories:
  - tabular-regression
  - tabular-classification
language:
  - ar
tags:
  - real-estate
  - saudi-arabia
  - housing
size_categories:
  - 1K<n<10K
---

# Aqar.fm Saudi Real Estate Listings Dataset

## Dataset Description

This dataset contains real estate listings scraped from [sa.aqar.fm](https://sa.aqar.fm/), covering various regions in Saudi Arabia. It includes data for sales, rentals, and auctions.

- **Total Records:** 6,252
- **Date Collected:** 30/12/2025
- **Source:** Public listings on sa.aqar.fm

## File Structure

The dataset is provided in CSV and JSON formats:

- `aqar_fm_listings_cleaned.csv`: The main dataset containing all cleaned listings.
- `aqar_fm_listings_sale_cleaned.csv`: Subset containing only properties for sale.
- `aqar_fm_listings_rental_cleaned.csv`: Subset containing only properties for rent.
- `aqar_fm_listings_auction_cleaned.csv`: Subset containing only properties for auction.

## Columns

| Column Name                                                                                                      | Description                                     |
| ---------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `id`                                                                                                             | Unique identifier for the listing               |
| `title`                                                                                                          | Title of the listing                            |
| `url`                                                                                                            | URL of the listing on aqar.fm                   |
| `price`                                                                                                          | Price of the property (SAR)                     |
| `meter_price`                                                                                                    | Price per square meter                          |
| `price_2_payments`, `price_4_payments`, `price_12_payments`                                                      | Installment payment options most likely         |
| `rnpl_monthly_price`                                                                                             | Rent Now Pay Later monthly price most likely    |
| `area_sqm`                                                                                                       | Area in square meters                           |
| `deed_area`                                                                                                      | Area as recorded in the deed                    |
| `num_bedrooms`, `num_bathrooms`, `num_living_rooms`, `num_kitchens`, `num_rooms`                                 | Counts of various rooms                         |
| `floor_level`                                                                                                    | Floor level of the property                     |
| `furnished`                                                                                                      | Whether the property is furnished               |
| `duplex`                                                                                                         | Whether the property is a duplex                |
| `ac`, `lift`, `maid_room`, `driver_room`, `pool`, `basement`, `backyard`, `playground`, `car_entrance`, `stairs` | Boolean flags for amenities                     |
| `water_availability`, `electrical_availability`, `drainage_availability`                                         | Boolean flags for utilities                     |
| `private_roof`, `two_entrances`, `special_entrance`, `apartment_in_villa`                                        | Boolean flags for specific features             |
| `street_width`                                                                                                   | Width of the street in meters                   |
| `direction`                                                                                                      | Facing direction of the property                |
| `city`, `district`, `address`                                                                                    | Location details                                |
| `latitude`, `longitude`                                                                                          | Geographic coordinates                          |
| `category_id`, `category_name`, `category_en`                                                                    | Property category identifiers and names         |
| `sale_type`                                                                                                      | Type of sale (e.g., sale, rent, daily, auction) |
| `is_rental`, `is_sale`, `is_auction`, `is_daily_rental`                                                          | Boolean flags for listing type                  |
| `create_time`, `published_at`, `last_update`                                                                     | Timestamps for the listing                      |
| `verified`, `boosted`, `premium`                                                                                 | Listing status flags                            |
| `has_img`, `has_video`                                                                                           | Whether the listing has images or videos        |
| `ad_license_number`, `deed_number`, `rega_licensed`                                                              | Legal and licensing information                 |
| `plan_no`, `parcel_no`                                                                                           | Land planning details                           |
| `user_verified`, `company_name`, `user_paid_tier`                                                                | Advertiser information                          |
| `description`                                                                                                    | Full text description of the property           |
| `images`, `videos`                                                                                               | Lists of image and video URLs                   |

## Usage

This dataset is suitable for:

- Real estate price prediction models.
- Market analysis of the Saudi housing market.
- Geographic distribution analysis of properties.

## Disclaimer

This dataset was collected for educational and research purposes. All rights to the original data belong to aqar.fm. Please respect their Terms of Service.
