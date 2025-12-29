import pandas as pd
import json
import re
from typing import Any, Dict


def clean_price(price: Any) -> float | None:
    """Clean price data - remove commas, convert to float."""
    if pd.isna(price) or price == "" or price is None:
        return None
    if isinstance(price, (int, float)):
        return float(price)
    # Remove commas and any non-numeric characters except decimal point
    price_str = str(price).replace(",", "").strip()
    price_str = re.sub(r"[^\d.]", "", price_str)
    try:
        return float(price_str) if price_str else None
    except ValueError:
        return None


def clean_numeric(value: Any) -> float | None:
    """Clean numeric fields (area, bedrooms, etc.)."""
    if pd.isna(value) or value == "" or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    # Extract first number from string
    value_str = str(value).strip()
    match = re.search(r"\d+\.?\d*", value_str)
    if match:
        try:
            return float(match.group())
        except ValueError:
            return None
    return None


def clean_boolean(value: Any) -> bool | None:
    """Clean boolean fields."""
    if pd.isna(value) or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    value_str = str(value).lower().strip()
    if value_str in ["true", "1", "yes", "نعم"]:
        return True
    elif value_str in ["false", "0", "no", "لا"]:
        return False
    return None


def clean_text(value: Any) -> str | None:
    """Clean text fields - remove extra whitespace, normalize."""
    if pd.isna(value) or value == "":
        return None
    text = str(value).strip()
    # Replace multiple spaces with single space
    text = re.sub(r"\s+", " ", text)
    return text if text else None


def clean_list_field(value: Any) -> list | None:
    """Clean list fields (images, videos)."""
    if pd.isna(value) or value == "" or value is None:
        return None

    if isinstance(value, list):
        return value if value else None

    # Handle string representation of list
    value_str = str(value).strip()
    if value_str.startswith("[") and value_str.endswith("]"):
        try:
            parsed = eval(value_str)
            return parsed if parsed else None
        except:
            return None
    return None


def clean_dict_field(value: Any) -> dict | None:
    """Clean dictionary fields (category)."""
    if pd.isna(value) or value == "" or value is None:
        return None

    if isinstance(value, dict):
        return value

    # Handle string representation of dict
    value_str = str(value).strip()
    if value_str.startswith("{") and value_str.endswith("}"):
        try:
            parsed = eval(value_str)
            return parsed if parsed else None
        except:
            return None
    return None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning functions to all columns in the dataframe."""
    df_cleaned = df.copy()

    # Price columns
    price_columns = [
        "price",
        "meter_price",
        "price_2_payments",
        "price_4_payments",
        "price_12_payments",
        "rnpl_monthly_price",
    ]
    for col in price_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_price)

    # Numeric columns
    numeric_columns = [
        "area_sqm",
        "deed_area",
        "num_bedrooms",
        "num_bathrooms",
        "num_living_rooms",
        "num_kitchens",
        "num_rooms",
        "floor_level",
        "age",
        "street_width",
        "city_id",
        "district_id",
        "province_id",
        "latitude",
        "longitude",
        "user_paid_tier",
    ]
    for col in numeric_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_numeric)

    # Boolean columns
    boolean_columns = [
        "furnished",
        "duplex",
        "ac",
        "lift",
        "maid_room",
        "driver_room",
        "pool",
        "basement",
        "backyard",
        "playground",
        "car_entrance",
        "stairs",
        "stores",
        "wells",
        "trees",
        "water_availability",
        "electrical_availability",
        "drainage_availability",
        "private_roof",
        "two_entrances",
        "special_entrance",
        "apartment_in_villa",
        "is_rental",
        "is_sale",
        "is_auction",
        "is_daily_rental",
        "verified",
        "boosted",
        "premium",
        "has_img",
        "has_video",
        "user_verified",
        "rega_licensed",
    ]
    for col in boolean_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_boolean)

    # Text columns
    text_columns = [
        "id",
        "title",
        "url",
        "rent_period",
        "zoning",
        "direction",
        "city",
        "district",
        "address",
        "sale_type",
        "ad_license_number",
        "deed_number",
        "plan_no",
        "parcel_no",
        "user_type",
        "company_name",
        "description",
        "street_direction",
    ]
    for col in text_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_text)

    # Timestamp columns - keep as is but ensure proper format
    timestamp_columns = ["create_time", "published_at", "last_update"]
    for col in timestamp_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce")

    # List columns
    list_columns = ["images", "videos"]
    for col in list_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_list_field)

    # Dictionary column
    if "category" in df_cleaned.columns:
        df_cleaned["category"] = df_cleaned["category"].apply(clean_dict_field)

    return df_cleaned


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate listings based on ID or URL."""
    # Remove duplicates based on ID if available
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="first")
    # Also remove duplicates based on URL as fallback
    elif "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="first")

    return df


def fix_line_terminators(file_path: str) -> None:
    """Fix unusual line terminators in a file by reading and rewriting with standard LF."""
    with open(file_path, "rb") as f:
        content = f.read()

    # Replace all types of line endings with LF (\n)
    content = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")

    with open(file_path, "wb") as f:
        f.write(content)


def main():
    """Main cleaning process."""
    print("Starting data cleaning process...")

    # Load the data
    print("Loading data from CSV...")
    df = pd.read_csv("aqar_fm_listings.csv")

    print(f"Loaded {len(df)} records")
    print(f"Columns: {df.columns.tolist()}")

    # Remove duplicates
    print("\nRemoving duplicates...")
    df = remove_duplicates(df)
    print(f"Records after removing duplicates: {len(df)}")

    # Clean the data
    print("\nCleaning data...")
    df_cleaned = clean_dataframe(df)

    # Show some statistics
    print("\nData cleaning summary:")
    print(f"Total records: {len(df_cleaned)}")
    print(f"Null values per column:")
    print(df_cleaned.isnull().sum())

    # Save cleaned data
    print("\nSaving cleaned data...")
    df_cleaned.to_csv("aqar_fm_listings_cleaned.csv", index=False, lineterminator="\n")
    df_cleaned.to_json(
        "aqar_fm_listings_cleaned.json", orient="records", force_ascii=False, indent=2
    )

    # Fix line terminators in original files
    print("\nFixing line terminators in original files...")
    fix_line_terminators("aqar_fm_listings.csv")
    fix_line_terminators("aqar_fm_listings.json")

    print("\nData cleaning completed successfully!")
    print(f"Cleaned files saved as:")
    print("  - aqar_fm_listings_cleaned.csv")
    print("  - aqar_fm_listings_cleaned.json")
    print(f"Original files have been fixed for line terminators.")


if __name__ == "__main__":
    main()
