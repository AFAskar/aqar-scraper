# Aqar.fm Listings Scraper

A small Python scraper that collects real-estate listings from [sa.aqar.fm](https://sa.aqar.fm/) and exports them as a CSV file for analysis.

The scraper:

- Iterates over paginated listing pages on sa.aqar.fm
- Uses cached HTTP responses (via joblib) to avoid re-downloading the same pages
- Parses listing data primarily from embedded JSON (`__NEXT_DATA__`) with a BeautifulSoup fallback
- Saves all results into `data/raw/aqar_fm_listings.csv` and `data/raw/aqar_fm_listings.json`

> ⚠️ **Disclaimer**  
> This project is for personal/educational use only. When using it, you are responsible for complying with aqar.fm’s Terms of Service, robots.txt, and all applicable laws. Do not use it for abusive or high‑volume scraping.

---

## Project Structure

- `main.py` – Entry point and core scraper logic
- `pyproject.toml` – Project metadata and dependencies
- `data/raw/aqar_fm_listings.csv` – Output CSV (created by the scraper)
- `data/raw/aqar_fm_listings.json` – Output JSON (created by the scraper)
- `data/processed/aqar_fm_listings_cleaned.csv` – Cleaned CSV (created by the clean script)
- `data/output/aqar_fm_listings_auction_cleaned.csv` – auction CSV (all auction listings)
- `data/output/aqar_fm_listings_rental_cleaned.csv` – rental CSV (all rental listings)
- `data/output/aqar_fm_listings_sale_cleaned.csv` – sale CSV (all sale listings)
- `data/cache/` – HTTP response cache managed by joblib
- `checks.ipynb` – Example notebook for inspecting the data (optional)

---

## Requirements

- Python **3.13+**
- A working internet connection
- Basic understanding of environment variables / `.env` files

Python dependencies (also defined in `pyproject.toml`):

- `beautifulsoup4`
- `httpx`
- `joblib`
- `pandas`
- `python-dotenv`

---

## Installation

1. Install [uv](https://github.com/astral-sh/uv) if you don’t already have it.
2. From the project root:

   ```bash
   uv sync
   ```

   This will create a virtual environment and install all dependencies.

## Configuration

The site uses Cloudflare and some anti‑bot mechanisms. To make your requests look like a real browser session, you must supply a few cookies via environment variables.

Create a `.env` file in the project root with:

```env
REQ_DEVICE_TOKEN=your_req_device_token_here
CF_CLEARANCE=your_cf_clearance_here
CF_BM=your_cf_bm_here
```

How to obtain these:

1. Open your browser’s DevTools (Network tab).
2. Visit aqar.fm listings pages.
3. Inspect a normal page request and copy the values of:
   - `req-device-token`
   - `cf_clearance`
   - `__cf_bm`
4. Paste them into `.env` as shown above.

If these are missing or invalid, the script may be blocked or return “you have been blocked” in the HTML.

---

## Usage

From the project root:

### Scraper

```bash
uv run main.py
```

The script will:

1. Generate a list of listing URLs starting at:

   ```python
   rooturl = "https://sa.aqar.fm/%D8%B9%D9%82%D8%A7%D8%B1%D8%A7%D8%AA/"
   all_urls = [rooturl + f"{i}" for i in range(1, 9999)]
   ```

2. Fetch pages concurrently (up to 10 threads).
3. Automatically stop when it encounters a page containing the Arabic text “لا توجد نتائج” (“no results”), or if it detects that you are blocked.
4. Parse each page and extract fields such as:
   - `title`, `url`, `price`, `description`
   - `city`, `district`, `address`, `coordinates` (`lat`, `lng`)
   - `sale_type` (`sale`, `rental`, or `auction`)
   - `area_sqm`, `num_bedrooms`, `num_bathrooms`, `num_living_rooms`
   - `floor_level`, `street_width`, `age`
   - **Attributes**: `furnished`, `ac`, `kitchen`, `lift`, `car_entrance`, etc.
   - **Media**: `images`, `videos`
   - **Metadata**: `create_time`, `published_at`, `user_info`
5. Save all listings into:

   ```text
   data/raw/aqar_fm_listings.csv
   data/raw/aqar_fm_listings.json
   ```

The scraper also uses a joblib `Memory` cache under `./data/cache` so repeated runs don’t refetch unchanged pages.

### Clean the Data

To process the raw scraped data, run:

```bash
uv run clean_data.py
```

This script performs several cleaning and normalization steps:

1.  **Deduplication**: Removes duplicate listings based on ID or URL.
2.  **Data Type Conversion**: Converts prices and numeric fields (area, bedrooms, etc.) to proper number formats.
3.  **Text Normalization**:
    - Normalizes Arabic text (unifying aleph forms, etc.).
    - Removes diacritics (Tashkeel).
    - Removes emojis and extra whitespace.
4.  **Boolean Standardization**: Converts various yes/no/1/0 formats to standard booleans.
5.  **Dataset Splitting**: Separates the data into three categories based on `sale_type`:
    - **Sale**: Listings for sale.
    - **Rental**: Listings for rent.
    - **Auction**: Listings for auction.

**Outputs:**

The script generates the following files in `data/processed/` and `data/output/`:

- `data/processed/aqar_fm_listings_cleaned.csv` (Full cleaned dataset)
- `data/output/aqar_fm_listings_sale_cleaned.csv`
- `data/output/aqar_fm_listings_rental_cleaned.csv`
- `data/output/aqar_fm_listings_auction_cleaned.csv`

(JSON versions are also generated for each)

---

## Customization

You can tweak behavior directly in `main.py`:

- **Starting URL / category**  
  Change `rooturl` to scrape a different path or category on aqar.fm.

- **Concurrency**  
  Adjust `max_workers` in the `ThreadPoolExecutor` to control the number of concurrent requests.

- **Page limit / early stop**

  - The script will stop automatically when it sees “لا توجد نتائج”.
  - It also uses a global `STOP_PAGE` to remember the first page without results.
  - To impose a hard limit, you can:
    - Reduce the `range(1, 9999)` to a smaller number of pages, or
    - Manually set `STOP_PAGE` in `main.py` to something finite.

- **Parsed fields**  
  The CSS selectors and icon-to-field mapping live in `parse_category_page()`.  
  You can extend or modify these to extract additional fields.

---

## Notes and Caveats

- If the site changes its HTML structure or CSS classes, parsing may break; in that case, update the selectors in `parse_category_page()`.
- If your cookies expire or change, you’ll need to refresh the `.env` values.
- High-frequency scraping might trigger additional anti-bot measures. Consider:
  - Lowering concurrency
  - Adding small random delays
  - Running less frequently

---

## License

MIT
