# Aqar.fm Listings Scraper

A small Python scraper that collects real-estate listings from [sa.aqar.fm](https://sa.aqar.fm/) and exports them as a CSV file for analysis.

The scraper:

- Iterates over paginated listing pages on sa.aqar.fm
- Uses cached HTTP responses (via joblib) to avoid re-downloading the same pages
- Parses listing data with BeautifulSoup
- Saves all results into `aqar_fm_listings.csv`

> ⚠️ **Disclaimer**  
> This project is for personal/educational use only. When using it, you are responsible for complying with aqar.fm’s Terms of Service, robots.txt, and all applicable laws. Do not use it for abusive or high‑volume scraping.

---

## Project Structure

- `main.py` – Entry point and core scraper logic
- `pyproject.toml` – Project metadata and dependencies
- `aqar_fm_listings.csv` – Output CSV (created by the scraper)
- `cache/` – HTTP response cache managed by joblib
- `checks.ipynb` – Example notebook for inspecting the data (optional)
- `data.html` – Example saved HTML page (optional)
- `output.json` – Legacy/example output (not used by main.py)

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

You can use either **uv** (recommended, since `uv.lock` is present) or plain **pip**.

### Option 1: Using uv

1. Install [uv](https://github.com/astral-sh/uv) if you don’t already have it.
2. From the project root:

   ```bash
   uv sync
   ```

   This will create a virtual environment and install all dependencies.

### Option 2: Using pip

1. Create and activate a virtual environment (optional but recommended):

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # on Windows: .venv\Scripts\activate
   ```

2. Install dependencies:

   ```bash
   pip install beautifulsoup4 httpx joblib pandas python-dotenv
   ```

---

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

### With uv

```bash
uv run main.py
```

### With plain Python

(Activate your virtualenv if you created one.)

```bash
python main.py
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
   - `title`
   - `url`
   - `price`
   - `description`
   - `city`
   - `neighborhood`
   - `sale_type` (`sale`, `rental`, or `auction`)
   - `area_sqm`
   - `num_bedrooms`
   - `num_bathrooms`
   - `num_living_rooms`
   - `zoning`
   - `street-width`
5. Save all listings into:

   ```text
   aqar_fm_listings.csv
   ```

The scraper also uses a joblib `Memory` cache under `./cache` so repeated runs don’t refetch unchanged pages.

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
