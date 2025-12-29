import httpx
import json
import os
from dotenv import load_dotenv
from joblib import Memory
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

memory = Memory("./cache", verbose=0)

STOP_PAGE = float("inf")


@memory.cache
def fetch_data(url: str) -> str | None:
    global STOP_PAGE
    try:
        page_num = int(url.split("/")[-1])
    except (ValueError, IndexError):
        page_num = 0

    if page_num >= STOP_PAGE:
        return None

    cookies = {
        "req-device-token": os.getenv("REQ_DEVICE_TOKEN", "get-your-cookies"),
        "cf_clearance": os.getenv("CF_CLEARANCE", "get-your-cookies"),
        "__cf_bm": os.getenv("CF_BM", "get-your-cookies"),
    }

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9,ar;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=0, i",
        "referer": "https://duckduckgo.com/",
        "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    }

    timeout = 30
    while True:
        try:
            response = httpx.get(
                url,
                cookies=cookies,
                headers=headers,
                timeout=timeout,
                follow_redirects=True,
            )
            break
        except httpx.ReadTimeout:
            print(
                f"Timeout fetching {url} with {timeout}s, retrying with {timeout + 10}s..."
            )
            timeout += 10

    textof = response.text

    assert not "you have been blocked" in textof.lower(), "Blocked by the website"

    if "لا توجد نتائج" in textof:
        STOP_PAGE = min(STOP_PAGE, page_num)
        return None

    print(f"Fetched data from {url}")
    return textof


def parse_category_page(page: str) -> list[dict]:
    output = []
    soup = BeautifulSoup(page, "html.parser")
    listings = soup.select(
        "#__next > main > div > div._root__Szbd6 > div > div:nth-child(2) > div._container__Lu67A > div._list__Ka30R > div"
    )
    for listing in listings:

        def get_text(selector):
            element = listing.select_one(selector)
            return element.get_text(separator=" ").strip() if element else None

        def is_auction(strornum):
            if not strornum:
                return None
            if "مزاد" in strornum:
                return True
            return False

        def get_type(string: str) -> str | None:
            rental_keywords = ["ايجار", "شهري", "سنوي"]
            if string:
                for keyword in rental_keywords:
                    if keyword in string:
                        return "rental"
                if is_auction(string):
                    return "auction"
                return "sale"
            return None

        priceorauction = get_text("a > div > div._content__W4gas > div._price__X51mi")

        sale_type = get_type(priceorauction)

        dict_item = {}
        dict_item["title"] = get_text(
            "a > div > div._content__W4gas > div._titleRow__1AWv1 > h4"
        )
        listing_url = listing.select_one("a")
        dict_item["url"] = (
            "https://sa.aqar.fm" + listing_url["href"]
            if listing_url and "href" in listing_url.attrs
            else None
        )
        if sale_type == "auction":
            dict_item["price"] = None
        elif sale_type == "rental":
            # for rentals, keep only the number part
            dict_item["price"] = priceorauction.split(" ")[0]
        else:
            # for sales there is only a number part
            dict_item["price"] = (
                priceorauction if not is_auction(priceorauction) else None
            )
        dict_item["description"] = get_text(
            "a > div > div._content__W4gas > div._description__zVaD6"
        )
        dict_item["city"] = get_text(
            "a > div > div._content__W4gas > div._footer__CnldH > p > span:nth-child(1)"
        )
        dict_item["neighborhood"] = (
            get_text(
                "a > div > div._content__W4gas > div._footer__CnldH > p > span:nth-child(2)"
            )
            .replace("-", "")
            .strip()
        )
        dict_item["sale_type"] = sale_type
        svgimgs = listing.select(
            "a > div > div._content__W4gas > div._specs__nbsgm .icon_icon___L1OO img[src]"
        )
        svgurl2span = {img["src"]: img.parent for img in svgimgs}
        for k, span in svgurl2span.items():
            if not k:
                continue
            icon_name: str = k.split("/")[-1].split(".")[0]
            value: str | None = span.parent.get_text(strip=True)
            icon_map = {
                "area": "area_sqm",
                "bed-king": "num_bedrooms",
                "bath": "num_bathrooms",
                "couch": "num_living_rooms",
                "pinned-note": "zoning",
                "street": "street-width",
            }

            icon_value = icon_map.get(icon_name, icon_name)
            dict_item[icon_value] = value if value != "undefined" else None
        output.append(dict_item)
    return output


def parse_using_json(page: str) -> list[dict]:
    output = []
    soup = BeautifulSoup(page, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if not script_tag:
        return parse_category_page(page)

    try:
        data = json.loads(script_tag.string)
        listing_ids_parent = data["props"]["pageProps"]["__APOLLO_STATE__"][
            "ROOT_QUERY"
        ]["WEB"]
        # listing ids are stored under the find sql key find({\"from\":20,\"size\":20,\"sort\":{\"create_time\":\"desc\",\"has_img\":\"desc\"},\"where\":{}}) so the from size may vary
        listing_ids_key = next(
            key
            for key in listing_ids_parent.keys()
            if key.startswith('find({"from":') and key.endswith("})")
        )
        listing_ids = listing_ids_parent[listing_ids_key]["listings"]

        for listing_id in listing_ids:
            listing_data = data["props"]["pageProps"]["__APOLLO_STATE__"].get(
                listing_id, {}
            )
            dict_item = {}
            dict_item["Id"] = listing_data.get("id")
            dict_item["title"] = listing_data.get("title")
            dict_item["uri"] = listing_data.get("uri")

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing JSON data: {e}")

    return output


def parse_all_category_pages(pages: list[str]) -> list[dict]:
    all_listings = []
    for page in pages:
        listings = parse_category_page(page)
        all_listings.extend(listings)
    return all_listings


def get_all_category_pages(
    rooturl: str = "https://sa.aqar.fm/%D8%B9%D9%82%D8%A7%D8%B1%D8%A7%D8%AA/",
) -> list[str]:
    all_urls = [rooturl + f"{i}" for i in range(1, 9999)]

    all_pages = []
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for page in executor.map(fetch_data, all_urls):
                if page:
                    all_pages.append(page)
    except AssertionError as e:
        print(f"Stopped fetching more pages due to error: {e}")
    return all_pages


if __name__ == "__main__":

    rooturl = "https://sa.aqar.fm/%D8%B9%D9%82%D8%A7%D8%B1%D8%A7%D8%AA/"
    all_urls = [rooturl + f"{i}" for i in range(1, 9999)]

    all_pages = get_all_category_pages(rooturl)

    all_listings = parse_all_category_pages(all_pages)

    df = pd.DataFrame(all_listings)
    df.to_csv("aqar_fm_listings.csv", index=False)
