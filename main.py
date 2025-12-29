from typing import Literal
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
        dict_item["district"] = (
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


def get_category_details(key: str):
    json_data = """
    {
  "0": {
    "id": 0,
    "name": "عقارات",
    "en": "All",
    "plural": "عقارات",
    "uri": "عقارات",
    "path": "/عقارات",
    "keywords": ["عقارات", "عقار", "للبيع", "للإيجار", "تأجير", "أجار"],
    "description": "تصفح آلاف العقارات المعروضة للبيع والايجار في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بمساحات وأسعار مختلفة مع الاطلاع على أفضل الأحياء تقييمًا من خلال ميزة تقييم الحي في تطبيق عقار",
    "index": 0
  },
  "1": {
    "id": 1,
    "name": "شقة للإيجار",
    "en": "Apartment for rent",
    "ga_listing_type": "rent",
    "ga_property_category": "apartment",
    "is_rent": true,
    "plural": "شقق للإيجار",
    "uri": "شقق-للإيجار",
    "path": "/شقق-للإيجار",
    "keywords": ["شقة", "شقق", "للإيجار", "تأجير", "أجار"],
    "description": "شقق للايجار بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عقد سنوي بالصور و الأسعار, يمكنكم البحث عن شقق بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عقد سنوي بسعر مناسب",
    "index": 1
  },
  "2": {
    "id": 2,
    "name": "أرض للبيع",
    "en": "Land for sale",
    "ga_listing_type": "sale",
    "ga_property_category": "land",
    "is_rent": false,
    "plural": "أراضي للبيع",
    "uri": "أراضي-للبيع",
    "path": "/أراضي-للبيع",
    "keywords": ["أرض", "ارض", "اراضي", "أراضي", "اراضى", "اراض", "للبيع"],
    "description": "اراضي للبيع بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بسعر المتر و الصور, يمكنك البحث عن ارض للبيع بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بسعر مناسب من المالك",
    "index": 2
  },
  "3": {
    "id": 3,
    "ga_listing_type": "sale",
    "ga_property_category": "villa",
    "is_rent": false,
    "name": "فيلا للبيع",
    "en": "Villa for sale",
    "plural": "فلل للبيع",
    "uri": "فلل-للبيع",
    "path": "/فلل-للبيع",
    "keywords": ["فيلا", "دوبلكس", "دبلكس", "دوبلكسات", "فلل", "للبيع"],
    "description": "فلل للبيع بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية فلل وقصور فاخرة و قصور للبيع بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية فلل وقصور فاخرة. يمكنك البحث عن فلل بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية فلل وقصور فاخرة بسعر مناسب",
    "index": 3
  },
  "4": {
    "id": 4,
    "ga_listing_type": "rent",
    "ga_property_category": "flat",
    "is_rent": true,
    "name": "دور للإيجار",
    "en": "Big flat for rent",
    "plural": "دور للإيجار",
    "uri": "دور-للإيجار",
    "path": "/دور-للإيجار",
    "keywords": ["دور", "أدوار", "للإيجار", "تأجير", "أجار"],
    "description": "ادوار للايجار بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية جديد بجميع الأحياء و الأسعار. يمكنك البحث عن دور ارضي بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية جديد بسعر مناسب",
    "index": 4
  },
  "5": {
    "id": 5,
    "ga_listing_type": "rent",
    "ga_property_category": "villa",
    "is_rent": true,
    "name": "فيلا للإيجار",
    "en": "Villa for rent",
    "plural": "فلل للإيجار",
    "uri": "فلل-للإيجار",
    "path": "/فلل-للإيجار",
    "keywords": [
      "فيلا",
      "دبلكس",
      "دوبلكسات",
      "فلل",
      "للإيجار",
      "تأجير",
      "أجار",
      "فلل للإيجار"
    ],
    "description": " فلل للايجار بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عرض الشارع أكثر من 10 متر يمكنك ايجاد فيلا أو دبلكس للايجار بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عرض الشارع أكثر من 10 متر بسعر مناسب",
    "index": 5
  },
  "6": {
    "id": 6,
    "ga_listing_type": "sale",
    "ga_property_category": "apartment",
    "is_rent": false,
    "name": "شقة للبيع",
    "en": "Apartment for sale",
    "plural": "شقق للبيع",
    "uri": "شقق-للبيع",
    "path": "/شقق-للبيع",
    "keywords": ["شقة", "شقق", "للبيع"],
    "description": "شقق للبيع بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية مدخل خاص بالصور و شقق تمليك. يمكنك ايجاد شقق للبيع بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية مدخل خاص بسعر مناسب",
    "index": 6
  },
  "7": {
    "id": 7,
    "ga_listing_type": "sale",
    "ga_property_category": "building",
    "is_rent": false,
    "name": "عمارة للبيع",
    "en": "Building for sale",
    "plural": "عمائر للبيع",
    "uri": "عمائر-للبيع",
    "path": "/عمائر-للبيع",
    "keywords": ["عمائر", "عمارات", "عماير", "للبيع"],
    "description": "عمائر سكنية وتجارية للبيع في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عمر العقار أقل من 5 سنوات بمساحات و مميزات مختلفة بأفضل الأسعار مع الصور والتفاصيل ...",
    "index": 7
  },
  "8": {
    "id": 8,
    "ga_listing_type": "rent",
    "ga_property_category": "store",
    "is_rent": true,
    "name": "محل للإيجار",
    "en": "Store for rent",
    "plural": "محلات للإيجار",
    "uri": "محلات-للإيجار",
    "path": "/محلات-للإيجار",
    "keywords": ["محل", "محال", "محلات", "للإيجار", "تأجير", "أجار"],
    "description": "محلات ومعارض تجارية للإيجار في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عرض الشارع أكثر من 30 متر بمساحات و مميزات مختلفة بأفضل الأسعار مع الصور والتفاصيل ... ",
    "index": 8
  },
  "9": {
    "id": 9,
    "ga_listing_type": "sale",
    "ga_property_category": "house",
    "is_rent": false,
    "name": "بيت للبيع",
    "en": "Small house for sale",
    "plural": "بيت للبيع",
    "uri": "بيت-للبيع",
    "path": "/بيت-للبيع",
    "keywords": ["بيت", "بيوت", "للبيع"],
    "description": "بيوت من دور واحد , وبيوت شعبية للبيع في  الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية الواجهة شرق بمساحات و أسعار مختلفة مع الصور والتفاصيل",
    "index": 9
  },
  "10": {
    "id": 10,
    "ga_listing_type": "sale",
    "ga_property_category": "lounge",
    "is_rent": false,
    "name": "استراحة للبيع",
    "en": "Lounge for sale",
    "plural": "استراحة للبيع",
    "uri": "استراحة-للبيع",
    "path": "/استراحة-للبيع",
    "keywords": [
      "استراحة",
      "استراحات",
      "منتجعات",
      "شاليهات",
      "منتزه",
      "منتزهات",
      "للبيع"
    ],
    "description": "استراحات فاخرة للبيع في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بمساحات وأسعار مناسبة مع تصفح الصور والتفاصيل و المميزات والخدمات ...",
    "index": 10
  },
  "11": {
    "id": 11,
    "ga_listing_type": "rent",
    "ga_property_category": "house",
    "is_rent": true,
    "name": "بيت للإيجار",
    "en": "Small house for rent",
    "plural": "بيت للإيجار",
    "uri": "بيت-للإيجار",
    "path": "/بيت-للإيجار",
    "keywords": ["بيت", "بيوت", "للإيجار", "تأجير", "أجار"],
    "description": "للبحث عن بيوت من دور واحد أو بيوت شعبية للإيجار الشهري أو السنوي في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بمساحات و أسعار مناسبة",
    "index": 11
  },
  "12": {
    "id": 12,
    "ga_listing_type": "sale",
    "ga_property_category": "farm",
    "is_rent": false,
    "name": "مزرعة للبيع",
    "en": "Farm for sale",
    "plural": "مزرعة للبيع",
    "uri": "مزرعة-للبيع",
    "path": "/مزرعة-للبيع",
    "keywords": ["مزرعة", "مزارع", "للبيع"],
    "description": "مزارع للبيع الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بمساحات وأسعار مناسبة مع تصفح الصور والتفاصيل و المميزات والخدمات ...",
    "index": 12
  },
  "13": {
    "id": 13,
    "ga_listing_type": "rent",
    "ga_property_category": "lounge",
    "is_rent": true,
    "name": "استراحة للإيجار",
    "en": "Lounge for rent",
    "plural": "استراحة للإيجار",
    "uri": "استراحة-للإيجار",
    "path": "/استراحة-للإيجار",
    "keywords": [
      "استراحة",
      "استراحات",
      "منتجعات",
      "شاليهات",
      "منتزه",
      "منتزهات",
      "للإيجار",
      "تأجير",
      "أجار"
    ],
    "description": "استراحات فاخرة للحجز اليومي والشهري والسنوي للعوائل والأفراد في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بمساحات وأسعار مناسبة مع تصفح الصور والتفاصيل و المميزات والخدمات و الأماكن القريبة ...",
    "index": 13
  },
  "14": {
    "id": 14,
    "ga_listing_type": "rent",
    "ga_property_category": "office",
    "is_rent": true,
    "name": "مكتب تجاري للإيجار",
    "en": "Office for rent",
    "plural": "مكتب تجاري للإيجار",
    "uri": "مكتب-تجاري-للإيجار",
    "path": "/مكتب-تجاري-للإيجار",
    "keywords": ["مكتب تجاري", "مكاتب تجارية", "للإيجار", "تأجير", "أجار"],
    "description": "مكاتب تجارية و إدارية للإيجار في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عرض الشارع أكثر من 15 متر بمساحات و مميزات وتجهيزات مختلفة بأفضل الأسعار مع الصور والتفاصيل ...",
    "index": 14
  },
  "15": {
    "id": 15,
    "ga_listing_type": "rent",
    "ga_property_category": "land",
    "is_rent": true,
    "name": "أرض للإيجار",
    "en": "Land for rent",
    "plural": "أراضي للإيجار",
    "uri": "أراضي-للإيجار",
    "path": "/أراضي-للإيجار",
    "keywords": [
      "أرض",
      "ارض",
      "اراضي",
      "أراضي",
      "اراضى",
      "اراض",
      "للإيجار",
      "تأجير",
      "أجار"
    ],
    "description": "أراضي سكنية و وتجارية و زراعية وصناعية للإيجار بمساحات و أسعار مختلفة في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية مع خدمة الاطلاع على المؤشرات العقارية من عقار",
    "index": 15
  },
  "16": {
    "id": 16,
    "ga_listing_type": "rent",
    "ga_property_category": "building",
    "is_rent": true,
    "name": "عمارة للإيجار",
    "en": "Building for rent",
    "plural": "عمائر للإيجار",
    "uri": "عمائر-للإيجار",
    "path": "/عمائر-للإيجار",
    "keywords": ["عمائر", "عمارات", "عماير", "للإيجار", "تأجير", "أجار"],
    "description": "عمائر سكنية وتجارية للإيجار في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية بمساحات و مميزات مختلفة بأفضل الأسعار مع الصور والتفاصيل ... ",
    "index": 16
  },
  "17": {
    "id": 17,
    "ga_listing_type": "rent",
    "ga_property_category": "warehouse",
    "is_rent": true,
    "name": "مستودع للإيجار",
    "en": "Warehouse for rent",
    "plural": "مستودع للإيجار",
    "uri": "مستودع-للإيجار",
    "path": "/مستودع-للإيجار",
    "keywords": ["مستودع", "مستودعات", "للإيجار", "تأجير", "أجار"],
    "description": "مستودعات للإيجار بإيجار سنوي وشهري ويومي في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عرض الشارع أكثر من 30 متر بمساحات و مميزات وتجهيزات مختلفة بأفضل الأسعار مع الصور والتفاصيل ...",
    "index": 17
  },
  "18": {
    "id": 18,
    "ga_listing_type": "rent",
    "ga_property_category": "tent",
    "is_rent": true,
    "name": "مخيم للإيجار",
    "en": "Tent for rent",
    "plural": "مخيم للإيجار",
    "uri": "مخيم-للإيجار",
    "path": "/مخيم-للإيجار",
    "keywords": ["مخيم", "مخيمات", "للإيجار", "تأجير", "أجار"],
    "description": "مخيمات فاخرة للإيجار بالساعة و الحجز اليومي والشهري بمساحات وأسعار مناسبة مع تصفح الصور والتفاصيل و المميزات والخدمات و الأماكن القريبة على عقار ... ، تطبيق عقار",
    "index": 18
  },
  "19": {
    "id": 19,
    "ga_listing_type": "rent",
    "ga_property_category": "room",
    "is_rent": true,
    "name": "غرفة للإيجار",
    "en": "Room for rent",
    "plural": "غرف للإيجار",
    "uri": "غرف-للإيجار",
    "path": "/غرف-للإيجار",
    "keywords": ["غرفة", "غرف", "للإيجار", "تأجير", "أجار"],
    "description": "غرف للإيجار بإيجار يومي و شهري وسنوي بالرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية مؤثثة وبدون تأثيث بأسعار مناسبة و مساحات مختلفة",
    "index": 19
  },
  "20": {
    "id": 20,
    "ga_listing_type": "sale",
    "ga_property_category": "store",
    "is_rent": false,
    "name": "محل للبيع",
    "en": "Store for sale",
    "plural": "محلات للبيع",
    "uri": "محلات-للبيع",
    "path": "/محلات-للبيع",
    "keywords": ["محل", "محال", "محلات", "للبيع"],
    "description": "محلات ومعارض تجارية للبيع و التقبيل في الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عرض الشارع أكثر من 10 متر بمساحات و مميزات مختلفة بأفضل الأسعار مع الصور والتفاصيل ... ",
    "index": 20
  },
  "22": {
    "id": 22,
    "ga_listing_type": "sale",
    "ga_property_category": "floor",
    "is_rent": false,
    "name": "دور للبيع",
    "en": "Floor for sale",
    "plural": "دور للبيع",
    "uri": "دور-للبيع",
    "path": "/دور-للبيع",
    "keywords": [
      "دور للبيع",
      "ادوار للبيع",
      "دور ارضي للبيع",
      "بيت دور للبيع",
      "ادوار تمليك"
    ],
    "description": "أدوار للبيع الرياض، جدة، الدمام، مكة، المدينة و جميع مناطق المملكة العربية السعودية عمر العقار أقل من 5 سنوات، يمكنك البحث عن دور أرضي أو أول أو ثاني بأسعار مناسبة على عقار",
    "index": 21
  },
  "23": {
    "id": 23,
    "ga_listing_type": "rent",
    "ga_property_category": "chalet",
    "is_rent": true,
    "name": "شاليه للإيجار",
    "en": "Chalet for rent",
    "plural": "شاليه للإيجار",
    "uri": "شاليه-للإيجار",
    "path": "/شاليه-للإيجار",
    "keywords": ["شاليه", "شاليهات", "شاليه للايجار", "للإيجار"],
    "description": "تصفح أفضل وأرخص الشاليهات للحجز اليومي والشهري والسنوي في مكان واحد بمساحات وأسعار مناسبة، ابحث بالخريطة وتصفح الصور والتفاصيل و الأماكن القريبة ... ، تطبيق عقار",
    "index": 22
  },
  "24": {
    "id": 24,
    "ga_listing_type": null,
    "ga_property_category": "other",
    "is_rent": false,
    "name": "أخرى",
    "en": "Other",
    "plural": "أخرى",
    "uri": "أخرى",
    "path": "/أخرى",
    "keywords": ["أخرى"],
    "index": 23
  },
  "101": {
    "id": 101,
    "ga_listing_type": "daily",
    "ga_property_category": "apartment",
    "is_rent": true,
    "name": "شقة مفروشة للحجز",
    "en": "Apartment for booking",
    "plural": "شقق مفروشة للحجز",
    "uri": "شقق-مفروشة-للحجز",
    "path": "/شقق-مفروشة-للحجز",
    "keywords": ["حجز", "مفروشة", "للحجز", "شقة"],
    "index": 1
  },
  "102": {
    "id": 102,
    "ga_listing_type": "daily",
    "ga_property_category": "villa",
    "is_rent": true,
    "name": "فيلا مفروشة للحجز",
    "en": "Villa for booking",
    "plural": "فلل مفروشة للحجز",
    "uri": "فلل-مفروشة-للحجز",
    "path": "/فلل-مفروشة-للحجز",
    "keywords": ["حجز", "مفروشة", "للحجز", "فيلا"],
    "index": 2
  },
  "103": {
    "id": 103,
    "ga_listing_type": "daily",
    "ga_property_category": "studio",
    "is_rent": true,
    "name": "استديو مفروش للحجز",
    "en": "Studio for booking",
    "plural": "استديوهات مفروشة للحجز",
    "uri": "استديوهات-مفروشة-للحجز",
    "path": "/استديوهات-مفروشة-للحجز",
    "keywords": ["حجز", "مفروش", "استديو", "للحجز"],
    "index": 3
  },
  "104": {
    "id": 104,
    "ga_listing_type": "daily",
    "ga_property_category": "chalet",
    "is_rent": true,
    "name": "شاليه للحجز",
    "en": "Chalet for booking",
    "plural": "شاليهات واستراحات للحجز",
    "uri": "شاليهات-للحجز",
    "path": "/شاليهات-للحجز",
    "keywords": ["حجز", "للإيجار", "للحجز", "شاليه"],
    "index": 4
  },
  "106": {
    "id": 106,
    "ga_listing_type": "daily",
    "ga_property_category": "tent",
    "is_rent": true,
    "name": "مخيم للحجز",
    "en": "Tent for booking",
    "plural": "مخيمات للحجز",
    "uri": "مخيمات-للحجز",
    "path": "/مخيمات-للحجز",
    "keywords": ["حجز", "للإيجار", "مخيم", "للحجز"],
    "index": 6
  },
  "107": {
    "id": 107,
    "ga_listing_type": "daily",
    "ga_property_category": "farm",
    "is_rent": true,
    "name": "مزرعة للحجز",
    "en": "Farm for booking",
    "plural": "مزارع للحجز",
    "uri": "مزارع-للحجز",
    "path": "/مزارع-للحجز",
    "keywords": ["حجز", "للإيجار", "مزرعة", "للحجز"],
    "index": 7
  },
  "108": {
    "id": 108,
    "ga_listing_type": "daily",
    "ga_property_category": "hall",
    "is_rent": true,
    "name": "قاعة للحجز",
    "en": "Hall for booking",
    "plural": "قاعات للحجز",
    "uri": "قاعات-للحجز",
    "path": "/قاعات-للحجز",
    "keywords": ["حجز", "للإيجار", "للحجز", "قاعة"],
    "index": 8
  }
}
    """
    categoriesDictionary = json.loads(json_data)
    return categoriesDictionary.get(key, None)


def parse_using_json(page: str) -> list[dict]:
    """parses the page content using embedded JSON data


    JSON EXAMPLE:
                "ElasticWebListing:6490057": {
                    "__typename": "ElasticWebListing",
                    "id": 6490057,
                    "rnpl_monthly_price": 3567,
                    "sov_campaign_id": null,
                    "boosted": 0,
                    "ac": 1,
                    "age": 0,
                    "apts": null,
                    "area": 50,
                    "deed_area": 418.5,
                    "backyard": null,
                    "basement": null,
                    "beds": 2,
                    "car_entrance": 0,
                    "category": 1,
                    "city_id": 21,
                    "create_time": 1764684040,
                    "biddable": false,
                    "published_at": 1764684040,
                    "direction_id": 4,
                    "district_id": 494,
                    "province_id": 5,
                    "driver": null,
                    "duplex": null,
                    "extra_unit": 0,
                    "family": 1,
                    "family_section": null,
                    "fb": null,
                    "fl": 0,
                    "furnished": 0,
                    "has_img": 1,
                    "imgs": [
                        "046793280_1764844417913.jpg",
                        "046793285_1764844437360.jpg",
                        "046793282_1764844450847.jpg",
                        "046793283_1764844465389.jpg",
                        "046793283_1764844478580.jpg",
                        "046793283_1764844498867.jpg",
                        "046793285_1764844512480.jpg",
                        "046793287_1764844541471.jpg",
                        "046793286_1764844563516.jpg",
                        "046793288_1764844589667.jpg",
                        "046793286_1764844606238.jpg",
                        "046793285_1764844629556.jpg",
                        "046793285_1764844642821.jpg",
                        "046793281_1764844669259.jpg"
                    ],
                    "imgs_desc": [
                        "غرفة نوم واحدة وصالة مع مطبخ وحمام",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        ""
                    ],
                    "ketchen": 1,
                    "last_update": 1766664465,
                    "refresh": 1766664465,
                    "lift": 1,
                    "livings": 1,
                    "location": {
                        "__typename": "Location",
                        "lat": 24.896124,
                        "lng": 46.611506
                    },
                    "maid": null,
                    "men_place": null,
                    "meter_price": null,
                    "playground": null,
                    "pool": null,
                    "premium": 0,
                    "price": 40000,
                    "price_2_payments": 42000,
                    "price_4_payments": 0,
                    "price_12_payments": 0,
                    "range_price": null,
                    "rent_period": 3,
                    "rooms": 2,
                    "stairs": null,
                    "stores": null,
                    "status": 0,
                    "street_direction": 4,
                    "street_width": 30,
                    "tent": null,
                    "trees": null,
                    "type": null,
                    "user_id": 4679328,
                    "user": {
                        "__typename": "ListingUser",
                        "phone": 0,
                        "name": "محمد عادل عبدالرحمن الراشد",
                        "img": null,
                        "type": 1,
                        "paid": 5,
                        "fee": null,
                        "review": 5,
                        "iam_verified": true,
                        "rega_id": null,
                        "bml_license_number": null,
                        "bml_url": null,
                        "company_name": "شركة كبار نجد للتطوير العقاري شركة شخص واحد",
                        "company_logo": "046793288_1753779728439.jpg"
                    },
                    "employee": {
                        "__typename": "ListingEmployee",
                        "phone": 0,
                        "name": "محمد عادل عبدالرحمن الراشد",
                        "img": null
                    },
                    "user_type": 0,
                    "vb": null,
                    "wc": 1,
                    "wells": null,
                    "women_place": null,
                    "has_video": 1,
                    "videos": [
                        {
                        "__typename": "Video",
                        "video": "046793286_1764686613180",
                        "thumbnail": "046793281_1764686613182.jpg",
                        "orientation": null
                        }
                    ],
                    "verified": 1,
                    "special": null,
                    "employee_user_id": 4679328,
                    "mgr_user_id": 4679328,
                    "unique_listing": 0,
                    "advertiser_type": null,
                    "appraisal_id": null,
                    "appraisal": null,
                    "virtual_tour_link": "",
                    "project_id": null,
                    "approved": 0,
                    "native": null,
                    "gh_id": null,
                    "private_listing": false,
                    "blur": null,
                    "location_circle_radius": null,
                    "width": null,
                    "length": null,
                    "water_availability": true,
                    "electrical_availability": true,
                    "drainage_availability": false,
                    "private_roof": false,
                    "apartment_in_villa": false,
                    "two_entrances": false,
                    "special_entrance": true,
                    "daily_rentable": false,
                    "has_extended_details": true,
                    "extended_details": {
                        "__typename": "ListingExtendedDetails",
                        "minimum_booking_days": 0,
                        "ad_license_url": "https://eservicesredp.rega.gov.sa/public/OfficesBroker/ElanDetails/08de318a-80b5-4674-8d5d-ddda5297ff0c"
                    },
                    "hide_contact_details": false,
                    "ad_license_number": "7200781267",
                    "deed_number": "9689649121700000",
                    "rega_licensed": true,
                    "published": true,
                    "comments_enabled": false,
                    "content": "‏شقق في العارض للبنات فقط\nمساحة الشقة  50 م\n\nشقق بتشطيب فاخر جداً وفي منطقة مخدومة ✨\n\nتتميز الشقق بأمنها وخدماتها وهدؤها ، تمتاز المنطقة بالخدمة الكاملة من حيث المدارس والاسواق والشوارع الرئيسية ( شارع ابو بكر و شارع الملك عبد العزيز ) \n\nالشقة عبارة عن غرفة نوم وصالة منفصله ومطبخ راكب وجاهز ودورة مياة \nالشقة دخولها ذكي \nالمكيافات راكبة\n\n For girls only Apartments in Al-Arid\nApartment area: 50 sqm\n\nApartments with very high-end finishing, located in a fully serviced area ✨\n\nThe apartments are known for their security, services, and quiet environment. The area offers full services, including schools, markets, and main roads (Abu Bakr Street and King Abdulaziz Road).\n\nThe apartment consists of one bedroom, a separate living room, a fully installed and ready kitchen, and a bathroom.\nThe apartment has smart access.\nAll air-conditioning units are installed.",
                    "address": "شارع اسماء بنت مالك, حي العارض, مدينة الرياض, منطقة الرياض",
                    "district": "حي العارض",
                    "direction": "شمال الرياض",
                    "city": "الرياض",
                    "title": "شقة للإيجار في شارع اسماء بنت مالك, حي العارض, مدينة الرياض, منطقة الرياض",
                    "path": "/شقق-للإيجار/الرياض/شمال-الرياض/حي-العارض/شارع-اسماء-بنت-مالك-حي-العارض-مدينة-الرياض-منطقة-الرياض-6490057",
                    "uri": "شارع-اسماء-بنت-مالك-حي-العارض-مدينة-الرياض-منطقة-الرياض-6490057",
                    "original_range_price": null,
                    "plan_no": "2078/أب",
                    "parcel_no": "4143",
                    "precise_location_request_status": "approved",
                    "precise_location_request_approved_at": null,
                    "is_auction": null,
                    "document_pdf": null,
                    "rega_total_price": 40000,
                    "rega_meter_price": null
                },
    """
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
            dict_item["id"] = listing_data.get("id")
            dict_item["title"] = listing_data.get("title")
            dict_item["price"] = listing_data.get("price")
            dict_item["area_sqm"] = listing_data.get("area")
            dict_item["num_bedrooms"] = listing_data.get("beds")
            dict_item["num_bathrooms"] = listing_data.get("wc")
            dict_item["num_living_rooms"] = listing_data.get("livings")
            dict_item["zoning"] = listing_data.get("type")
            dict_item["street-width"] = listing_data.get("street_width")
            dict_item["category"] = get_category_details(
                str(listing_data.get("category"))
            )

            dict_item["city"] = listing_data.get("city")
            dict_item["district"] = listing_data.get("district")
            dict_item["address"] = listing_data.get("address")
            dict_item["description"] = listing_data.get("content")
            dict_item["latitude"] = listing_data.get("location", {}).get("lat")
            dict_item["longitude"] = listing_data.get("location", {}).get("lng")
            dict_item["images"] = listing_data.get("imgs", [])
            dict_item["videos"] = [
                video.get("video") for video in listing_data.get("videos", [])
            ]
            output.append(dict_item)

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
