import json
import os
import re
import pandas as pd

from src.utils.helpers import clean_text, safe_int
from src.utils.logger import get_logger

logger = get_logger("process")

PROPERTY_TYPE_LABELS = {
    "byt": "byty",
    "dum": "domy",
    "pozemek": "pozemky",
    "komercni": "komercni prostory",
    "ostatni": "ostatni",
}

NEIGHBOURHOOD_TO_ZONE = {
    "Staré Město": "Praha 1", "Nové Město": "Praha 1", "Malá Strana": "Praha 1", "Josefov": "Praha 1",
    "Vinohrady": "Praha 2", "Vyšehrad": "Praha 2", "Nusle": "Praha 4", "Žižkov": "Praha 3",
    "Smíchov": "Praha 5", "Košíře": "Praha 5", "Motol": "Praha 5", "Hlubočepy": "Praha 5",
    "Dejvice": "Praha 6", "Břevnov": "Praha 6", "Bubeneč": "Praha 6",
    "Holešovice": "Praha 7", "Troja": "Praha 7",
    "Karlín": "Praha 8", "Libeň": "Praha 8", "Čimice": "Praha 8", "Dolní Chabry": "Praha 8", "Kobylisy": "Praha 8",
    "Vysočany": "Praha 9", "Prosek": "Praha 9", "Střížkov": "Praha 9", "Hloubětín": "Praha 9", "Kyje": "Praha 14",
    "Strašnice": "Praha 10", "Vršovice": "Praha 10", "Malešice": "Praha 10", "Záběhlice": "Praha 10",
    "Chodov": "Praha 11", "Háje": "Praha 11",
    "Modřany": "Praha 12", "Kamýk": "Praha 12", "Stodůlky": "Praha 13",
    "Hostivař": "Praha 15", "Horní Měcholupy": "Praha 15",
    "Klánovice": "Praha 21", "Újezd nad Lesy": "Praha 21",
    "Zbraslav": "Praha 16", "Radotín": "Praha 16",
    "Běchovice": "Praha 21", "Čakovice": "Praha 18", "Letňany": "Praha 18",
    "Benice": "Praha 22", "Uhříněves": "Praha 22",
    "Kunratice": "Praha 4", "Podolí": "Praha 4", "Braník": "Praha 4", "Krč": "Praha 4",
    "Petrovice": "Praha 10", "Lhotka": "Praha 4", "Písnice": "Praha 12", "Bohnice": "Praha 8",
    "Hrdlořezy": "Praha 9", "Třeboradice": "Praha 18", "Satalice": "Praha 19", "Kbely": "Praha 19",
}

LAYOUT_RE = re.compile(r"(\d+\+(?:kk|1))", re.IGNORECASE)
AREA_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m²", re.IGNORECASE)
PRICE_RE = re.compile(r"([\d\s]+)\s*Kč", re.IGNORECASE)
PRAHA_ZONE_RE = re.compile(r"(Praha\s*\d+)", re.IGNORECASE)
BAD_DISTRICT_PHRASES = [
    "Cena na vyžádání", "Cena na vyzadani", "na vyžádání", "na vyzadani",
    "včetně", "vcetne", "bez provize", "rezervováno", "rezervovano"
]


def make_property_link(url, fallback_link=None, source=None):
    if isinstance(fallback_link, str) and fallback_link.startswith("http"):
        return fallback_link
    if not url or pd.isna(url):
        return None
    url = str(url).strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if not url.startswith("/"):
        url = "/" + url
    base = "https://www.bezrealitky.cz" if source == "bezrealitky" else "https://www.sreality.cz"
    return base + url


def extract_detail_features(details_json):
    if not details_json:
        return {"has_balcony": None, "has_parking": None, "has_terrace": None, "has_elevator": None, "has_cellar": None}
    try:
        data = json.loads(details_json)
        return {
            "has_balcony": data.get("balcony"),
            "has_parking": data.get("parking"),
            "has_terrace": data.get("terrace"),
            "has_elevator": data.get("elevator"),
            "has_cellar": data.get("cellar"),
        }
    except Exception:
        return {"has_balcony": None, "has_parking": None, "has_terrace": None, "has_elevator": None, "has_cellar": None}


def normalize_prague_zone(text):
    if not text:
        return None
    m = PRAHA_ZONE_RE.search(str(text))
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    return None


def clean_district_text(text):
    if not text:
        return None
    txt = clean_text(text)
    if not txt or txt.lower() == "praha":
        return None
    for bad in BAD_DISTRICT_PHRASES:
        txt = txt.replace(bad, "").replace(bad.lower(), "")
    txt = clean_text(txt).strip(",;- ")
    if not txt or txt.lower() == "praha":
        return None
    return txt


def infer_property_type(title, property_search_type):
    if property_search_type:
        return property_search_type
    title = (title or "").lower()
    if "byt" in title:
        return "byt"
    if "dům" in title or "dum" in title or "domu" in title:
        return "dum"
    if "pozemek" in title or "pozemku" in title:
        return "pozemek"
    if "komer" in title:
        return "komercni"
    return "ostatni"


def convert_property_type_label(code):
    return PROPERTY_TYPE_LABELS.get(code, code)


def deduce_zone_from_text(text):
    if not text:
        return None
    zone = normalize_prague_zone(text)
    if zone:
        return zone
    for neighbourhood, mapped_zone in NEIGHBOURHOOD_TO_ZONE.items():
        if neighbourhood.lower() in str(text).lower():
            return mapped_zone
    return None


def deduce_district_and_zone(address_text, title_text=None):
    address_text = clean_text(address_text)
    title_text = clean_text(title_text)
    combined = " | ".join([x for x in [address_text, title_text] if x])
    zone = deduce_zone_from_text(combined)
    found_district = None
    for neighbourhood, mapped_zone in NEIGHBOURHOOD_TO_ZONE.items():
        if neighbourhood.lower() in combined.lower():
            found_district = neighbourhood
            zone = zone or mapped_zone
            break
    if not found_district and combined:
        m = re.search(r"Praha\s*-\s*([^,|]+)", combined, re.IGNORECASE)
        if m:
            guess = clean_district_text(m.group(1))
            if guess:
                found_district = guess
    if not zone:
        zone = "Praha - Ostatní"
    if not found_district:
        found_district = zone if zone.startswith("Praha ") and zone != "Praha - Ostatní" else "Praha - Ostatní"
    if found_district == "Praha":
        found_district = "Praha - Ostatní"
    if zone == "Praha":
        zone = "Praha - Ostatní"
    return found_district, zone


def parse_title(title, property_search_type=None):
    title = clean_text(title)
    property_type_code = infer_property_type(title, property_search_type)
    out = {
        "property_type_code": property_type_code,
        "property_type": convert_property_type_label(property_type_code),
        "layout_type": None,
        "area_m2": None,
        "price_czk": None,
        "price_per_m2_czk": None,
        "full_address": None,
        "street_address": None,
        "district_name": None,
        "prague_zone": None,
        "city_name": "Praha",
        "region_name": "Praha",
        "country_name": "Czech Republic",
    }
    if not title:
        out["district_name"] = "Praha - Ostatní"
        out["prague_zone"] = "Praha - Ostatní"
        return out
    m = LAYOUT_RE.search(title)
    if m:
        out["layout_type"] = m.group(1)
    m = AREA_RE.search(title)
    if m:
        try:
            out["area_m2"] = float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    m = PRICE_RE.search(title)
    if m:
        out["price_czk"] = safe_int(m.group(1))
    if out["price_czk"] and out["area_m2"]:
        out["price_per_m2_czk"] = round(out["price_czk"] / out["area_m2"], 2)

    address_txt = None
    try:
        after_area = re.split(r"\d+(?:[.,]\d+)?\s*m²", title, maxsplit=1)[1]
        before_price = re.split(r"[\d\s]+\s*Kč", after_area, maxsplit=1)[0]
        address_txt = clean_text(before_price)
    except Exception:
        address_txt = None

    if address_txt:
        out["full_address"] = address_txt
        if "," in address_txt:
            street_part, _ = [clean_text(x) for x in address_txt.split(",", 1)]
            out["street_address"] = street_part
        else:
            out["street_address"] = address_txt

    district_name, prague_zone = deduce_district_and_zone(address_txt, title)
    out["district_name"] = district_name
    out["prague_zone"] = prague_zone
    return out


def process_master_csv(input_path="data/listings_master.csv", output_path="data/listings_processed.csv"):
    logger.info("STAGE: CSV processing started")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Missing input file: {input_path}")
    df = pd.read_csv(input_path)
    if df.empty:
        df.to_csv(output_path, index=False)
        return output_path
    if "composite_id" not in df.columns and {"source", "property_search_type", "url_id"}.issubset(df.columns):
        df["composite_id"] = df["source"].astype(str) + "_" + df["property_search_type"].astype(str) + "_" + df["url_id"].astype(str)
    parsed = pd.DataFrame([parse_title(t, p) for t, p in zip(df["title"].fillna(""), df.get("property_search_type", pd.Series([None] * len(df))))])
    out = pd.concat([df.copy(), parsed], axis=1)
    out["property_link"] = [make_property_link(u, p, s) for u, p, s in zip(out.get("url", pd.Series([None] * len(out))), out.get("property_link", pd.Series([None] * len(out))), out.get("source", pd.Series([None] * len(out))))]
    feature_df = out.get("details_json", pd.Series([None] * len(out))).apply(extract_detail_features).apply(pd.Series)
    out = pd.concat([out, feature_df], axis=1)
    column_order = [
        "composite_id", "url_id", "source", "property_search_type", "property_type_code", "property_type",
        "url", "property_link", "title", "timestamp", "exists",
        "layout_type", "area_m2", "price_czk", "price_per_m2_czk",
        "full_address", "street_address", "district_name", "prague_zone", "city_name", "region_name", "country_name",
        "latitude", "longitude", "seller_type", "floor", "ownership_type", "energy_class",
        "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar", "description", "details_json"
    ]
    existing = [c for c in column_order if c in out.columns]
    remaining = [c for c in out.columns if c not in existing]
    out = out[existing + remaining]
    out.to_csv(output_path, index=False)
    logger.info(f"STAGE: CSV processing finished | output rows: {len(out)}")
    return output_path
