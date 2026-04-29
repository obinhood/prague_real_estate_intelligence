import json
import os
import re
import unicodedata
import pandas as pd
from typing import Dict, Optional, Tuple

from src.utils.helpers import clean_text, safe_int
from src.utils.logger import get_logger

logger = get_logger("process")

# ─── Property type labels ─────────────────────────────────────────────────────

PROPERTY_TYPE_LABELS = {
    "byt":      "byty",
    "dum":      "domy",
    "pozemek":  "pozemky",
    "komercni": "komercni prostory",
    "ostatni":  "ostatni",
}

SOURCE_SCOPE_LABELS = {
    "praha": {
        "region_name": "Praha",
        "city_name": "Praha",
        "country_name": "Czech Republic",
    },
    "stredocesky_kraj": {
        "region_name": "Středočeský kraj",
        "city_name": None,
        "country_name": "Czech Republic",
    },
}

# ─── Neighbourhood → city district mapping ────────────────────────────────────

NEIGHBOURHOOD_TO_ZONE: Dict[str, str] = {
    # Praha 1
    "Staré Město": "Praha 1", "Nové Město": "Praha 1",
    "Malá Strana": "Praha 1", "Josefov": "Praha 1", "Hradčany": "Praha 1",
    # Praha 2
    "Vinohrady": "Praha 2", "Vyšehrad": "Praha 2",
    # Praha 3
    "Žižkov": "Praha 3",
    # Praha 4
    "Nusle": "Praha 4", "Podolí": "Praha 4", "Braník": "Praha 4",
    "Krč": "Praha 4", "Lhotka": "Praha 4", "Kunratice": "Praha 4",
    "Šeberov": "Praha 4", "Libuš": "Praha 4",
    # Praha 5
    "Smíchov": "Praha 5", "Košíře": "Praha 5", "Motol": "Praha 5",
    "Hlubočepy": "Praha 5", "Radlice": "Praha 5", "Jinonice": "Praha 5",
    "Zličín": "Praha 5", "Řeporyje": "Praha 5",
    # Praha 6
    "Dejvice": "Praha 6", "Břevnov": "Praha 6", "Bubeneč": "Praha 6",
    "Střešovice": "Praha 6", "Vokovice": "Praha 6", "Veleslavín": "Praha 6",
    "Ruzyně": "Praha 6", "Suchdol": "Praha 6", "Lysolaje": "Praha 6",
    "Řepy": "Praha 6",
    # Praha 7
    "Holešovice": "Praha 7", "Troja": "Praha 7", "Letná": "Praha 7",
    # Praha 8
    "Karlín": "Praha 8", "Libeň": "Praha 8", "Čimice": "Praha 8",
    "Dolní Chabry": "Praha 8", "Kobylisy": "Praha 8", "Bohnice": "Praha 8",
    "Ďáblice": "Praha 8",
    # Praha 9
    "Vysočany": "Praha 9", "Prosek": "Praha 9", "Střížkov": "Praha 9",
    "Hloubětín": "Praha 9", "Hrdlořezy": "Praha 9",
    # Praha 10
    "Strašnice": "Praha 10", "Vršovice": "Praha 10", "Malešice": "Praha 10",
    "Záběhlice": "Praha 10", "Petrovice": "Praha 10",
    # Praha 11
    "Chodov": "Praha 11", "Háje": "Praha 11",
    # Praha 12
    "Modřany": "Praha 12", "Kamýk": "Praha 12", "Písnice": "Praha 12",
    # Praha 13
    "Stodůlky": "Praha 13", "Nové Butovice": "Praha 13",
    # Praha 14
    "Kyje": "Praha 14",
    # Praha 15
    "Hostivař": "Praha 15", "Horní Měcholupy": "Praha 15",
    # Praha 16
    "Zbraslav": "Praha 16", "Radotín": "Praha 16",
    # Praha 17
    "Řepy": "Praha 17",
    # Praha 18
    "Čakovice": "Praha 18", "Letňany": "Praha 18", "Třeboradice": "Praha 18",
    # Praha 19
    "Satalice": "Praha 19", "Kbely": "Praha 19",
    # Praha 20
    "Horní Počernice": "Praha 20",
    # Praha 21
    "Klánovice": "Praha 21", "Újezd nad Lesy": "Praha 21", "Běchovice": "Praha 21",
    # Praha 22
    "Benice": "Praha 22", "Uhříněves": "Praha 22",
}

ZONE_TO_BOROUGHS: Dict[str, set] = {}
for _borough, _zone in NEIGHBOURHOOD_TO_ZONE.items():
    ZONE_TO_BOROUGHS.setdefault(_zone, set()).add(_borough)

# ─── Street → (borough, district) lookup ─────────────────────────────────────
# Keys are canonical Czech spellings. A normalised (diacritics-stripped,
# lowercase) shadow dict is built at module load time for fuzzy matching.
#
# Add entries as you discover streets in your data; they are looked up as
# substrings of the normalised address/title text, longest-first to avoid
# short names shadowing longer ones.

STREET_TO_LOCATION: Dict[str, Tuple[str, str]] = {
    # ── Praha 1 ──────────────────────────────────────────────────────────────
    "Václavské náměstí":        ("Nové Město",  "Praha 1"),
    "Na Příkopě":               ("Staré Město", "Praha 1"),
    "Národní třída":            ("Nové Město",  "Praha 1"),
    "Národní":                  ("Nové Město",  "Praha 1"),
    "Vodičkova":                ("Nové Město",  "Praha 1"),
    "Spálená":                  ("Nové Město",  "Praha 1"),
    "Štěpánská":                ("Nové Město",  "Praha 1"),
    "Hybernská":                ("Nové Město",  "Praha 1"),
    "Revoluční":                ("Staré Město", "Praha 1"),
    "Pařížská":                 ("Josefov",     "Praha 1"),
    "Staroměstské náměstí":     ("Staré Město", "Praha 1"),
    "Celetná":                  ("Staré Město", "Praha 1"),
    "Rytířská":                 ("Staré Město", "Praha 1"),
    "Havelská":                 ("Staré Město", "Praha 1"),
    "Platnéřská":               ("Staré Město", "Praha 1"),
    "Týnská":                   ("Staré Město", "Praha 1"),
    "Nerudova":                 ("Malá Strana", "Praha 1"),
    "Karmelitská":              ("Malá Strana", "Praha 1"),
    "Mostecká":                 ("Malá Strana", "Praha 1"),
    "Malostranské náměstí":     ("Malá Strana", "Praha 1"),
    "Valdštejnská":             ("Malá Strana", "Praha 1"),
    "Tomášská":                 ("Malá Strana", "Praha 1"),
    "U Lužického semináře":     ("Malá Strana", "Praha 1"),
    "Míšeňská":                 ("Malá Strana", "Praha 1"),
    "Loretánská":               ("Hradčany",    "Praha 1"),
    "Hradčanské náměstí":       ("Hradčany",    "Praha 1"),
    "Keplerova":                ("Hradčany",    "Praha 1"),

    # ── Praha 2 ──────────────────────────────────────────────────────────────
    "Korunní":                  ("Vinohrady",   "Praha 2"),
    "Mánesova":                 ("Vinohrady",   "Praha 2"),
    "Blanická":                 ("Vinohrady",   "Praha 2"),
    "Polská":                   ("Vinohrady",   "Praha 2"),
    "Chopinova":                ("Vinohrady",   "Praha 2"),
    "Italská":                  ("Vinohrady",   "Praha 2"),
    "Římská":                   ("Vinohrady",   "Praha 2"),
    "Slezská":                  ("Vinohrady",   "Praha 2"),
    "Máchova":                  ("Vinohrady",   "Praha 2"),
    "Anglická":                 ("Vinohrady",   "Praha 2"),
    "Francouzská":              ("Vinohrady",   "Praha 2"),
    "Americká":                 ("Vinohrady",   "Praha 2"),
    "Londýnská":                ("Vinohrady",   "Praha 2"),
    "Belgická":                 ("Vinohrady",   "Praha 2"),
    "Uruguayská":               ("Vinohrady",   "Praha 2"),
    "Španělská":                ("Vinohrady",   "Praha 2"),
    "Jugoslávská":              ("Vinohrady",   "Praha 2"),
    "Lublaňská":                ("Nové Město",  "Praha 2"),
    "Rašínovo nábřeží":         ("Nové Město",  "Praha 2"),
    "Rašínova":                 ("Nové Město",  "Praha 2"),
    "Vyšehradská":              ("Nové Město",  "Praha 2"),
    "Neklanova":                ("Vyšehrad",    "Praha 2"),
    "Sokolská":                 ("Nové Město",  "Praha 2"),
    "Ječná":                    ("Nové Město",  "Praha 2"),
    "Žitná":                    ("Nové Město",  "Praha 2"),
    "Legerova":                 ("Nové Město",  "Praha 2"),
    "Mánesovo nábřeží":         ("Nové Město",  "Praha 2"),

    # ── Praha 3 ──────────────────────────────────────────────────────────────
    "Seifertova":               ("Žižkov",      "Praha 3"),
    "Žižkovo náměstí":          ("Žižkov",      "Praha 3"),
    "Chelčického":              ("Žižkov",      "Praha 3"),
    "Chlumova":                 ("Žižkov",      "Praha 3"),
    "Jičínská":                 ("Žižkov",      "Praha 3"),
    "Ondříčkova":               ("Žižkov",      "Praha 3"),
    "Pešlova":                  ("Žižkov",      "Praha 3"),
    "Lupáčova":                 ("Žižkov",      "Praha 3"),
    "Koněvova":                 ("Žižkov",      "Praha 3"),
    "Prokopova":                ("Žižkov",      "Praha 3"),
    "Husitská":                 ("Žižkov",      "Praha 3"),
    "Tachovské náměstí":        ("Žižkov",      "Praha 3"),
    "Roháčova":                 ("Žižkov",      "Praha 3"),
    "Bořivojova":               ("Žižkov",      "Praha 3"),
    "Jeseniova":                ("Žižkov",      "Praha 3"),
    "Sudoměřská":               ("Žižkov",      "Praha 3"),
    "Žerotínova":               ("Žižkov",      "Praha 3"),
    "Kubelíkova":               ("Žižkov",      "Praha 3"),
    "Náměstí Jiřího z Poděbrad":("Vinohrady",   "Praha 3"),
    "Mánesova":                 ("Vinohrady",   "Praha 2"),  # note: Praha 2 border
    "Dykova":                   ("Vinohrady",   "Praha 3"),
    "Perunova":                 ("Vinohrady",   "Praha 3"),

    # ── Praha 4 ──────────────────────────────────────────────────────────────
    "Na Pankráci":              ("Nusle",       "Praha 4"),
    "Budějovická":              ("Michle",      "Praha 4"),
    "Vyskočilova":              ("Michle",      "Praha 4"),
    "Antala Staška":            ("Nusle",       "Praha 4"),
    "Milevská":                 ("Nusle",       "Praha 4"),
    "Novodvorská":              ("Braník",      "Praha 4"),
    "K Libuši":                 ("Libuš",       "Praha 4"),
    "Krčská":                   ("Krč",         "Praha 4"),
    "V Zátiší":                 ("Podolí",      "Praha 4"),
    "Podolské nábřeží":         ("Podolí",      "Praha 4"),
    "Branická":                 ("Braník",      "Praha 4"),
    "Olbrachtova":              ("Nusle",       "Praha 4"),
    "Táborská":                 ("Nusle",       "Praha 4"),
    "Pujmanové":                ("Nusle",       "Praha 4"),

    # ── Praha 5 ──────────────────────────────────────────────────────────────
    "Nádražní":                 ("Smíchov",     "Praha 5"),
    "Plzeňská":                 ("Smíchov",     "Praha 5"),
    "Ostrovského":              ("Smíchov",     "Praha 5"),
    "Štefánikova":              ("Smíchov",     "Praha 5"),
    "Radlická":                 ("Smíchov",     "Praha 5"),
    "Holečkova":                ("Smíchov",     "Praha 5"),
    "Stroupežnického":          ("Smíchov",     "Praha 5"),
    "Arbesovo náměstí":         ("Smíchov",     "Praha 5"),
    "Lidická":                  ("Smíchov",     "Praha 5"),
    "Preslova":                 ("Smíchov",     "Praha 5"),
    "Vrchlického":              ("Smíchov",     "Praha 5"),
    "Šmídkova":                 ("Smíchov",     "Praha 5"),
    "Plzeňské náměstí":         ("Smíchov",     "Praha 5"),
    "Na Větrníku":              ("Košíře",      "Praha 5"),
    "Náměstí 14. října":        ("Smíchov",     "Praha 5"),

    # ── Praha 6 ──────────────────────────────────────────────────────────────
    "Dejvická":                 ("Dejvice",     "Praha 6"),
    "Bubenečská":               ("Bubeneč",     "Praha 6"),
    "Podbabská":                ("Bubeneč",     "Praha 6"),
    "Na Ořechovce":             ("Dejvice",     "Praha 6"),
    "Wuchterlova":              ("Dejvice",     "Praha 6"),
    "Terronská":                ("Bubeneč",     "Praha 6"),
    "Jugoslávských partyzánů":  ("Dejvice",     "Praha 6"),
    "Rooseveltova":             ("Dejvice",     "Praha 6"),
    "Střešovická":              ("Střešovice",  "Praha 6"),
    "Patočkova":                ("Střešovice",  "Praha 6"),
    "Na Bateriích":             ("Dejvice",     "Praha 6"),
    "Thákurova":                ("Dejvice",     "Praha 6"),
    "Horoměřická":              ("Vokovice",    "Praha 6"),
    "Nad Šárkou":               ("Dejvice",     "Praha 6"),
    "Nad Paťankou":             ("Bubeneč",     "Praha 6"),
    "Evropská":                 ("Dejvice",     "Praha 6"),

    # ── Praha 7 ──────────────────────────────────────────────────────────────
    "Milady Horákové":          ("Holešovice",  "Praha 7"),
    "Dukelských hrdinů":        ("Holešovice",  "Praha 7"),
    "Veletržní":                ("Holešovice",  "Praha 7"),
    "Ortenovo náměstí":         ("Holešovice",  "Praha 7"),
    "Jablonského":              ("Holešovice",  "Praha 7"),
    "Letohradská":              ("Holešovice",  "Praha 7"),
    "Kamenická":                ("Holešovice",  "Praha 7"),
    "Kostelní":                 ("Holešovice",  "Praha 7"),
    "Veverkova":                ("Holešovice",  "Praha 7"),
    "Letenské náměstí":         ("Letná",       "Praha 7"),
    "Nábřeží Kapitána Jaroše":  ("Holešovice",  "Praha 7"),
    "Strojnická":               ("Holešovice",  "Praha 7"),
    "Nad Královskou oborou":    ("Bubeneč",     "Praha 7"),
    "U Průhonu":                ("Holešovice",  "Praha 7"),

    # ── Praha 8 ──────────────────────────────────────────────────────────────
    "Sokolovská":               ("Karlín",      "Praha 8"),
    "Thámova":                  ("Karlín",      "Praha 8"),
    "Pobřežní":                 ("Karlín",      "Praha 8"),
    "Vítkova":                  ("Karlín",      "Praha 8"),
    "Prvního pluku":            ("Karlín",      "Praha 8"),
    "Křižíkova":                ("Karlín",      "Praha 8"),
    "Rohanské nábřeží":         ("Karlín",      "Praha 8"),
    "Karlínské náměstí":        ("Karlín",      "Praha 8"),
    "Zenklova":                 ("Libeň",       "Praha 8"),
    "Ústecká":                  ("Kobylisy",    "Praha 8"),
    "Kyselova":                 ("Kobylisy",    "Praha 8"),
    "Davídkova":                ("Kobylisy",    "Praha 8"),
    "Střelničná":               ("Kobylisy",    "Praha 8"),
    "Čimická":                  ("Čimice",      "Praha 8"),
    "Ďáblická":                 ("Ďáblice",     "Praha 8"),

    # ── Praha 9 ──────────────────────────────────────────────────────────────
    "Prosecká":                 ("Prosek",      "Praha 9"),
    "Vysočanská":               ("Vysočany",    "Praha 9"),
    "Freyova":                  ("Vysočany",    "Praha 9"),
    "Poděbradská":              ("Hloubětín",   "Praha 9"),
    "Kolbenova":                ("Vysočany",    "Praha 9"),
    "Chlumecká":                ("Hloubětín",   "Praha 9"),
    "Mladoboleslavská":         ("Vysočany",    "Praha 9"),

    # ── Praha 10 ─────────────────────────────────────────────────────────────
    "Vršovická":                ("Vršovice",    "Praha 10"),
    "Kodaňská":                 ("Vršovice",    "Praha 10"),
    "Ruská":                    ("Vršovice",    "Praha 10"),
    "Bulharská":                ("Vršovice",    "Praha 10"),
    "Švehlova":                 ("Záběhlice",   "Praha 10"),
    "Přípotoční":               ("Malešice",    "Praha 10"),
    "Počernická":               ("Malešice",    "Praha 10"),
    "Limuzská":                 ("Malešice",    "Praha 10"),
    "Průběžná":                 ("Strašnice",   "Praha 10"),

    # ── Praha 11 ─────────────────────────────────────────────────────────────
    "Opatovická":               ("Chodov",      "Praha 11"),
    "Hviezdoslavova":           ("Háje",        "Praha 11"),
    "Ke Kateřinkám":            ("Chodov",      "Praha 11"),

    # ── Praha 12 ─────────────────────────────────────────────────────────────
    "Modřanská":                ("Modřany",     "Praha 12"),
    "Generála Šišky":           ("Modřany",     "Praha 12"),
    "Kamýcká":                  ("Kamýk",       "Praha 12"),

    # ── Praha 13 ─────────────────────────────────────────────────────────────
    "Stodůlecká":               ("Stodůlky",    "Praha 13"),
    "Seydlerova":               ("Stodůlky",    "Praha 13"),
    "Bucharova":                ("Stodůlky",    "Praha 13"),

    # ── Praha 14 ─────────────────────────────────────────────────────────────
    "Broumarská":               ("Kyje",        "Praha 14"),
    "Černokostelecká":          ("Kyje",        "Praha 14"),
}

# ─── Postal code (PSČ) → district ────────────────────────────────────────────
# Czech postal codes have a reliable 1-to-1 or few-to-1 mapping with Prague
# city districts. The first three digits of the PSČ are enough to pin down the
# district in almost all cases — more reliable than street matching.

PSC_TO_DISTRICT: Dict[str, str] = {
    "110": "Praha 1",
    "118": "Praha 1",
    "119": "Praha 1",
    "120": "Praha 2",
    "121": "Praha 2",
    "128": "Praha 2",
    "130": "Praha 3",
    "140": "Praha 4",
    "141": "Praha 4",
    "142": "Praha 4",
    "147": "Praha 4",
    "149": "Praha 4",
    "150": "Praha 5",
    "152": "Praha 5",
    "153": "Praha 5",
    "154": "Praha 5",
    "155": "Praha 13",
    "158": "Praha 13",
    "160": "Praha 6",
    "161": "Praha 6",
    "162": "Praha 6",
    "163": "Praha 6",
    "164": "Praha 6",
    "165": "Praha 6",
    "169": "Praha 6",
    "170": "Praha 7",
    "171": "Praha 7",
    "180": "Praha 8",
    "181": "Praha 8",
    "182": "Praha 8",
    "184": "Praha 8",
    "190": "Praha 9",
    "191": "Praha 9",
    "192": "Praha 9",
    "193": "Praha 9",
    "194": "Praha 14",
    "100": "Praha 10",
    "101": "Praha 10",
    "102": "Praha 10",
    "103": "Praha 10",
    "104": "Praha 10",
    "108": "Praha 10",
    "109": "Praha 10",
    "148": "Praha 11",
    "149": "Praha 11",
    "143": "Praha 12",
    "144": "Praha 12",
    "156": "Praha 16",
    "159": "Praha 22",
}

PSC_RE = re.compile(r"\b(\d{3})\s*0*(\d{2})\b")

# ─── Compile lookups at module load (diacritics-normalised keys) ──────────────

def _strip_diacritics(text: str) -> str:
    """Return lowercase text with Czech diacritics replaced by ASCII equivalents."""
    return "".join(
        c for c in unicodedata.normalize("NFD", text.lower())
        if unicodedata.category(c) != "Mn"
    )


# Build normalised street lookup, sorted longest-first so longer street names
# match before shorter ones that might be substrings (e.g. "Národní třída"
# before "Národní").
_STREET_LOOKUP_NORM: Dict[str, Tuple[str, str]] = {
    _strip_diacritics(k): v
    for k, v in sorted(STREET_TO_LOCATION.items(), key=lambda x: -len(x[0]))
}

# ─── Location lookup helpers ──────────────────────────────────────────────────

def lookup_location_by_street(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Scan *text* for known Prague street names and return ``(borough, district)``.
    Matching is diacritics-insensitive and case-insensitive.
    Returns ``(None, None)`` when no street is recognised.
    """
    if not text:
        return None, None
    normalised = _strip_diacritics(text)
    for street_key, (borough, district) in _STREET_LOOKUP_NORM.items():
        if street_key in normalised:
            return borough, district
    return None, None


def lookup_location_by_psc(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract a Czech postal code (PSČ) from *text* and return
    ``(None, district)``.  Borough cannot be determined from the PSČ alone so
    the first return value is always ``None``.
    Returns ``(None, None)`` when no recognised PSČ is found.
    """
    if not text:
        return None, None
    for m in PSC_RE.finditer(text):
        prefix = m.group(1)
        district = PSC_TO_DISTRICT.get(prefix)
        if district:
            return None, district
    return None, None


# ─── Regex patterns ───────────────────────────────────────────────────────────

LAYOUT_RE      = re.compile(r"(\d+\+(?:kk|1))", re.IGNORECASE)
AREA_RE        = re.compile(r"(\d+(?:[.,]\d+)?)\s*m²", re.IGNORECASE)
PRICE_RE       = re.compile(r"([\d\s]+)\s*Kč", re.IGNORECASE)
PRAHA_ZONE_RE  = re.compile(r"(Praha\s*\d+)", re.IGNORECASE)
DISTRICT_RE    = re.compile(r"^Praha\s+\d+$", re.IGNORECASE)
DISTRICT_NUM_RE = re.compile(r"Praha\s*(\d+)", re.IGNORECASE)
PRICE_CONTINUATION_RE = re.compile(r"^\s+\d{3}(?:\s+\d{3})*(?:\s*Kč)?", re.IGNORECASE)

BAD_DISTRICT_PHRASES = [
    "Cena na vyžádání", "Cena na vyzadani", "na vyžádání", "na vyzadani",
    "včetně", "vcetne", "bez provize", "rezervováno", "rezervovano",
]
KNOWN_NON_LISTING_TITLES = {
    "předchozí", "predchozi", "další", "dalsi", "další stránka", "dalsi stranka",
    "prodeje pozemků", "prodeje pozemku", "pronájmy bytů", "pronajmy bytu",
}
KNOWN_BOROUGH_NAMES = set(NEIGHBOURHOOD_TO_ZONE.keys())
VALID_PRAGUE_ZONE_NUMBERS = set(range(1, 23))

# ─── Derived-field enrichment constants ──────────────────────────────────────

# Prague ring classification by district number
_INNER_DISTRICTS   = {1, 2}
_CENTRAL_DISTRICTS = {3, 4, 5, 7}
_MIDDLE_DISTRICTS  = {6, 8, 9, 10}
# 11+ → outer

# Size band boundaries (m²)
_SIZE_BANDS = [
    (25,   "micro"),   # < 25
    (40,   "studio"),  # 25–40
    (60,   "small"),   # 40–60
    (90,   "medium"),  # 60–90
    (130,  "large"),   # 90–130
    (None, "xlarge"),  # 130+
]

# Price tiers (CZK)
_SALE_PRICE_TIERS = [
    (3_000_000,  "budget"),   # < 3 M
    (6_000_000,  "mid"),      # 3–6 M
    (12_000_000, "premium"),  # 6–12 M
    (None,       "luxury"),   # > 12 M
]
_RENT_PRICE_TIERS = [
    (15_000, "budget"),   # < 15 k/mo
    (30_000, "mid"),      # 15–30 k
    (60_000, "premium"),  # 30–60 k
    (None,   "luxury"),   # > 60 k
]

# Floor category boundaries
_FLOOR_CATEGORIES = [
    (0,  "ground"),    # floor == 0
    (3,  "low"),       # 1–3
    (8,  "mid"),       # 4–8
    (15, "high"),      # 9–15
    (None, "penthouse"),  # 16+
]


# ─── Utility functions ────────────────────────────────────────────────────────

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
        return {
            "has_balcony": None, "has_parking": None, "has_terrace": None,
            "has_elevator": None, "has_cellar": None,
        }
    try:
        data = json.loads(details_json)
        return {
            "has_balcony":  data.get("balcony"),
            "has_parking":  data.get("parking"),
            "has_terrace":  data.get("terrace"),
            "has_elevator": data.get("elevator"),
            "has_cellar":   data.get("cellar"),
        }
    except Exception:
        return {
            "has_balcony": None, "has_parking": None, "has_terrace": None,
            "has_elevator": None, "has_cellar": None,
        }


def normalize_prague_zone(text):
    if not text:
        return None
    text = str(text)
    for match in DISTRICT_NUM_RE.finditer(text):
        try:
            zone_number = int(match.group(1))
        except (TypeError, ValueError):
            continue
        if zone_number not in VALID_PRAGUE_ZONE_NUMBERS:
            continue
        remainder = text[match.end():]
        if PRICE_CONTINUATION_RE.match(remainder):
            continue
        return f"Praha {zone_number}"
    return None


def is_valid_prague_zone(value) -> bool:
    if not value or pd.isna(value):
        return False
    match = DISTRICT_NUM_RE.search(str(value))
    if not match:
        return False
    try:
        return int(match.group(1)) in VALID_PRAGUE_ZONE_NUMBERS
    except (TypeError, ValueError):
        return False


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


def looks_like_listing_title(text: str) -> bool:
    """
    Return True when *text* looks like a real property listing title.

    Acceptance signals (property keyword AND at least one of):
    - price in CZK
    - area in m²
    - room layout pattern (e.g. "2+kk", "3+1")

    Navigation links ("Další stránka"), category headings, and very short
    strings are rejected.  The layout pattern is included as a third signal
    because Sreality card anchors often carry layout info but not price.
    """
    cleaned = clean_text(text)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in KNOWN_NON_LISTING_TITLES:
        return False
    if len(cleaned) < 8:
        return False
    has_property_keyword = any(
        token in lowered
        for token in ["prodej", "byt", "dům", "dum", "pozemek", "komer", "prodej domu", "prodej bytu"]
    )
    if not has_property_keyword:
        return False
    has_price  = bool(PRICE_RE.search(cleaned))
    has_area   = bool(AREA_RE.search(cleaned))
    has_layout = bool(LAYOUT_RE.search(cleaned))
    return has_price or has_area or has_layout


def infer_property_type(title, property_search_type):
    if property_search_type:
        return str(property_search_type).split("_", 1)[0]
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


def infer_market_scope(property_search_type: Optional[str]) -> str:
    token = str(property_search_type or "").lower()
    if token.endswith("_sc"):
        return "stredocesky_kraj"
    return "praha"


def infer_scope_defaults(property_search_type: Optional[str]) -> Dict[str, Optional[str]]:
    return SOURCE_SCOPE_LABELS[infer_market_scope(property_search_type)].copy()


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


def _infer_borough_from_text(combined_text):
    for neighbourhood, mapped_zone in NEIGHBOURHOOD_TO_ZONE.items():
        if neighbourhood.lower() in combined_text.lower():
            return neighbourhood, mapped_zone
    return None, None


def assess_location_quality(borough_name, district_name, address_text=None, title_text=None):
    """
    Return a comma-separated list of issue codes, or ``"ok"``.

    Issue codes
    -----------
    missing_district           district_name empty or sentinel
    missing_borough            borough_name empty or sentinel
    borough_looks_like_zone    borough_name matches "Praha N" pattern
    district_looks_like_borough  district_name is a known neighbourhood name
    borough_zone_mismatch      borough maps to a different district than recorded
    zone_without_known_borough district "Praha N" but no borough found in text
    """
    combined = clean_text(" | ".join([x for x in [address_text, title_text] if x])) or ""
    issues = []

    if not district_name or district_name == "Praha - Ostatní":
        issues.append("missing_district")

    if borough_name == "Praha - Ostatní" or not borough_name:
        issues.append("missing_borough")

    if borough_name and DISTRICT_RE.match(str(borough_name)):
        issues.append("borough_looks_like_zone")

    if (
        district_name
        and district_name not in ("Praha - Ostatní",)
        and not DISTRICT_RE.match(str(district_name))
        and district_name in KNOWN_BOROUGH_NAMES
    ):
        issues.append("district_looks_like_borough")

    if borough_name and district_name and DISTRICT_RE.match(str(district_name)):
        expected_district = NEIGHBOURHOOD_TO_ZONE.get(borough_name)
        if expected_district and expected_district != district_name:
            issues.append("borough_zone_mismatch")

    if district_name and DISTRICT_RE.match(str(district_name)) and combined:
        boroughs = ZONE_TO_BOROUGHS.get(district_name, set())
        found_borough = next(
            (name for name in boroughs if name.lower() in combined.lower()), None
        )
        if boroughs and not found_borough:
            issues.append("zone_without_known_borough")

    return ",".join(sorted(set(issues))) if issues else "ok"


def deduce_district_and_zone(address_text, title_text=None):
    """
    Derive ``(borough_name, district_name, prague_zone, location_quality)``
    from a free-text address and optional title.

    Resolution order
    ----------------
    1. Neighbourhood keyword match (NEIGHBOURHOOD_TO_ZONE)
    2. Explicit "Praha N" zone regex
    3. "Praha - <name>" pattern for borough
    4. Street-name lookup (STREET_TO_LOCATION)
    5. Postal-code lookup (PSC_TO_DISTRICT)
    6. Fallback sentinels
    """
    address_text = clean_text(address_text)
    title_text   = clean_text(title_text)
    combined     = " | ".join([x for x in [address_text, title_text] if x])
    zone         = deduce_zone_from_text(combined)
    borough_name = None
    district_name = None

    # Step 1 – neighbourhood keyword
    borough_name, borough_zone = _infer_borough_from_text(combined)
    if borough_name:
        zone = zone or borough_zone
        district_name = borough_zone

    # Step 2 – "Praha - <neighbourhood>" pattern
    if not borough_name and combined:
        m = re.search(r"Praha\s*[-–]\s*([^,|]+)", combined, re.IGNORECASE)
        if m:
            guess = clean_district_text(m.group(1))
            if guess:
                borough_name  = guess
                district_name = NEIGHBOURHOOD_TO_ZONE.get(guess, zone)

    # Step 3 – street-name lookup (fills in missing borough and/or district)
    if not district_name or district_name == "Praha - Ostatní":
        st_borough, st_district = lookup_location_by_street(combined)
        if st_district:
            district_name = st_district
            zone = zone or st_district
        if st_borough and not borough_name:
            borough_name = st_borough

    # Step 4 – postal-code lookup (district only; PSČ does not encode borough)
    if not district_name or district_name == "Praha - Ostatní":
        _, psc_district = lookup_location_by_psc(combined)
        if psc_district:
            district_name = psc_district
            zone = zone or psc_district

    expected_zone = NEIGHBOURHOOD_TO_ZONE.get(borough_name) if borough_name else None
    if expected_zone:
        district_name = expected_zone
        zone = expected_zone

    if zone and not is_valid_prague_zone(zone):
        zone = None
    if district_name and district_name != "Praha - Ostatní" and not is_valid_prague_zone(district_name):
        district_name = None

    # Fallback sentinels
    if not zone:
        zone = "Praha - Ostatní"
    if not district_name:
        district_name = (
            zone if zone.startswith("Praha ") and zone != "Praha - Ostatní"
            else "Praha - Ostatní"
        )
    if not borough_name:
        borough_name = "Praha - Ostatní"
    for field_val in [borough_name, district_name, zone]:
        pass  # sentinels already handled above
    if borough_name == "Praha":
        borough_name = "Praha - Ostatní"
    if district_name == "Praha":
        district_name = "Praha - Ostatní"
    if zone == "Praha":
        zone = "Praha - Ostatní"

    location_quality = assess_location_quality(borough_name, district_name, address_text, title_text)
    return borough_name, district_name, zone, location_quality


def parse_title(title, property_search_type=None):
    title = clean_text(title)
    property_type_code = infer_property_type(title, property_search_type)
    scope_defaults = infer_scope_defaults(property_search_type)
    out = {
        "property_type_code":  property_type_code,
        "property_type":       convert_property_type_label(property_type_code),
        "layout_type":         None,
        "area_m2":             None,
        "price_czk":           None,
        "price_per_m2_czk":    None,
        "full_address":        None,
        "street_address":      None,
        "borough_name":        None,
        "district_name":       None,
        "prague_zone":         None,
        "location_quality":    "missing_district,missing_borough",
        "city_name":           scope_defaults["city_name"],
        "region_name":         scope_defaults["region_name"],
        "country_name":        scope_defaults["country_name"],
    }
    if not title:
        if infer_market_scope(property_search_type) == "praha":
            out["borough_name"]  = "Praha - Ostatní"
            out["district_name"] = "Praha - Ostatní"
            out["prague_zone"]   = "Praha - Ostatní"
        else:
            out["borough_name"]  = None
            out["district_name"] = None
            out["prague_zone"]   = None
            out["location_quality"] = "missing_district,missing_borough"
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

    # Extract the address segment sitting between area and price in the title
    address_txt = None
    try:
        after_area   = re.split(r"\d+(?:[.,]\d+)?\s*m²", title, maxsplit=1)[1]
        before_price = re.split(r"[\d\s]+\s*Kč",         after_area, maxsplit=1)[0]
        address_txt  = clean_text(before_price)
    except Exception:
        address_txt = None

    if address_txt:
        out["full_address"] = address_txt
        if "," in address_txt:
            street_part, _ = [clean_text(x) for x in address_txt.split(",", 1)]
            out["street_address"] = street_part
        else:
            out["street_address"] = address_txt

    if infer_market_scope(property_search_type) == "praha":
        borough_name, district_name, prague_zone, location_quality = deduce_district_and_zone(
            address_txt, title
        )
        out["borough_name"]     = borough_name
        out["district_name"]    = district_name
        out["prague_zone"]      = prague_zone
        out["location_quality"] = location_quality
    else:
        locality = None
        if address_txt:
            parts = [clean_text(part) for part in re.split(r",|\|", address_txt) if clean_text(part)]
            locality = parts[-1] if parts else clean_text(address_txt)
        locality = clean_text(locality)
        if locality and locality.lower().startswith("praha"):
            locality = None
        out["borough_name"] = locality
        out["district_name"] = locality
        out["prague_zone"] = None
        out["location_quality"] = "ok" if locality else "missing_district,missing_borough"
    return out


# ─── Per-row derived-field enrichment ────────────────────────────────────────

def _bedroom_count(layout_type) -> Optional[int]:
    """Return bedroom count from layout string e.g. '2+kk' → 2, '3+1' → 3."""
    if not layout_type or pd.isna(layout_type):
        return None
    m = re.match(r"(\d+)\+", str(layout_type).strip())
    return int(m.group(1)) if m else None


def _is_studio(layout_type, title="") -> Optional[bool]:
    """True for 1+kk or explicit garsonier titles."""
    if layout_type and not pd.isna(layout_type):
        lt = str(layout_type).strip().lower()
        if lt in ("1+kk", "garsoniéra", "garsoniera", "garsonier", "1+0"):
            return True
        m = re.match(r"(\d+)\+", lt)
        if m and int(m.group(1)) >= 2:
            return False
    title_lc = (title or "").lower()
    if "garson" in title_lc:
        return True
    return None


def _size_band(area_m2) -> Optional[str]:
    """Categorical size bucket based on area in m²."""
    try:
        area = float(area_m2)
    except (TypeError, ValueError):
        return None
    prev = 0
    for limit, label in _SIZE_BANDS:
        if limit is None or area < limit:
            return label
        prev = limit
    return "xlarge"


def _floor_category(floor) -> Optional[str]:
    """Categorical floor position."""
    try:
        f = int(floor)
    except (TypeError, ValueError):
        return None
    if f < 0:
        return "basement"
    for limit, label in _FLOOR_CATEGORIES:
        if limit is None or f <= limit:
            return label
    return "penthouse"


def _prague_ring(district_or_zone) -> Optional[str]:
    """Classify Praha N district into geographic ring."""
    if not district_or_zone or pd.isna(district_or_zone):
        return None
    m = DISTRICT_NUM_RE.search(str(district_or_zone))
    if not m:
        return "other"
    n = int(m.group(1))
    if n in _INNER_DISTRICTS:
        return "inner"
    if n in _CENTRAL_DISTRICTS:
        return "central"
    if n in _MIDDLE_DISTRICTS:
        return "middle"
    return "outer"


def _is_new_build(energy_class, title="") -> Optional[bool]:
    """True when energy class is A-level or title contains 'novostavba'."""
    if energy_class and not pd.isna(energy_class):
        ec = str(energy_class).strip().upper()
        if ec.startswith("A"):
            return True
    title_norm = _strip_diacritics((title or "").lower())
    if "novostavba" in title_norm or "nova stavba" in title_norm:
        return True
    return False


def _price_tier(price_czk, property_search_type) -> Optional[str]:
    """Categorical price bucket. Thresholds differ for sale vs rent."""
    try:
        price = float(price_czk)
    except (TypeError, ValueError):
        return None
    pst = str(property_search_type or "").lower()
    tiers = _RENT_PRICE_TIERS if ("rent" in pst or "pronajem" in pst or "najem" in pst) else _SALE_PRICE_TIERS
    for limit, label in tiers:
        if limit is None or price < limit:
            return label
    return tiers[-1][1]


def _amenity_score(row) -> int:
    """Count of confirmed amenities: balcony, parking, terrace, elevator, cellar."""
    cols = ["has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar"]
    total = 0
    for col in cols:
        val = row.get(col)
        if val is True or val == 1 or str(val).lower() == "true":
            total += 1
    return total


def enrich_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add analyst-useful derived columns to a processed listings DataFrame.

    New columns
    -----------
    bedroom_count       int      Number of bedrooms from layout_type
    is_studio           bool     True for 1+kk / garsonier units
    size_band           str      micro / studio / small / medium / large / xlarge
    amenity_score       int      0–5  count of confirmed amenities
    floor_category      str      basement / ground / low / mid / high / penthouse
    prague_ring         str      inner / central / middle / outer / other
    is_new_build        bool     energy class A* or title contains 'novostavba'
    price_tier          str      budget / mid / premium / luxury (sale or rent thresholds)
    """
    out = df.copy()

    out["bedroom_count"]  = out.apply(
        lambda r: _bedroom_count(r.get("layout_type")), axis=1
    )
    out["is_studio"] = out.apply(
        lambda r: _is_studio(r.get("layout_type"), r.get("title", "")), axis=1
    )
    out["size_band"] = out.apply(
        lambda r: _size_band(r.get("area_m2")), axis=1
    )
    out["amenity_score"] = out.apply(_amenity_score, axis=1)
    out["floor_category"] = out.apply(
        lambda r: _floor_category(r.get("floor")), axis=1
    )
    # prague_ring: prefer district_name, fall back to prague_zone
    out["prague_ring"] = out.apply(
        lambda r: _prague_ring(r.get("district_name") or r.get("prague_zone")), axis=1
    )
    out["is_new_build"] = out.apply(
        lambda r: _is_new_build(r.get("energy_class"), r.get("title", "")), axis=1
    )
    out["price_tier"] = out.apply(
        lambda r: _price_tier(r.get("price_czk"), r.get("property_search_type")), axis=1
    )

    logger.debug(f"Derived-field enrichment complete | rows: {len(out)}")
    return out


def process_master_csv(
    input_path:  str = "data/listings_master.csv",
    output_path: str = "data/listings_processed.csv",
):
    logger.info("STAGE: CSV processing started")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Missing input file: {input_path}")
    df = pd.read_csv(input_path)
    if df.empty:
        df.to_csv(output_path, index=False)
        return output_path

    if (
        "composite_id" not in df.columns
        and {"source", "property_search_type", "url_id"}.issubset(df.columns)
    ):
        df["composite_id"] = (
            df["source"].astype(str) + "_" +
            df["property_search_type"].astype(str) + "_" +
            df["url_id"].astype(str)
        )

    out = process_master_dataframe(df)
    out.to_csv(output_path, index=False)
    logger.info(f"STAGE: CSV processing finished | output rows: {len(out)}")
    return output_path


def process_master_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["title"] = out["title"].apply(clean_text)
    out = out[out["title"].apply(looks_like_listing_title)].copy()
    out = out.drop_duplicates(subset=["composite_id"], keep="first").reset_index(drop=True)

    parsed = pd.DataFrame([
        parse_title(t, p)
        for t, p in zip(
            out["title"].fillna(""),
            out.get("property_search_type", pd.Series([None] * len(out))),
        )
    ])
    out = pd.concat([out, parsed], axis=1)

    out["property_link"] = [
        make_property_link(u, p, s)
        for u, p, s in zip(
            out.get("url",           pd.Series([None] * len(out))),
            out.get("property_link", pd.Series([None] * len(out))),
            out.get("source",        pd.Series([None] * len(out))),
        )
    ]

    feature_df = (
        out.get("details_json", pd.Series([None] * len(out)))
        .apply(extract_detail_features)
        .apply(pd.Series)
    )
    out = pd.concat([out, feature_df], axis=1)

    out = enrich_derived_fields(out)

    column_order = [
        "composite_id", "url_id", "source", "property_search_type",
        "property_type_code", "property_type",
        "url", "property_link", "title", "timestamp", "exists",
        "layout_type", "bedroom_count", "is_studio",
        "area_m2", "size_band", "price_czk", "price_per_m2_czk", "price_tier",
        "full_address", "street_address", "borough_name", "district_name",
        "prague_zone", "prague_ring", "location_quality",
        "city_name", "region_name", "country_name",
        "latitude", "longitude", "seller_type", "floor", "floor_category",
        "ownership_type", "energy_class", "is_new_build",
        "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar",
        "amenity_score",
        "description", "details_json",
    ]
    existing = [c for c in column_order if c in out.columns]
    remaining = [c for c in out.columns if c not in existing]
    return out[existing + remaining]
