import json
import re
import pandas as pd


def clean_text(value):
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def safe_float(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    txt = clean_text(value).replace(" ", "").replace(",", ".")
    txt = re.sub(r"[^0-9.]", "", txt)
    if txt == "":
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def safe_int(value):
    val = safe_float(value)
    return int(round(val)) if val is not None else None


def json_dumps_safe(value):
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return None
