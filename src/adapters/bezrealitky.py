import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from src.config import CONFIG
from src.utils.helpers import clean_text, safe_float, json_dumps_safe
from src.utils.process_csv import looks_like_listing_title
from src.utils.logger import get_logger

logger = get_logger("bezrealitky")


LISTING_HREF_RE = re.compile(
    r"/(?:nemovitosti-[a-z-]+|detail)/(?P<id>\d+)(?:[-/]|$)",
    re.IGNORECASE,
)
LAYOUT_HINT_RE = re.compile(r"\b\d+\+(?:kk|1)\b", re.IGNORECASE)
AREA_HINT_RE = re.compile(r"\b\d+(?:[.,]\d+)?\s*m²\b", re.IGNORECASE)
PRICE_HINT_RE = re.compile(r"\b\d[\d\s\xa0]*\s*Kč\b", re.IGNORECASE)
PROPERTY_TITLE_PREFIX_RE = re.compile(
    r"^(?P<prefix>Prodej\s+(?:bytu|domu|pozemku|gar[aá]že|gar[aá]žov[eé]ho\s+st[aá]n[ií]|komer[cč]n[ií]ho?\s+prostoru?))\s*(?P<address>.*)$",
    re.IGNORECASE,
)
HEADING_TITLE_RE = re.compile(
    r"(Prodej\s+(?:bytu|domu|pozemku|gar[aá]že|gar[aá]žov[eé]ho\s+st[aá]n[ií]|komer[cč]n[ií]ho?\s+prostoru?)\s+.+?)(?=\s+\d+\+(?:kk|1)\b|\s+\d+(?:[.,]\d+)?\s*m²\b|\s+\d[\d\s\xa0]*\s*Kč\b|$)",
    re.IGNORECASE,
)


class BezrealitkyAdapter:
    source_name = "bezrealitky"

    def __init__(self):
        cfg = CONFIG["sources"]["bezrealitky"]
        self.domain = cfg["domain"]
        self.timeout = cfg["timeout_seconds"]
        self.headers = {"User-Agent": cfg["user_agent"]}
        self.property_paths = cfg["property_paths"]
        self.max_workers = CONFIG.get("max_workers_listing_details", 4)
        self.progress_every = CONFIG.get("detail_progress_log_every", 25)
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch(self, url, retries=3, backoff=2):
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 404:
                    logger.warning(f"404 for URL: {url}")
                    raise
                last_error = e
                logger.warning(f"HTTP error attempt {attempt}/{retries} for {url}: {e}")
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                last_error = e
                logger.warning(f"Timeout/connection attempt {attempt}/{retries} for {url}: {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
        raise last_error

    def absolute_url(self, url):
        if not url:
            return None
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"{self.domain}{url if url.startswith('/') else '/' + url}"

    def extract_url_id(self, url):
        m = LISTING_HREF_RE.search(url or "")
        return m.group(1) if m else url

    def _extract_card_context_text(self, anchor):
        node = anchor
        best_text = clean_text(anchor.get_text(" ", strip=True))
        for _ in range(6):
            node = getattr(node, "parent", None)
            if node is None:
                break
            candidate = clean_text(node.get_text(" ", strip=True))
            if not candidate:
                continue
            best_text = candidate
            if PRICE_HINT_RE.search(candidate) and (
                AREA_HINT_RE.search(candidate) or LAYOUT_HINT_RE.search(candidate)
            ):
                return candidate
        return best_text

    def _strip_listing_noise(self, text):
        cleaned = clean_text(text)
        if not cleaned:
            return cleaned
        cleaned = re.sub(
            r"^(?:Předchozí|Následující|Přidat do oblíbených|Nový|Nepřehlédněte|Developerský projekt|Prodává Bezrealitky|Image)\s+",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"^(?:(?:Předchozí|Následující|Přidat do oblíbených|Nový|Nepřehlédněte|Developerský projekt|Prodává Bezrealitky|Image)\s+)+",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"\s+Hypotéka již od.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+\(\s*\d[\d\s\xa0]*Kč\s*/\s*m²\s*\)", "", cleaned, flags=re.IGNORECASE)
        return clean_text(cleaned)

    def _extract_heading_title(self, text):
        cleaned = self._strip_listing_noise(text)
        if not cleaned:
            return None
        match = HEADING_TITLE_RE.search(cleaned)
        if match:
            return clean_text(match.group(1))
        return cleaned

    def _build_listing_title(self, anchor_title, context_text):
        anchor_title = self._strip_listing_noise(anchor_title)
        context_text = self._strip_listing_noise(context_text)
        title_seed = self._extract_heading_title(anchor_title) or self._extract_heading_title(context_text) or anchor_title

        if looks_like_listing_title(title_seed) and not any(
            noise in title_seed.lower() for noise in ["předchozí", "následující", "hypotéka již od"]
        ):
            return title_seed

        if not title_seed:
            return context_text

        match = PROPERTY_TITLE_PREFIX_RE.match(title_seed)
        if not match:
            return context_text if looks_like_listing_title(context_text) else anchor_title

        prefix = clean_text(match.group("prefix"))
        address = clean_text(match.group("address"))
        layout = clean_text(LAYOUT_HINT_RE.search(context_text).group(0)) if LAYOUT_HINT_RE.search(context_text) else None
        area = clean_text(AREA_HINT_RE.search(context_text).group(0)) if AREA_HINT_RE.search(context_text) else None
        price = clean_text(PRICE_HINT_RE.search(context_text).group(0)) if PRICE_HINT_RE.search(context_text) else None

        pieces = [prefix]
        if layout:
            pieces.append(layout)
        if area:
            pieces.append(area)
        if address:
            pieces.append(address)
        if price:
            pieces.append(price)
        rebuilt = clean_text(" ".join(piece for piece in pieces if piece))
        if looks_like_listing_title(rebuilt):
            return rebuilt
        return context_text if looks_like_listing_title(context_text) else rebuilt

    def detect_max_pages(self, base_url):
        html = self.fetch(base_url)
        soup = BeautifulSoup(html, "html.parser")
        numbers = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            txt = clean_text(a.get_text())
            if ("page=" in href or "/strana-" in href) and txt and txt.isdigit():
                numbers.append(int(txt))
        detected = max(numbers) if numbers else 1
        logger.info(f"Detected max pages for {base_url}: {detected}")
        return detected

    def parse_listing_cards(self, soup, property_search_type):
        rows, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            full_url = self.absolute_url(href)
            href_l = href.lower()
            if "bezrealitky.cz" not in full_url.lower():
                continue
            if not LISTING_HREF_RE.search(href_l):
                continue
            url_id = self.extract_url_id(full_url)
            composite_id = f"{self.source_name}_{property_search_type}_{url_id}"
            if composite_id in seen:
                continue
            raw_title = clean_text(a.get_text(" ", strip=True))
            context_text = self._extract_card_context_text(a)
            title = self._build_listing_title(raw_title, context_text)
            if not looks_like_listing_title(title):
                continue
            seen.add(composite_id)
            rows.append({
                "composite_id": composite_id,
                "url_id": url_id,
                "source": self.source_name,
                "property_search_type": property_search_type,
                "url": href,
                "property_link": full_url,
                "title": title,
            })
        return rows

    def parse_detail_page(self, row):
        out = dict(row)
        try:
            html = self.fetch(out["property_link"])
            soup = BeautifulSoup(html, "html.parser")
            page_text = clean_text(soup.get_text(" ", strip=True)) or ""
            lower = page_text.lower()
            meta_desc = soup.find("meta", attrs={"name": "description"})
            out["description"] = clean_text(meta_desc.get("content")) if meta_desc and meta_desc.get("content") else None
            out["seller_type"] = "owner"
            out["ownership_type"] = "owner"

            m_energy = re.search(r"energetick[aá].{0,20}([A-G])", page_text, re.IGNORECASE)
            out["energy_class"] = m_energy.group(1).upper() if m_energy else None
            m_floor = re.search(r"podlaž[ií]\s+(\d+)", page_text, re.IGNORECASE)
            out["floor"] = m_floor.group(1) if m_floor else None

            features = {
                "balcony": ("balkon" in lower or "lodžie" in lower or "lodzie" in lower),
                "parking": ("parkování" in lower or "parkovani" in lower or "garáž" in lower or "garaz" in lower),
                "terrace": ("terasa" in lower),
                "elevator": ("výtah" in lower or "vytah" in lower),
                "cellar": ("sklep" in lower),
            }
            out["details_json"] = json_dumps_safe(features)

            m_lat = re.search(r'"lat"\s*:\s*([0-9]+\.[0-9]+)', html)
            m_lon = re.search(r'"lng"\s*:\s*([0-9]+\.[0-9]+)', html) or re.search(r'"lon"\s*:\s*([0-9]+\.[0-9]+)', html)
            out["latitude"] = safe_float(m_lat.group(1)) if m_lat else None
            out["longitude"] = safe_float(m_lon.group(1)) if m_lon else None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Skipping detail page due to HTTP error: {out.get('property_link')} | {e}")
        except Exception as e:
            logger.warning(f"Skipping detail page after repeated failure: {out.get('property_link')} | {e}")

        for k in ["description", "seller_type", "ownership_type", "energy_class", "floor", "details_json", "latitude", "longitude"]:
            out.setdefault(k, None)
        return out

    def scrape(self):
        logger.info("STAGE: Bezrealitky list-page crawl started")
        all_rows = []
        max_pages_cfg = CONFIG.get("max_pages")
        for property_search_type, base_url in self.property_paths.items():
            logger.info(f"Collecting property type: {property_search_type}")
            try:
                max_pages = max_pages_cfg or self.detect_max_pages(base_url)
            except Exception as e:
                logger.exception(f"{property_search_type} | failed max-page detection: {e}")
                continue
            for page in range(1, max_pages + 1):
                logger.info(f"{property_search_type} | scraping page {page}/{max_pages}")
                try:
                    html = self.fetch(f"{base_url}?page={page}")
                    soup = BeautifulSoup(html, "html.parser")
                    page_rows = self.parse_listing_cards(soup, property_search_type)
                    all_rows.extend(page_rows)
                    logger.info(f"{property_search_type} | page {page}/{max_pages} | found {len(page_rows)} listing rows")
                except Exception as e:
                    logger.exception(f"{property_search_type} | failed page {page}: {e}")

        logger.info(f"STAGE: Bezrealitky list-page crawl finished | raw rows: {len(all_rows)}")
        listing_rows = list({r['composite_id']: r for r in all_rows}.values())
        logger.info(f"STAGE: Bezrealitky dedupe finished | unique listing rows: {len(listing_rows)}")

        if not CONFIG.get("enable_detail_scraping", True):
            logger.info("STAGE: Detail scraping disabled | returning list-page data only")
            return listing_rows

        logger.info(f"STAGE: Detail-page enrichment started | total listings: {len(listing_rows)} | workers: {CONFIG.get('max_workers_listing_details', 4)}")
        enriched, completed, total = [], 0, len(listing_rows)
        with ThreadPoolExecutor(max_workers=CONFIG.get("max_workers_listing_details", 4)) as ex:
            futures = [ex.submit(self.parse_detail_page, row) for row in listing_rows]
            for fut in as_completed(futures):
                try:
                    enriched.append(fut.result())
                except Exception as e:
                    logger.warning(f"Detail future failed: {e}")
                completed += 1
                if completed % CONFIG.get("detail_progress_log_every", 25) == 0 or completed == total:
                    logger.info(f"STAGE: Detail-page enrichment progress | {completed}/{total}")
        logger.info(f"STAGE: Detail-page enrichment finished | enriched rows: {len(enriched)}")
        return enriched
