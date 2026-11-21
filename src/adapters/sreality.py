import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from src.config import CONFIG
from src.utils.helpers import clean_text, safe_float, json_dumps_safe
from src.utils.process_csv import looks_like_listing_title
from src.utils.logger import get_logger

logger = get_logger("sreality")

# Thread-local storage so each worker thread owns its own session,
# avoiding any subtle shared-mutable-state issues across threads.
_thread_local = threading.local()


def _make_session(headers: dict, timeout: int) -> requests.Session:
    """
    Build a requests.Session with:
    - HTTP keep-alive connection pooling (pool_connections=10, pool_maxsize=20)
    - A conservative urllib3-level retry for transient network errors only
      (no retry on HTTP errors — those are handled in SrealityAdapter.fetch)
    """
    session = requests.Session()
    session.headers.update(headers)
    retry = Retry(
        total=0,          # urllib3 retries disabled — we handle retries ourselves
        connect=1,        # one reconnect attempt on connection error
        read=0,
        backoff_factor=0,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,   # number of connection pools
        pool_maxsize=20,       # connections per pool
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class SrealityAdapter:
    source_name = "sreality"

    def __init__(self):
        cfg = CONFIG["sources"]["sreality"]
        self.domain = cfg["domain"]
        self.timeout = cfg["timeout_seconds"]
        self.headers = {"User-Agent": cfg["user_agent"]}
        self.property_paths = cfg["property_paths"]
        self.max_workers = CONFIG.get("max_workers_listing_details", 8)
        self.max_workers_list = CONFIG.get("max_workers_list_pages", 4)
        self.progress_every = CONFIG.get("detail_progress_log_every", 50)
        # Main-thread session for list-page crawl
        self.session = _make_session(self.headers, self.timeout)

    def _get_thread_session(self) -> requests.Session:
        """Return (or create) a per-thread session for detail-page workers."""
        if not hasattr(_thread_local, "session"):
            _thread_local.session = _make_session(self.headers, self.timeout)
        return _thread_local.session

    def fetch(self, url, retries=3, backoff=2, session=None):
        """Fetch *url* with retry logic. Uses *session* if provided, else main session."""
        s = session or self.session
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                resp = s.get(url, timeout=self.timeout)
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
        m = re.search(r"/detail/.+?/(\d+)(?:$|[/?#])", url)
        return m.group(1) if m else url

    def detect_max_pages(self, base_url):
        html = self.fetch(base_url)
        soup = BeautifulSoup(html, "html.parser")
        numbers = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            txt = clean_text(a.get_text())
            # Only count anchor text as a page number when the link itself is a pagination link
            if ("strana=" in href or "/strana-" in href or "page=" in href) and txt and txt.isdigit():
                numbers.append(int(txt))
        detected = max(numbers) if numbers else 1
        logger.info(f"Detected max pages for {base_url}: {detected}")
        return detected

    def parse_listing_cards(self, soup, property_search_type):
        rows, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "/detail/" not in href:
                continue
            full_url = self.absolute_url(href)
            url_id = self.extract_url_id(full_url)
            composite_id = f"{self.source_name}_{property_search_type}_{url_id}"
            if composite_id in seen:
                continue
            seen.add(composite_id)
            title = clean_text(a.get_text(" ", strip=True))
            if not looks_like_listing_title(title):
                continue
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
            html = self.fetch(out["property_link"], session=self._get_thread_session())
            soup = BeautifulSoup(html, "html.parser")
            page_text = clean_text(soup.get_text(" ", strip=True)) or ""
            lower = page_text.lower()
            meta_desc = soup.find("meta", attrs={"name": "description"})
            out["description"] = clean_text(meta_desc.get("content")) if meta_desc and meta_desc.get("content") else None

            if "bez realitky" in lower or "bez rk" in lower:
                out["seller_type"] = "owner"
            elif "realitní kancelář" in lower or "rk" in lower:
                out["seller_type"] = "agency"
            else:
                out["seller_type"] = None

            out["ownership_type"] = None
            for val in ["osobní", "družstevní", "státní/obecní"]:
                if val in lower:
                    out["ownership_type"] = val
                    break

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
            m_lon = re.search(r'"lon"\s*:\s*([0-9]+\.[0-9]+)', html)
            out["latitude"] = safe_float(m_lat.group(1)) if m_lat else None
            out["longitude"] = safe_float(m_lon.group(1)) if m_lon else None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Skipping detail page due to HTTP error: {out.get('property_link')} | {e}")
        except Exception as e:
            logger.warning(f"Skipping detail page after repeated failure: {out.get('property_link')} | {e}")

        for k in ["description", "seller_type", "ownership_type", "energy_class", "floor", "details_json", "latitude", "longitude"]:
            out.setdefault(k, None)
        return out

    def _scrape_property_type(self, property_search_type: str, base_url: str, max_pages_cfg) -> list:
        """
        Scrape all list pages for a single property_search_type.
        Designed to be called from a thread pool so each type runs concurrently.
        Each thread uses the shared main session (connection-pooled, thread-safe).
        """
        rows = []
        try:
            max_pages = max_pages_cfg or self.detect_max_pages(base_url)
        except Exception as e:
            logger.exception(f"{property_search_type} | failed max-page detection: {e}")
            return rows

        for page in range(1, max_pages + 1):
            try:
                html = self.fetch(f"{base_url}?strana={page}")
                soup = BeautifulSoup(html, "html.parser")
                page_rows = self.parse_listing_cards(soup, property_search_type)
                rows.extend(page_rows)
                logger.info(
                    f"{property_search_type} | page {page}/{max_pages} | found {len(page_rows)} listing rows"
                )
            except Exception as e:
                logger.exception(f"{property_search_type} | failed page {page}: {e}")
        return rows

    def scrape(self):
        logger.info("STAGE: Sreality list-page crawl started")
        max_pages_cfg = CONFIG.get("max_pages")
        all_rows = []

        # Parallelise across property types (each type scrapes its own pages sequentially
        # so pagination order is preserved; different types are fully independent).
        max_workers_list = self.max_workers_list
        logger.info(
            f"STAGE: List-page crawl using {max_workers_list} parallel property-type workers"
        )
        with ThreadPoolExecutor(max_workers=max_workers_list) as ex:
            futures = {
                ex.submit(self._scrape_property_type, pst, url, max_pages_cfg): pst
                for pst, url in self.property_paths.items()
            }
            for fut in as_completed(futures):
                pst = futures[fut]
                try:
                    pst_rows = fut.result()
                    all_rows.extend(pst_rows)
                    logger.info(f"{pst} | crawl complete | rows collected: {len(pst_rows)}")
                except Exception as e:
                    logger.exception(f"{pst} | list-page crawl failed: {e}")

        logger.info(f"STAGE: Sreality list-page crawl finished | raw rows: {len(all_rows)}")
        listing_rows = list({r['composite_id']: r for r in all_rows}.values())
        logger.info(f"STAGE: Sreality dedupe finished | unique listing rows: {len(listing_rows)}")

        if not CONFIG.get("enable_detail_scraping", True):
            logger.info("STAGE: Detail scraping disabled | returning list-page data only")
            return listing_rows

        detail_workers = self.max_workers
        logger.info(
            f"STAGE: Detail-page enrichment started | "
            f"total listings: {len(listing_rows)} | workers: {detail_workers}"
        )
        enriched, completed, total = [], 0, len(listing_rows)
        with ThreadPoolExecutor(max_workers=detail_workers) as ex:
            futures = [ex.submit(self.parse_detail_page, row) for row in listing_rows]
            for fut in as_completed(futures):
                try:
                    enriched.append(fut.result())
                except Exception as e:
                    logger.warning(f"Detail future failed: {e}")
                completed += 1
                if completed % self.progress_every == 0 or completed == total:
                    logger.info(f"STAGE: Detail-page enrichment progress | {completed}/{total}")
        logger.info(f"STAGE: Detail-page enrichment finished | enriched rows: {len(enriched)}")
        return enriched
