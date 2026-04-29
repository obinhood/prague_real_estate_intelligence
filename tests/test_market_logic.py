from datetime import datetime
from pathlib import Path
import unittest

import pandas as pd

from src.analytics.service import _prepare_frame, get_market_overview, MarketDataBundle
from src.utils.process_csv import deduce_district_and_zone, looks_like_listing_title
from src.utils.state import build_history_snapshot, reconcile_current_with_previous


class MarketLogicTests(unittest.TestCase):
    def test_postgres_schema_contains_core_tables_and_views(self):
        schema_sql = Path("src/db/postgres_schema.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS listings", schema_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS listing_snapshots", schema_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS listing_status_events", schema_sql)
        self.assertIn("CREATE OR REPLACE VIEW daily_market_metrics", schema_sql)
        self.assertIn("CREATE OR REPLACE VIEW daily_listing_movements", schema_sql)

    def test_non_listing_titles_are_rejected(self):
        self.assertFalse(looks_like_listing_title("Předchozí"))
        self.assertFalse(looks_like_listing_title("prodeje pozemků"))
        self.assertTrue(looks_like_listing_title("Prodej bytu 2+kk 54 m² Praha - Smíchov 8 490 000 Kč"))

    def test_location_separates_borough_and_district(self):
        borough, district, zone, quality = deduce_district_and_zone("Nádražní, Praha - Smíchov", "Prodej bytu 2+kk 54 m²")
        self.assertEqual(borough, "Smíchov")
        self.assertEqual(district, "Praha 5")
        self.assertEqual(zone, "Praha 5")
        self.assertEqual(quality, "ok")

    def test_reconciliation_counts_stayed_new_and_removed(self):
        previous = pd.DataFrame(
            [
                {"composite_id": "listing_a", "source": "sreality", "property_search_type": "byt", "url_id": "a", "price_czk": 100, "first_seen_at": datetime(2026, 4, 27, 9), "is_active": True, "is_removed": False},
                {"composite_id": "listing_b", "source": "sreality", "property_search_type": "byt", "url_id": "b", "price_czk": 200, "first_seen_at": datetime(2026, 4, 27, 9), "is_active": True, "is_removed": False},
            ]
        )
        current = pd.DataFrame(
            [
                {"composite_id": "listing_b", "source": "sreality", "property_search_type": "byt", "url_id": "b", "price_czk": 220},
                {"composite_id": "listing_c", "source": "sreality", "property_search_type": "byt", "url_id": "c", "price_czk": 300},
            ]
        )
        now = datetime(2026, 4, 28, 9)
        current_state, summary = reconcile_current_with_previous(current, previous, now)
        self.assertEqual(summary["new_listings"], 1)
        self.assertEqual(summary["removed_listings"], 1)
        self.assertEqual(summary["price_changes"], 1)
        removed_rows = current_state[current_state["is_removed"] == True]
        self.assertEqual(len(removed_rows), 1)
        self.assertEqual(removed_rows.iloc[0]["composite_id"], "listing_a")

    def test_history_snapshot_marks_removed_once(self):
        previous = pd.DataFrame(
            [
                {"composite_id": "listing_a", "source": "sreality", "property_search_type": "byt", "url_id": "a", "is_active": True, "is_removed": False},
                {"composite_id": "listing_old_removed", "source": "sreality", "property_search_type": "byt", "url_id": "x", "is_active": False, "is_removed": True},
            ]
        )
        current = pd.DataFrame([{"composite_id": "listing_b", "source": "sreality", "property_search_type": "byt", "url_id": "b"}])
        snapshot = build_history_snapshot(current, previous, datetime(2026, 4, 28, 9))
        removed_ids = set(snapshot[snapshot["exists_on_source"] == False]["composite_id"].tolist())
        self.assertEqual(removed_ids, {"listing_a"})

    def test_previous_available_snapshot_logic(self):
        current = _prepare_frame(
            pd.DataFrame(
                [
                    {"composite_id": "a", "price_czk": 100, "is_active": True, "last_seen_at": "2026-04-28 10:00:00", "district_name": "Smíchov", "prague_zone": "Praha 5"},
                    {"composite_id": "b", "price_czk": 200, "is_active": True, "last_seen_at": "2026-04-28 10:00:00", "district_name": "Karlín", "prague_zone": "Praha 8"},
                ]
            )
        )
        history = _prepare_frame(
            pd.DataFrame(
                [
                    {"composite_id": "a", "price_czk": 100, "exists_on_source": True, "scraped_at": "2026-04-27 10:00:00", "district_name": "Smíchov", "prague_zone": "Praha 5"},
                    {"composite_id": "a", "price_czk": 110, "exists_on_source": True, "scraped_at": "2026-04-28 10:00:00", "district_name": "Smíchov", "prague_zone": "Praha 5"},
                    {"composite_id": "b", "price_czk": 200, "exists_on_source": True, "scraped_at": "2026-04-28 10:00:00", "district_name": "Karlín", "prague_zone": "Praha 8"},
                ]
            )
        )
        overview = get_market_overview(MarketDataBundle(current, history, pd.DataFrame()), {})
        self.assertEqual(str(overview["latest_snapshot_date"]), "2026-04-28")
        self.assertEqual(str(overview["previous_snapshot_date"]), "2026-04-27")
        self.assertEqual(overview["new_listings"], 1)


if __name__ == "__main__":
    unittest.main()
