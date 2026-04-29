import os


CONFIG = {
    "database_url": os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://obinhood:Qmwn1234@localhost:5432/prague_real_estate",
    ),
    "max_pages": None,
    "enable_detail_scraping": True,
    "max_workers_listing_details": 4,
    "detail_progress_log_every": 25,
    "sources": {
        "sreality": {
            "domain": "https://www.sreality.cz",
            "timeout_seconds": 45,
            "user_agent": "Mozilla/5.0",
            "property_paths": {
                # Prague
                "byt":      "https://www.sreality.cz/hledani/prodej/byty/praha",
                "dum":      "https://www.sreality.cz/hledani/prodej/domy/praha",
                "pozemek":  "https://www.sreality.cz/hledani/prodej/pozemky/praha",
                "komercni": "https://www.sreality.cz/hledani/prodej/komercni/praha",
                "ostatni":  "https://www.sreality.cz/hledani/prodej/ostatni/praha",
                # Středočeský kraj (Central Bohemia — surrounds Prague)
                "byt_sc":      "https://www.sreality.cz/hledani/prodej/byty/stredocesky-kraj",
                "dum_sc":      "https://www.sreality.cz/hledani/prodej/domy/stredocesky-kraj",
                "pozemek_sc":  "https://www.sreality.cz/hledani/prodej/pozemky/stredocesky-kraj",
                "komercni_sc": "https://www.sreality.cz/hledani/prodej/komercni/stredocesky-kraj",
            }
        },
        "bezrealitky": {
            "domain": "https://www.bezrealitky.cz",
            "timeout_seconds": 45,
            "user_agent": "Mozilla/5.0",
            "property_paths": {
                # Prague
                "byt":     "https://www.bezrealitky.cz/vypis/nabidka-prodej/byt/praha",
                "dum":     "https://www.bezrealitky.cz/vypis/nabidka-prodej/dum/praha",
                "pozemek": "https://www.bezrealitky.cz/vypis/nabidka-prodej/pozemek/praha",
                "ostatni": "https://www.bezrealitky.cz/vypis/nabidka-prodej/garaz/praha",
                # Středočeský kraj
                "byt_sc":     "https://www.bezrealitky.cz/vypis/nabidka-prodej/byt/stredocesky-kraj",
                "dum_sc":     "https://www.bezrealitky.cz/vypis/nabidka-prodej/dum/stredocesky-kraj",
                "pozemek_sc": "https://www.bezrealitky.cz/vypis/nabidka-prodej/pozemek/stredocesky-kraj",
            }
        }
    }
}
