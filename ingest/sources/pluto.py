"""Ingest PLUTO (one row per NYC tax lot, with council/community district and lat/lon)
into the bronze layer.

Run it with:: make ingest-pluto

This is the biggest source so far (~858k rows as of this writing) -- a full run takes
longer than ZAP did. Still one bronze snapshot per day you run it; same idempotency
guarantee as every other source.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ingest import socrata
from ingest.sources.zap import load_dotenv

log = logging.getLogger(__name__)

SOURCE_NAME = "pluto"
DATASET_ID = "64uk-42ks"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
ORDER_BY = "bbl"  # one row per tax lot; bbl is unique


def run(**kwargs) -> Path:
    return socrata.run_source(SOURCE_NAME, BASE_URL, order_by=ORDER_BY, **kwargs)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    run()


if __name__ == "__main__":
    main()
