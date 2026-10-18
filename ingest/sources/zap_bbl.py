"""Ingest the ZAP BBL crosswalk (project_id <-> bbl) into the bronze layer.

Run it with:: make ingest-zap-bbl
"""

from __future__ import annotations

import logging
from pathlib import Path

from ingest import socrata
from ingest.sources.zap import load_dotenv  # reuse zap.py's .env loader

log = logging.getLogger(__name__)

SOURCE_NAME = "zap_bbl"
DATASET_ID = "2iga-a6mk"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
# project_id + bbl together are unique (a project can span several lots), so sorting
# by both gives Socrata a stable order to page through -- sorting by project_id alone
# wouldn't, since many rows share the same project_id.
ORDER_BY = "project_id, bbl"


def run(**kwargs) -> Path:
    return socrata.run_source(SOURCE_NAME, BASE_URL, order_by=ORDER_BY, **kwargs)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    run()


if __name__ == "__main__":
    main()
