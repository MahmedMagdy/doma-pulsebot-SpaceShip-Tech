import csv
import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger(__name__)
@dataclass(frozen=True)
class VipRecord:
    abbreviation: str
    sector: str
    rating: str
    meaning_en: str
    meaning_ar: str


VIP_DATA_CACHE: dict[str, VipRecord] = {}
VIP_DATA_LOCK = threading.Lock()


def extract_keyword_from_row(row: list) -> str:
    """
    Header-agnostic keyword extraction from a raw CSV row.

    Rules:
    - Iterate cells left-to-right.
    - Return the FIRST non-empty cell that has length > 1 and contains English letters.
    - Ignore empty/symbol-only cells.
    """
    if not isinstance(row, list):
        return ""
    for cell in row:
        value = str(cell or "").strip()
        if len(value) <= 1:
            continue
        if not re.search(r"[A-Za-z]", value):
            continue
        return value
    return ""


def sanitize_and_build_domain(raw_keyword: str) -> str:
    """
    Strictly sanitize user/raw keyword and return a safe .me domain.

    - trim + lower
    - remove trailing '.me' exactly once
    - keep only [a-z0-9-]
    - normalize repeated hyphens and edge hyphens
    """
    normalized = str(raw_keyword or "").strip().lower()
    if not normalized:
        return ""
    base = normalized.removesuffix(".me")
    clean_base_word = re.sub(r"[^a-z0-9-]", "", base)
    clean_base_word = re.sub(r"-{2,}", "-", clean_base_word).strip("-")
    if not clean_base_word:
        return ""
    return f"{clean_base_word}.me"


def load_vip_database(folder: Path) -> dict[str, VipRecord]:
    records: dict[str, VipRecord] = {}
    if not folder.exists() or not folder.is_dir():
        LOGGER.warning("VIP data folder missing: %s", folder)
        return records

    # Intentionally load all CSV files in vip_data/ for dynamic growth without naming constraints.
    csv_paths = sorted(folder.glob("*.[cC][sS][vV]"))
    if not csv_paths:
        LOGGER.warning("No VIP CSV files found in: %s", folder)
        return records

    for csv_path in csv_paths:
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                for row in reader:
                    if not isinstance(row, list) or not row:
                        continue
                    raw_keyword = extract_keyword_from_row(row)
                    full_domain = sanitize_and_build_domain(raw_keyword)
                    if not full_domain:
                        continue
                    abbreviation = full_domain.removesuffix(".me")
                    if not abbreviation:
                        continue

                    if abbreviation in records:
                        LOGGER.warning(
                            "Duplicate VIP abbreviation '%s' in %s; keeping first occurrence",
                            abbreviation,
                            csv_path.name,
                        )
                        continue

                    records[abbreviation] = VipRecord(
                        abbreviation=abbreviation,
                        sector=str(row[1] if len(row) > 1 else "").strip(),
                        rating=str(row[2] if len(row) > 2 else "").strip(),
                        meaning_en=str(row[3] if len(row) > 3 else "").strip(),
                        meaning_ar=str(row[4] if len(row) > 4 else "").strip(),
                    )
        except OSError as exc:
            LOGGER.warning("Failed reading VIP CSV %s: %s", csv_path.name, exc)
            continue
        except csv.Error as exc:
            LOGGER.warning("Malformed VIP CSV %s: %s", csv_path.name, exc)
            continue

    return records


def reload_vip_database(folder: Path) -> dict[str, VipRecord]:
    global VIP_DATA_CACHE
    fresh_records = load_vip_database(folder)
    with VIP_DATA_LOCK:
        VIP_DATA_CACHE = fresh_records
        return dict(VIP_DATA_CACHE)


def get_vip_database(folder: Path) -> dict[str, VipRecord]:
    global VIP_DATA_CACHE
    with VIP_DATA_LOCK:
        if not VIP_DATA_CACHE:
            VIP_DATA_CACHE = load_vip_database(folder)
        return dict(VIP_DATA_CACHE)
