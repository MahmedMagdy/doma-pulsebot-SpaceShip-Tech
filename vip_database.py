import csv
import logging
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
ABBREVIATION_HEADER_TOKENS = {"الاختصار", "abbreviation"}
HEADER_ROW_TOKENS = {
    "الاختصار",
    "abbreviation",
    "القطاع",
    "sector",
    "التقييم",
    "rating",
    "المعنى بالإنجليزي",
    "المعنى بالإنجليزية",
    "meaning in english",
    "المعنى بالعربي",
    "المعنى بالعربية",
    "meaning in arabic",
}
POSITIONAL_COLUMN_COUNT = 5


def _first_header(headers: set[str], *candidates: str) -> Optional[str]:
    for candidate in candidates:
        if candidate in headers:
            return candidate
    return None


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
                all_rows = list(reader)
                if not all_rows:
                    continue

                first = all_rows[0]
                normalized_headers = {str(col).strip() for col in first}
                has_named_headers = any(
                    token in normalized_headers
                    for token in (
                        *ABBREVIATION_HEADER_TOKENS,
                        "القطاع",
                        "Sector",
                        "التقييم",
                        "Rating",
                    )
                )

                if has_named_headers:
                    handle.seek(0)
                    dict_reader = csv.DictReader(handle)
                    if not dict_reader.fieldnames:
                        LOGGER.warning("Skipping VIP CSV without headers: %s", csv_path.name)
                        continue

                    headers = {header.strip() for header in dict_reader.fieldnames if isinstance(header, str)}
                    abbr_key = _first_header(headers, "الاختصار", "Abbreviation")
                    sector_key = _first_header(headers, "القطاع", "Sector")
                    rating_key = _first_header(headers, "التقييم", "Rating")
                    meaning_en_key = _first_header(
                        headers,
                        "المعنى بالإنجليزي",
                        "المعنى بالإنجليزية",
                        "Meaning in English",
                    )
                    meaning_ar_key = _first_header(
                        headers,
                        "المعنى بالعربي",
                        "المعنى بالعربية",
                        "Meaning in Arabic",
                    )

                    if abbr_key is None:
                        LOGGER.warning("Skipping VIP CSV missing abbreviation column: %s", csv_path.name)
                        continue

                    for row in dict_reader:
                        if not isinstance(row, dict):
                            continue
                        abbreviation = str(row.get(abbr_key) or "").strip().lower()
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
                            sector=str(row.get(sector_key) or "").strip() if sector_key else "",
                            rating=str(row.get(rating_key) or "").strip() if rating_key else "",
                            meaning_en=str(row.get(meaning_en_key) or "").strip() if meaning_en_key else "",
                            meaning_ar=str(row.get(meaning_ar_key) or "").strip() if meaning_ar_key else "",
                        )
                else:
                    for idx, row in enumerate(all_rows):
                        if len(row) < 1:
                            continue
                        if idx == 0:
                            first_row_tokens = {
                                str(col).strip().lower()
                                for col in row[:POSITIONAL_COLUMN_COUNT]
                            }
                            if first_row_tokens & HEADER_ROW_TOKENS:
                                continue
                        abbreviation = str(row[0]).strip().lower()
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
