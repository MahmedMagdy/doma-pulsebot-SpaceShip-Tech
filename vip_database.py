import csv
import logging
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

    csv_paths = sorted(folder.glob("DOM*.[cC][sS][vV]"))
    if not csv_paths:
        LOGGER.warning("No VIP CSV files found in: %s", folder)
        return records

    for csv_path in csv_paths:
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    LOGGER.warning("Skipping VIP CSV without headers: %s", csv_path.name)
                    continue

                headers = {header.strip() for header in reader.fieldnames if isinstance(header, str)}
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

                for row in reader:
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
        except OSError as exc:
            LOGGER.warning("Failed reading VIP CSV %s: %s", csv_path.name, exc)
            continue
        except csv.Error as exc:
            LOGGER.warning("Malformed VIP CSV %s: %s", csv_path.name, exc)
            continue

    return records
