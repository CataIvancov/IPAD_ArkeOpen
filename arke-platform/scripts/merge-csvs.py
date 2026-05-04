#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
import sys
from collections import defaultdict

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DRIVE_CSV = BASE_DIR / "data" / "drive-sites-to-arkeogis-fixed.csv"
AIRTABLE_CSV = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only.csv"
OUTPUT_CSV = BASE_DIR / "data" / "ipad-sites-combined.csv"

# Backup old files
DRIVE_BACKUP = BASE_DIR / "data" / "drive-sites-to-arkeogis-fixed.csv.backup"
AIRTABLE_BACKUP = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only.csv.backup"

EXPECTED_OUTPUT_HEADERS = [
    "SITE_SOURCE_ID",
    "SITE_NAME",
    "LOCALISATION",
    "GEONAME_ID",
    "PROJECTION_SYSTEM",
    "LONGITUDE",
    "LATITUDE",
    "ALTITUDE",
    "CITY_CENTROID",
    "STATE_OF_KNOWLEDGE",
    "OCCUPATION",
    "STARTING_PERIOD",
    "ENDING_PERIOD",
    "MAIN_CHARAC",
    "CHARAC_LVL1",
    "CHARAC_LVL2",
    "CHARAC_LVL3",
    "CHARAC_LVL4",
    "CHARAC_EXP",
    "BIBLIOGRAPHY",
    "COMMENTS",
]

SITE_LEVEL_FIELDS = [
    "SITE_NAME",
    "LOCALISATION",
    "GEONAME_ID",
    "PROJECTION_SYSTEM",
    "LONGITUDE",
    "LATITUDE",
    "ALTITUDE",
    "CITY_CENTROID",
    "STATE_OF_KNOWLEDGE",
    "OCCUPATION",
]


def read_csv_rows(filepath: pathlib.Path) -> tuple[list[dict[str, str]], list[str]]:
    with filepath.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        headers = reader.fieldnames or []
        return [row for row in reader], headers


def normalize_rows(rows: list[dict[str, str]], headers: list[str]) -> list[dict[str, str]]:
    normalized = []
    for row in rows:
        normalized_row = {header: row.get(header, "") for header in headers}
        normalized.append(normalized_row)
    return normalized


def write_csv(rows: list[dict[str, str]], filepath: pathlib.Path, headers: list[str]) -> None:
    with filepath.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def validate_source_headers(headers: list[str], label: str) -> None:
    missing = [header for header in EXPECTED_OUTPUT_HEADERS if header not in headers]
    if missing:
        raise ValueError(f"{label} is missing required headers: {missing}")


def validate_duplicate_ids(rows: list[dict[str, str]]) -> list[str]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["SITE_SOURCE_ID"]].append(row)

    invalid_ids = []
    for site_id, site_rows in grouped.items():
        if len(site_rows) < 2:
            continue

        mismatched_fields = []
        for field in SITE_LEVEL_FIELDS:
            values = {row.get(field, "") for row in site_rows}
            if len(values) > 1:
                mismatched_fields.append(field)

        if mismatched_fields:
            invalid_ids.append(f"{site_id} ({', '.join(mismatched_fields)})")

    return invalid_ids


def main() -> None:
    if DRIVE_BACKUP.exists() or AIRTABLE_BACKUP.exists():
        raise FileExistsError("Refusing to overwrite existing backup files. Clean up *.backup before rerunning.")

    # Create backups
    DRIVE_CSV.rename(DRIVE_BACKUP)
    AIRTABLE_CSV.rename(AIRTABLE_BACKUP)
    print(f"Backed up {DRIVE_CSV.name} to {DRIVE_BACKUP.name}")
    print(f"Backed up {AIRTABLE_CSV.name} to {AIRTABLE_BACKUP.name}")

    try:
        # Read both CSVs
        drive_rows, drive_headers = read_csv_rows(DRIVE_BACKUP)
        airtable_rows, airtable_headers = read_csv_rows(AIRTABLE_BACKUP)

        print(f"Read {len(drive_rows)} rows from drive-sites CSV")
        print(f"Read {len(airtable_rows)} rows from airtable CSV")
        print(f"Drive headers: {drive_headers}")
        print(f"Airtable headers: {airtable_headers}")

        validate_source_headers(drive_headers, "drive-sites-to-arkeogis-fixed.csv")
        validate_source_headers(airtable_headers, "airtable-to-arkeogis-v4-site-only.csv")

        # Normalize rows onto the 21-column combined schema.
        normalized_drive = normalize_rows(drive_rows, EXPECTED_OUTPUT_HEADERS)
        normalized_airtable = normalize_rows(airtable_rows, EXPECTED_OUTPUT_HEADERS)

        # Combine rows
        combined_rows = normalized_drive + normalized_airtable
        print(f"Combined total: {len(combined_rows)} rows")

        # Repeated SITE_SOURCE_ID values are allowed only when site-level fields stay identical.
        invalid_duplicates = validate_duplicate_ids(combined_rows)
        if invalid_duplicates:
            raise ValueError(
                "Duplicate SITE_SOURCE_ID groups with conflicting site-level fields found: "
                + ", ".join(invalid_duplicates)
            )

        print("Duplicate SITE_SOURCE_ID groups validated")

        # Write combined CSV
        write_csv(combined_rows, OUTPUT_CSV, EXPECTED_OUTPUT_HEADERS)
        print(f"Wrote {len(combined_rows)} rows to {OUTPUT_CSV}")
    except Exception:
        DRIVE_BACKUP.rename(DRIVE_CSV)
        AIRTABLE_BACKUP.rename(AIRTABLE_CSV)
        print("Restored source CSVs after failure", file=sys.stderr)
        raise
    else:
        DRIVE_BACKUP.rename(DRIVE_CSV)
        AIRTABLE_BACKUP.rename(AIRTABLE_CSV)
        print("Restored source CSVs after successful merge")


if __name__ == "__main__":
    main()
