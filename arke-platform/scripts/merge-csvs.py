#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
from collections import Counter

BASE_DIR = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform")
DRIVE_CSV = BASE_DIR / "data" / "drive-sites-to-arkeogis-fixed.csv"
AIRTABLE_CSV = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only.csv"
OUTPUT_CSV = BASE_DIR / "data" / "ipad-sites-combined.csv"

# Backup old files
DRIVE_BACKUP = BASE_DIR / "data" / "drive-sites-to-arkeogis-fixed.csv.backup"
AIRTABLE_BACKUP = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only.csv.backup"


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


def main() -> None:
    # Create backups
    DRIVE_CSV.rename(DRIVE_BACKUP)
    AIRTABLE_CSV.rename(AIRTABLE_BACKUP)
    print(f"Backed up {DRIVE_CSV.name} to {DRIVE_BACKUP.name}")
    print(f"Backed up {AIRTABLE_CSV.name} to {AIRTABLE_BACKUP.name}")

    # Read both CSVs
    drive_rows, drive_headers = read_csv_rows(DRIVE_BACKUP)
    airtable_rows, airtable_headers = read_csv_rows(AIRTABLE_BACKUP)

    print(f"Read {len(drive_rows)} rows from drive-sites CSV")
    print(f"Read {len(airtable_rows)} rows from airtable CSV")
    print(f"Drive headers: {drive_headers}")
    print(f"Airtable headers: {airtable_headers}")

    # Get union of all headers
    all_headers = list(dict.fromkeys(drive_headers + airtable_headers))
    print(f"Combined headers: {all_headers}")

    # Normalize rows to have all headers
    normalized_drive = normalize_rows(drive_rows, all_headers)
    normalized_airtable = normalize_rows(airtable_rows, all_headers)

    # Combine rows
    combined_rows = normalized_drive + normalized_airtable
    print(f"Combined total: {len(combined_rows)} rows")

    # Check for duplicate IDs
    id_counter = Counter(row["SITE_SOURCE_ID"] for row in combined_rows)
    duplicates = [id for id, count in id_counter.items() if count > 1]
    if duplicates:
        print(f"WARNING: Found duplicate IDs: {duplicates}")
        raise ValueError(f"Duplicate SITE_SOURCE_ID values found: {duplicates}")
    else:
        print("No duplicate IDs found")

    # Write combined CSV
    write_csv(combined_rows, OUTPUT_CSV, all_headers)
    print(f"Wrote {len(combined_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
