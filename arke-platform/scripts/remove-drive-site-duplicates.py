#!/usr/bin/env python3

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DRIVE_CSV = ROOT / "data" / "drive-sites-to-arkeogis.csv"
DUPLICATES_CSV = ROOT / "data" / "airtable-drive-site-duplicates.csv"
REPORT_TXT = ROOT / "data" / "drive-sites-to-arkeogis-report.txt"


def load_drive_rows() -> list[dict[str, str]]:
    with DRIVE_CSV.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def load_duplicate_drive_ids() -> set[str]:
    with DUPLICATES_CSV.open("r", encoding="utf-8", newline="") as handle:
        return {row["DRIVE_SITE_SOURCE_ID"].strip() for row in csv.DictReader(handle) if row["DRIVE_SITE_SOURCE_ID"].strip()}


def write_drive_rows(rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with DRIVE_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def build_report(original_count: int, removed_rows: list[dict[str, str]], kept_rows: list[dict[str, str]]) -> str:
    with_coords = sum(1 for row in kept_rows if row.get("LONGITUDE", "").strip() and row.get("LATITUDE", "").strip())
    lines = [
        "Drive sites -> ArkeoGIS report",
        "",
        f"Original rows: {original_count}",
        f"Removed duplicate rows: {len(removed_rows)}",
        f"Rows kept: {len(kept_rows)}",
        f"Rows with coordinates: {with_coords}",
        f"Output: {DRIVE_CSV}",
        "",
        "Removed duplicates:",
    ]
    for row in removed_rows[:25]:
        lines.append(f"- {row['SITE_NAME']} | {row['SITE_SOURCE_ID']} | {row.get('LOCALISATION', '')}")
    if len(removed_rows) > 25:
        lines.append(f"- ... {len(removed_rows) - 25} more")
    return "\n".join(lines) + "\n"


def main() -> int:
    drive_rows = load_drive_rows()
    duplicate_ids = load_duplicate_drive_ids()
    if not drive_rows:
        raise RuntimeError(f"No rows found in {DRIVE_CSV}")

    fieldnames = list(drive_rows[0].keys())
    kept_rows = [row for row in drive_rows if row["SITE_SOURCE_ID"] not in duplicate_ids]
    removed_rows = [row for row in drive_rows if row["SITE_SOURCE_ID"] in duplicate_ids]

    write_drive_rows(kept_rows, fieldnames)
    report = build_report(len(drive_rows), removed_rows, kept_rows)
    REPORT_TXT.write_text(report, encoding="utf-8")
    print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
