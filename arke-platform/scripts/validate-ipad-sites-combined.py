#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
import sys
from collections import defaultdict

EXPECTED_HEADERS = [
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

DEFAULT_CSV = pathlib.Path(__file__).resolve().parents[1] / "data" / "ipad-sites-combined.csv"


def load_rows(path: pathlib.Path) -> tuple[list[str], list[dict[str, str]], list[tuple[int, int]]]:
    bad_lengths = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader)
        for line_no, row in enumerate(reader, start=2):
            if len(row) != len(header):
                bad_lengths.append((line_no, len(row)))

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        dict_reader = csv.DictReader(handle, delimiter=";")
        rows = list(dict_reader)

    return header, rows, bad_lengths


def validate(path: pathlib.Path) -> list[str]:
    header, rows, bad_lengths = load_rows(path)
    errors = []

    if header != EXPECTED_HEADERS:
        errors.append(f"Header mismatch: expected {EXPECTED_HEADERS}, got {header}")

    if bad_lengths:
        errors.extend(
            f"Row {line_no} has {field_count} columns; expected {len(EXPECTED_HEADERS)}"
            for line_no, field_count in bad_lengths
        )

    grouped: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    for line_no, row in enumerate(rows, start=2):
        grouped[row["SITE_SOURCE_ID"]].append((line_no, row))

    for site_id, site_rows in grouped.items():
        if len(site_rows) < 2:
            continue

        mismatched_fields = []
        for field in SITE_LEVEL_FIELDS:
            values = {row.get(field, "") for _, row in site_rows}
            if len(values) > 1:
                mismatched_fields.append(field)

        if mismatched_fields:
            lines = ", ".join(str(line_no) for line_no, _ in site_rows)
            errors.append(
                f"{site_id} has conflicting site-level fields ({', '.join(mismatched_fields)}) on lines {lines}"
            )

    return errors


def main() -> int:
    path = pathlib.Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_CSV
    errors = validate(path)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        row_count = sum(1 for _ in csv.DictReader(handle, delimiter=";"))
    print(f"OK: {path} parsed cleanly with {row_count} data rows and the expected 21-column schema")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
