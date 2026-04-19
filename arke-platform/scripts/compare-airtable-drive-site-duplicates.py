#!/usr/bin/env python3

from __future__ import annotations

import csv
import difflib
import importlib.util
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
Airtable_CSV = ROOT / "data" / "airtable-to-arkeogis-v4-site-only.csv"
DRIVE_CSV = ROOT / "data" / "drive-sites-to-arkeogis.csv"
OUTPUT_CSV = ROOT / "data" / "airtable-drive-site-duplicates.csv"
OUTPUT_REPORT = ROOT / "data" / "airtable-drive-site-duplicates-report.txt"
MATCH_THRESHOLD = 0.92

OUTPUT_HEADERS = [
    "AIRTABLE_SITE_NAME",
    "AIRTABLE_SITE_SOURCE_ID",
    "DRIVE_SITE_NAME",
    "DRIVE_SITE_SOURCE_ID",
    "MATCH_TYPE",
    "MATCH_SCORE",
    "AIRTABLE_LOCALISATION",
    "DRIVE_LOCALISATION",
    "AIRTABLE_LONGITUDE",
    "AIRTABLE_LATITUDE",
    "DRIVE_LONGITUDE",
    "DRIVE_LATITUDE",
]


def load_drive_builder_helpers():
    script_path = ROOT / "scripts" / "build-drive-sites-arkeogis-csv.py"
    spec = importlib.util.spec_from_file_location("build_drive_sites_arkeogis_csv", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load duplicate helpers from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.clean, module.norm_site_key


CLEAN, NORM_SITE_KEY = load_drive_builder_helpers()


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(None, left, right).ratio()


def exact_matches(
    airtable_rows: list[dict[str, str]],
    drive_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], set[tuple[str, str]], set[str], set[str]]:
    airtable_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    drive_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)

    for row in airtable_rows:
        site_name = CLEAN(row.get("SITE_NAME", ""))
        if site_name:
            airtable_by_key[NORM_SITE_KEY(site_name)].append(row)

    for row in drive_rows:
        site_name = CLEAN(row.get("SITE_NAME", ""))
        if site_name:
            drive_by_key[NORM_SITE_KEY(site_name)].append(row)

    rows = []
    matched_pairs: set[tuple[str, str]] = set()
    matched_airtable_ids: set[str] = set()
    matched_drive_ids: set[str] = set()

    for key in sorted(set(airtable_by_key) & set(drive_by_key)):
        for airtable_row in airtable_by_key[key]:
            for drive_row in drive_by_key[key]:
                rows.append(build_output_row(airtable_row, drive_row, "exact_norm", 1.0))
                matched_pairs.add((airtable_row["SITE_SOURCE_ID"], drive_row["SITE_SOURCE_ID"]))
                matched_airtable_ids.add(airtable_row["SITE_SOURCE_ID"])
                matched_drive_ids.add(drive_row["SITE_SOURCE_ID"])

    return rows, matched_pairs, matched_airtable_ids, matched_drive_ids


def fuzzy_matches(
    airtable_rows: list[dict[str, str]],
    drive_rows: list[dict[str, str]],
    matched_pairs: set[tuple[str, str]],
    matched_airtable_ids: set[str],
    matched_drive_ids: set[str],
) -> list[dict[str, str]]:
    available_airtable = [row for row in airtable_rows if row["SITE_SOURCE_ID"] not in matched_airtable_ids]
    available_drive = [row for row in drive_rows if row["SITE_SOURCE_ID"] not in matched_drive_ids]

    rows = []
    for drive_row in available_drive:
        drive_name = CLEAN(drive_row.get("SITE_NAME", ""))
        drive_key = NORM_SITE_KEY(drive_name)
        if not drive_key:
            continue

        best_score = 0.0
        best_airtable_rows: list[dict[str, str]] = []
        for airtable_row in available_airtable:
            pair_key = (airtable_row["SITE_SOURCE_ID"], drive_row["SITE_SOURCE_ID"])
            if pair_key in matched_pairs:
                continue
            airtable_name = CLEAN(airtable_row.get("SITE_NAME", ""))
            airtable_key = NORM_SITE_KEY(airtable_name)
            if not airtable_key or airtable_key == drive_key:
                continue

            score = similarity(drive_key, airtable_key)
            if score < MATCH_THRESHOLD:
                continue
            if score > best_score:
                best_score = score
                best_airtable_rows = [airtable_row]
            elif score == best_score:
                best_airtable_rows.append(airtable_row)

        for airtable_row in sorted(best_airtable_rows, key=lambda row: (CLEAN(row.get("SITE_NAME", "")).lower(), row["SITE_SOURCE_ID"])):
            rows.append(build_output_row(airtable_row, drive_row, "fuzzy_name", best_score))

    return rows


def build_output_row(airtable_row: dict[str, str], drive_row: dict[str, str], match_type: str, score: float) -> dict[str, str]:
    return {
        "AIRTABLE_SITE_NAME": CLEAN(airtable_row.get("SITE_NAME", "")),
        "AIRTABLE_SITE_SOURCE_ID": CLEAN(airtable_row.get("SITE_SOURCE_ID", "")),
        "DRIVE_SITE_NAME": CLEAN(drive_row.get("SITE_NAME", "")),
        "DRIVE_SITE_SOURCE_ID": CLEAN(drive_row.get("SITE_SOURCE_ID", "")),
        "MATCH_TYPE": match_type,
        "MATCH_SCORE": f"{score:.2f}",
        "AIRTABLE_LOCALISATION": CLEAN(airtable_row.get("LOCALISATION", "")),
        "DRIVE_LOCALISATION": CLEAN(drive_row.get("LOCALISATION", "")),
        "AIRTABLE_LONGITUDE": CLEAN(airtable_row.get("LONGITUDE", "")),
        "AIRTABLE_LATITUDE": CLEAN(airtable_row.get("LATITUDE", "")),
        "DRIVE_LONGITUDE": CLEAN(drive_row.get("LONGITUDE", "")),
        "DRIVE_LATITUDE": CLEAN(drive_row.get("LATITUDE", "")),
    }


def sort_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    match_order = {"exact_norm": 0, "fuzzy_name": 1}
    return sorted(
        rows,
        key=lambda row: (
            match_order.get(row["MATCH_TYPE"], 99),
            -float(row["MATCH_SCORE"]),
            row["DRIVE_SITE_NAME"].lower(),
            row["AIRTABLE_SITE_NAME"].lower(),
        ),
    )


def write_csv(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def build_report(airtable_count: int, drive_count: int, rows: list[dict[str, str]]) -> str:
    exact_count = sum(1 for row in rows if row["MATCH_TYPE"] == "exact_norm")
    fuzzy_count = sum(1 for row in rows if row["MATCH_TYPE"] == "fuzzy_name")
    lines = [
        "Airtable vs Google Drive site duplicate report",
        "",
        f"Airtable rows: {airtable_count}",
        f"Drive rows: {drive_count}",
        f"Exact duplicates: {exact_count}",
        f"Fuzzy duplicates: {fuzzy_count}",
        f"Output CSV: {OUTPUT_CSV}",
        "",
        "Top matches:",
    ]
    for row in rows[:20]:
        lines.append(
            f"- {row['MATCH_TYPE']} | {row['MATCH_SCORE']} | {row['DRIVE_SITE_NAME']} <> {row['AIRTABLE_SITE_NAME']}"
        )
    return "\n".join(lines) + "\n"


def write_report(report: str) -> None:
    OUTPUT_REPORT.write_text(report, encoding="utf-8")


def main() -> int:
    airtable_rows = load_rows(Airtable_CSV)
    drive_rows = load_rows(DRIVE_CSV)
    exact_rows, matched_pairs, matched_airtable_ids, matched_drive_ids = exact_matches(airtable_rows, drive_rows)
    fuzzy_rows = fuzzy_matches(airtable_rows, drive_rows, matched_pairs, matched_airtable_ids, matched_drive_ids)
    rows = sort_rows(exact_rows + fuzzy_rows)
    write_csv(rows)
    report = build_report(len(airtable_rows), len(drive_rows), rows)
    write_report(report)
    print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
