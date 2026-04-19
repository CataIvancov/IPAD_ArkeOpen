#!/usr/bin/env python3

from __future__ import annotations

import csv
import difflib
import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DRIVE_CSV = ROOT / "data" / "drive-sites-to-arkeogis.csv"
OUTPUT_CSV = ROOT / "data" / "drive-sites-to-arkeogis-duplicate-report.csv"
OUTPUT_REPORT = ROOT / "data" / "drive-sites-to-arkeogis-duplicate-report.txt"
MATCH_THRESHOLD = 0.92

OUTPUT_HEADERS = [
    "SITE_NAME_LEFT",
    "SITE_SOURCE_ID_LEFT",
    "SITE_NAME_RIGHT",
    "SITE_SOURCE_ID_RIGHT",
    "MATCH_TYPE",
    "MATCH_SCORE",
    "LOCALISATION_LEFT",
    "LOCALISATION_RIGHT",
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


def load_rows() -> list[dict[str, str]]:
    with DRIVE_CSV.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(None, left, right).ratio()


def build_output_row(left: dict[str, str], right: dict[str, str], match_type: str, score: float) -> dict[str, str]:
    return {
        "SITE_NAME_LEFT": CLEAN(left.get("SITE_NAME", "")),
        "SITE_SOURCE_ID_LEFT": CLEAN(left.get("SITE_SOURCE_ID", "")),
        "SITE_NAME_RIGHT": CLEAN(right.get("SITE_NAME", "")),
        "SITE_SOURCE_ID_RIGHT": CLEAN(right.get("SITE_SOURCE_ID", "")),
        "MATCH_TYPE": match_type,
        "MATCH_SCORE": f"{score:.2f}",
        "LOCALISATION_LEFT": CLEAN(left.get("LOCALISATION", "")),
        "LOCALISATION_RIGHT": CLEAN(right.get("LOCALISATION", "")),
    }


def find_duplicate_pairs(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    duplicate_rows = []
    usable_rows = [row for row in rows if CLEAN(row.get("SITE_NAME", ""))]

    for index, left in enumerate(usable_rows):
        left_name = CLEAN(left["SITE_NAME"])
        left_key = NORM_SITE_KEY(left_name)
        if not left_key:
            continue

        for right in usable_rows[index + 1 :]:
            right_name = CLEAN(right["SITE_NAME"])
            right_key = NORM_SITE_KEY(right_name)
            if not right_key:
                continue

            if left["SITE_SOURCE_ID"] == right["SITE_SOURCE_ID"]:
                continue

            if left_key == right_key:
                duplicate_rows.append(build_output_row(left, right, "exact_norm", 1.0))
                continue

            score = similarity(left_key, right_key)
            if score >= MATCH_THRESHOLD:
                duplicate_rows.append(build_output_row(left, right, "fuzzy_name", score))

    return sort_rows(duplicate_rows)


def sort_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    match_order = {"exact_norm": 0, "fuzzy_name": 1}
    return sorted(
        rows,
        key=lambda row: (
            match_order.get(row["MATCH_TYPE"], 99),
            -float(row["MATCH_SCORE"]),
            row["SITE_NAME_LEFT"].lower(),
            row["SITE_NAME_RIGHT"].lower(),
        ),
    )


def write_csv(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def build_report(total_rows: int, duplicate_rows: list[dict[str, str]]) -> str:
    exact_count = sum(1 for row in duplicate_rows if row["MATCH_TYPE"] == "exact_norm")
    fuzzy_count = sum(1 for row in duplicate_rows if row["MATCH_TYPE"] == "fuzzy_name")
    lines = [
        "Drive sites internal duplicate report",
        "",
        f"Drive rows: {total_rows}",
        f"Exact duplicate pairs: {exact_count}",
        f"Fuzzy duplicate pairs: {fuzzy_count}",
        f"Output CSV: {OUTPUT_CSV}",
        "",
        "Top matches:",
    ]
    for row in duplicate_rows[:20]:
        lines.append(
            f"- {row['MATCH_TYPE']} | {row['MATCH_SCORE']} | {row['SITE_NAME_LEFT']} <> {row['SITE_NAME_RIGHT']}"
        )
    return "\n".join(lines) + "\n"


def write_report(report: str) -> None:
    OUTPUT_REPORT.write_text(report, encoding="utf-8")


def main() -> int:
    rows = load_rows()
    duplicate_rows = find_duplicate_pairs(rows)
    write_csv(duplicate_rows)
    report = build_report(len(rows), duplicate_rows)
    write_report(report)
    print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
