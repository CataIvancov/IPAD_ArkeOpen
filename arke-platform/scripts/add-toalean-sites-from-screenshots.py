#!/usr/bin/env python3

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DRIVE_CSV = ROOT / "data" / "drive-sites-to-arkeogis.csv"
REPORT_TXT = ROOT / "data" / "toalean-screenshot-sites-import-report.txt"

ROCKSHELTER_SOURCE = "User-provided screenshot: Table I. Summary of Contents from Toalean Rockshelters."
OPEN_SITE_SOURCE = "User-provided screenshot: Table 5. Summary of Observations from Open Toalean Sites."

ROCKSHELTER_ENTRIES = [
    ("Leang Ulebaba", "South Sulawesi | Walanae headwaters", "Rockshelter / Rock shelter / Ceruk"),
    ("Tomatoa Kacicang", "South Sulawesi | Walanae headwaters", "Rockshelter / Rock shelter / Ceruk"),
    ("Batu Ejaya 1", "South Sulawesi | South coast", "Rockshelter / Rock shelter / Ceruk"),
    ("Batu Ejaya 2", "South Sulawesi | South coast", "Rockshelter / Rock shelter / Ceruk"),
    ("Panisi Takbutu", "South Sulawesi | Bone-Soppeng", "Rockshelter / Rock shelter / Ceruk"),
    ("Bola Batu", "South Sulawesi | Bone-Soppeng", "Rockshelter / Rock shelter / Ceruk"),
    ("Leang Jarie surface", "South Sulawesi | Patanuang Asue, Maros", "Open-air site"),
    ("Leang Pette Kere", "South Sulawesi | Leang-Leang, Maros", "Rockshelter / Rock shelter / Ceruk"),
    ("Leang Paja surface", "South Sulawesi | Leang-Leang, Maros", "Open-air site"),
    ("Leang Burung 1", "South Sulawesi | Leang-Leang, Maros", "Rockshelter / Rock shelter / Ceruk"),
    ("Ulu Leang 1", "South Sulawesi | Leang-Leang, Maros", "Rockshelter / Rock shelter / Ceruk"),
    ("Gua Bulusumi", "South Sulawesi | Pangkajene", "Rockshelter / Rock shelter / Ceruk"),
    ("Gua Macinai", "South Sulawesi | Pangkajene", "Rockshelter / Rock shelter / Ceruk"),
    ("Leang Garunggung", "South Sulawesi | Pangkajene", "Rockshelter / Rock shelter / Ceruk"),
    ("Belae karsts (surface)", "South Sulawesi | Pangkajene", "Open-air site"),
]

OPEN_SITE_ENTRIES = [
    ("Mandai", "South Sulawesi | Central-West Coast"),
    ("Belae", "South Sulawesi | Central-West Coast"),
    ("Padang Lampe", "South Sulawesi | Central-West Coast"),
    ("Campagaloe", "South Sulawesi | South Coast"),
    ("Ujung", "South Sulawesi | Bone and Soppeng"),
    ("Cabenge", "South Sulawesi | Bone and Soppeng"),
    ("Watanlamuru", "South Sulawesi | Bone and Soppeng"),
    ("Malindrung", "South Sulawesi | Bone and Soppeng"),
    ("Batang Mata Sapo", "South Sulawesi | Selayar Island"),
    ("Bonto Sunggu Asli", "South Sulawesi | Macassar Survey Area"),
    ("Gentung", "South Sulawesi | Macassar Survey Area"),
    ("Pakka Mukang", "South Sulawesi | Macassar Survey Area"),
    ("Balang Sari", "South Sulawesi | Macassar Survey Area"),
    ("Bonto Ramba Tua", "South Sulawesi | Macassar Survey Area"),
    ("Saukang Boe", "South Sulawesi | Macassar Survey Area"),
    ("Pammangkulang Batua", "South Sulawesi | Macassar Survey Area"),
    ("Bukit Bikulung", "South Sulawesi | Macassar Survey Area"),
    ("Salekowa Tua", "South Sulawesi | Macassar Survey Area"),
    ("Moncong Moncong", "South Sulawesi | Macassar Survey Area"),
]


def load_drive_builder_helpers():
    script_path = ROOT / "scripts" / "build-drive-sites-arkeogis-csv.py"
    spec = importlib.util.spec_from_file_location("build_drive_sites_arkeogis_csv", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load helpers from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.clean, module.norm_site_key, module.stable_id


CLEAN, NORM_SITE_KEY, STABLE_ID = load_drive_builder_helpers()


def load_drive_rows() -> list[dict[str, str]]:
    with DRIVE_CSV.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def make_row(site_name: str, localisation: str, charac_lvl1: str, comment: str) -> dict[str, str]:
    return {
        "SITE_SOURCE_ID": STABLE_ID(site_name),
        "SITE_NAME": site_name,
        "LOCALISATION": localisation,
        "GEONAME_ID": "",
        "PROJECTION_SYSTEM": "",
        "LONGITUDE": "",
        "LATITUDE": "",
        "ALTITUDE": "",
        "CITY_CENTROID": "No",
        "STATE_OF_KNOWLEDGE": "Literature",
        "OCCUPATION": "Not specified",
        "STARTING_PERIOD": "",
        "ENDING_PERIOD": "",
        "MAIN_CHARAC": "Archaeological Sites",
        "CHARAC_LVL1": charac_lvl1,
        "CHARAC_LVL2": "",
        "CHARAC_LVL3": "",
        "CHARAC_LVL4": "",
        "CHARAC_EXP": "No",
        "BIBLIOGRAPHY": "",
        "COMMENTS": comment,
        "WEB_IMAGES": "",
        "DUPLICATE_DB_NAME": "",
        "DUPLICATE_SCORE": "",
        "DUPLICATE_FLAG": "",
    }


def build_missing_rows(existing_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    existing_keys = {NORM_SITE_KEY(CLEAN(row.get("SITE_NAME", ""))) for row in existing_rows if CLEAN(row.get("SITE_NAME", ""))}
    new_rows = []

    for site_name, localisation, charac_lvl1 in ROCKSHELTER_ENTRIES:
        if NORM_SITE_KEY(site_name) in existing_keys:
            continue
        new_rows.append(make_row(site_name, localisation, charac_lvl1, ROCKSHELTER_SOURCE))
        existing_keys.add(NORM_SITE_KEY(site_name))

    for site_name, localisation in OPEN_SITE_ENTRIES:
        if NORM_SITE_KEY(site_name) in existing_keys:
            continue
        new_rows.append(make_row(site_name, localisation, "Open-air site", OPEN_SITE_SOURCE))
        existing_keys.add(NORM_SITE_KEY(site_name))

    return new_rows


def write_drive_rows(rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with DRIVE_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def write_report(original_count: int, added_rows: list[dict[str, str]], final_count: int) -> None:
    lines = [
        "Toalean screenshot site import report",
        "",
        f"Original rows: {original_count}",
        f"Rows added: {len(added_rows)}",
        f"Final rows: {final_count}",
        f"Output: {DRIVE_CSV}",
        "",
        "Added sites:",
    ]
    for row in added_rows:
        lines.append(f"- {row['SITE_NAME']} | {row['LOCALISATION']} | {row['CHARAC_LVL1']}")
    REPORT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    drive_rows = load_drive_rows()
    if not drive_rows:
        raise RuntimeError(f"No rows found in {DRIVE_CSV}")

    fieldnames = list(drive_rows[0].keys())
    new_rows = build_missing_rows(drive_rows)
    all_rows = drive_rows + new_rows
    write_drive_rows(all_rows, fieldnames)
    write_report(len(drive_rows), new_rows, len(all_rows))
    print(f"Added {len(new_rows)} rows to {DRIVE_CSV}")
    print(f"Report: {REPORT_TXT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
