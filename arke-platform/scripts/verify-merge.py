#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib

BASE_DIR = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform")
DRIVE_CSV = BASE_DIR / "data" / "drive-sites-to-arkeogis-fixed.csv"
AIRTABLE_CSV = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only.csv"
COMBINED_CSV = BASE_DIR / "data" / "ipad-sites-combined.csv"


def get_site_names(filepath: pathlib.Path) -> set[str]:
    with filepath.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        return {row["SITE_NAME"] for row in reader}


def main() -> None:
    drive_sites = get_site_names(DRIVE_CSV)
    airtable_sites = get_site_names(AIRTABLE_CSV)
    combined_sites = get_site_names(COMBINED_CSV)

    print(f"Drive sites: {len(drive_sites)} unique SITE_NAMEs")
    print(f"Airtable sites: {len(airtable_sites)} unique SITE_NAMEs")
    print(f"Combined sites: {len(combined_sites)} unique SITE_NAMEs")
    print(f"Expected combined: {len(drive_sites) + len(airtable_sites)}")

    # Check for duplicates between drive and airtable
    overlap = drive_sites & airtable_sites
    if overlap:
        print(f"\nDuplicate SITE_NAMEs between drive and airtable: {len(overlap)}")
        for name in sorted(overlap):
            print(f"  - {name}")

    # Check if all sites are in combined
    missing_from_combined = (drive_sites | airtable_sites) - combined_sites
    if missing_from_combined:
        print(f"\nERROR: {len(missing_from_combined)} sites missing from combined CSV:")
        for name in sorted(missing_from_combined):
            print(f"  - {name}")
    else:
        print("\n✓ All sites from both original CSVs are in the combined CSV")

    # Check if combined has extra sites
    extra_in_combined = combined_sites - (drive_sites | airtable_sites)
    if extra_in_combined:
        print(f"\nWARNING: {len(extra_in_combined)} extra sites in combined CSV:")
        for name in sorted(extra_in_combined):
            print(f"  - {name}")


if __name__ == "__main__":
    main()
