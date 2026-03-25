#!/usr/bin/env python3
"""
Merge 776 new clean sites into drive-sites-to-arkeogis.csv

Converts new sites to ArkeOGIS format and merges with existing dataset.
Handles different delimiters (comma for input, semicolon for ArkeOGIS output).

Usage:
    python3 scripts/merge-sites-to-arkeogis.py
"""

import csv
import hashlib
from pathlib import Path
from datetime import datetime


ROOT = Path(__file__).resolve().parents[1]
EXISTING_FILE = ROOT / "data" / "drive-sites-to-arkeogis.csv"
MERGE_READY_FILE = ROOT / "data" / "site-geolocation-merge-ready.csv"
OUTPUT_FILE = ROOT / "data" / "drive-sites-to-arkeogis-merged.csv"
BACKUP_FILE = ROOT / "data" / "drive-sites-to-arkeogis-backup.csv"

# ArkeOGIS output column headers
ARKEOGIS_HEADERS = [
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
    "WEB_IMAGES",
]


def load_existing_sites() -> list:
    """Load existing ArkeOGIS sites."""
    sites = []
    with open(EXISTING_FILE, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            sites.append(row)
    return sites


def generate_site_id(site_name: str) -> str:
    """Generate unique site source ID."""
    # Format: drive-[hash]
    hash_obj = hashlib.sha256(site_name.encode('utf-8'))
    hash_digest = hash_obj.hexdigest()[:12]
    return f"drive-{hash_digest}"


def convert_to_arkeogis_format(new_sites: list) -> list:
    """Convert new sites to ArkeOGIS format."""
    converted = []

    for site in new_sites:
        site_name = site.get('site_name', '').strip()

        row = {
            "SITE_SOURCE_ID": generate_site_id(site_name),
            "SITE_NAME": site_name,
            "LOCALISATION": "Indonesia",  # All sites are from Indonesia survey
            "GEONAME_ID": "",
            "PROJECTION_SYSTEM": site.get('coordinate_system', 'WGS84') or 'WGS84',
            "LONGITUDE": site.get('longitude', ''),
            "LATITUDE": site.get('latitude', ''),
            "ALTITUDE": "",
            "CITY_CENTROID": "",
            "STATE_OF_KNOWLEDGE": "Not documented",
            "OCCUPATION": "Not documented",
            "STARTING_PERIOD": "Not specified",
            "ENDING_PERIOD": "",
            "MAIN_CHARAC": "Archaeological site",
            "CHARAC_LVL1": "Archaeological site",
            "CHARAC_LVL2": "",
            "CHARAC_LVL3": "",
            "CHARAC_LVL4": "",
            "CHARAC_EXP": "",
            "BIBLIOGRAPHY": "",  # Could be enriched from bibliography extraction
            "COMMENTS": f"Source: {Path(site.get('source_file', '')).name} page {site.get('page', '')}",
            "WEB_IMAGES": "",
        }

        converted.append(row)

    return converted


def merge_sites() -> None:
    """Merge new sites into existing dataset."""
    print("Loading existing sites...")
    existing = load_existing_sites()
    print(f"  Found {len(existing)} existing sites")

    print("\nLoading new merge-ready sites...")
    with open(MERGE_READY_FILE, encoding='utf-8') as f:
        new_sites = list(csv.DictReader(f))
    print(f"  Found {len(new_sites)} new sites")

    # Convert to ArkeOGIS format
    print("\nConverting new sites to ArkeOGIS format...")
    converted = convert_to_arkeogis_format(new_sites)
    print(f"  Converted {len(converted)} sites")

    # Check for duplicates in merged data
    existing_names = {s.get('SITE_NAME', '').lower() for s in existing}
    new_names = {s.get('SITE_NAME', '').lower() for s in converted}
    overlap = existing_names & new_names

    if overlap:
        print(f"\n⚠️  WARNING: {len(overlap)} sites appear in both datasets")
        print(f"  These will not be added (keeping existing versions)")
        converted = [s for s in converted if s['SITE_NAME'].lower() not in overlap]
        print(f"  Proceeding with {len(converted)} truly new sites")

    # Merge
    merged = existing + converted

    print(f"\nMerge Results:")
    print(f"  Existing:      {len(existing):6d}")
    print(f"  New (added):   {len(converted):6d}")
    print(f"  TOTAL:         {len(merged):6d}")

    # Backup existing file
    print(f"\nBacking up existing file...")
    EXISTING_FILE.rename(BACKUP_FILE)
    print(f"  ✓ Saved to {BACKUP_FILE.name}")

    # Write merged file
    print(f"\nWriting merged dataset...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ARKEOGIS_HEADERS, delimiter=';')
        writer.writeheader()
        writer.writerows(merged)
    print(f"  ✓ Saved to {OUTPUT_FILE.name}")

    # Copy to original location
    OUTPUT_FILE.rename(EXISTING_FILE)
    print(f"  ✓ Renamed to {EXISTING_FILE.name}")

    # Verification
    print(f"\n{'='*60}")
    print(f"Merge Complete!")
    print(f"{'='*60}")
    print(f"  Previous size:  {len(existing):6d} sites")
    print(f"  New sites:      {len(converted):6d} sites")
    print(f"  New total:      {len(merged):6d} sites ({100*len(converted)/len(merged):.1f}% growth)")
    print(f"\n  Backup: {BACKUP_FILE.name}")
    print(f"  Updated: {EXISTING_FILE.name}")
    print(f"{'='*60}")

    # Show sample of new merged sites
    print(f"\nSample of newly added sites:")
    for site in converted[:10]:
        print(f"  ✓ {site['SITE_NAME']:30s} @ ({site['LATITUDE']:>8s}, {site['LONGITUDE']:>9s})")
    if len(converted) > 10:
        print(f"    ... and {len(converted) - 10} more")


if __name__ == "__main__":
    merge_sites()
