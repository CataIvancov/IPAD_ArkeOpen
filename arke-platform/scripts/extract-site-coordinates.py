#!/usr/bin/env python3
"""
Extract and enhance coordinates for high-confidence sites.

Uses regex patterns to extract GPS coordinates from OCR text and marks
them as "precise" (extracted from text) or "approximate" (region fallback).

Usage:
    python3 scripts/extract-site-coordinates.py
"""

import csv
import json
import re
from pathlib import Path
from typing import Dict, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = ROOT / "data" / "site-geolocation-candidates-highconf.csv"
TEXT_ROOT = ROOT / "data" / "google-drive-text"
OUTPUT_FILE = ROOT / "data" / "site-coordinates-extracted.csv"

# Regional center coordinates (from build-drive-sites-arkeogis-csv.py)
REGION_CENTER_COORDS = {
    "Indonesia | Sulawesi": (120.0, -2.0),
    "Indonesia | South Sulawesi": (119.5, -5.0),
    "Indonesia | Central Sulawesi": (121.5, -1.5),
    "Indonesia | Southeast Sulawesi": (122.5, -4.0),
    "Indonesia | North Sulawesi": (124.0, 1.0),
    "Indonesia | West Sulawesi": (119.3, -3.0),
    "Indonesia | Java": (110.5, -7.5),
    "Indonesia | West Java": (107.0, -6.5),
    "Indonesia | Central Java": (110.0, -7.3),
    "Indonesia | East Java": (113.0, -7.8),
    "Indonesia | Sumatra": (102.0, 0.0),
    "Indonesia | Kalimantan": (113.5, 0.5),
    "Indonesia | East Kalimantan": (117.5, 1.0),
    "Indonesia | South Kalimantan": (115.5, -3.0),
    "Indonesia | West Kalimantan": (110.0, 0.0),
    "Indonesia | Central Kalimantan": (113.5, -1.5),
    "Indonesia | Flores": (121.0, -8.5),
    "Indonesia | Nusa Tenggara": (121.5, -8.8),
    "Indonesia | Maluku Islands": (128.5, -3.0),
    "Indonesia | Papua": (140.0, -3.5),
    "Indonesia | Bali": (115.2, -8.3),
    "Timor-Leste": (125.5, -8.8),
}

# Coordinate extraction patterns (from build-drive-sites-arkeogis-csv.py)
COORD_DECIMAL_PATTERN = re.compile(
    r"(?:Coordinates:?\s*)?([0-9]{1,2}\.[0-9]{1,6})\s*([NS])\s*[,;\s]+([0-9]{1,3}\.[0-9]{1,6})\s*([EW])",
    re.IGNORECASE,
)

COORD_S_E_PATTERN = re.compile(
    r"[Ss]\s*([0-9]{1,2}\.[0-9]+)\s*[,;]\s*[Ee]\s*([0-9]{1,3}\.[0-9]+)",
    re.IGNORECASE,
)

COORD_LAT_LON_PATTERN = re.compile(
    r"[Ll]at(?:itude)?[:\s]*([\-0-9.]+)\s*([NS])\s*[,;\s]+[Ll]on(?:gitude)?[:\s]*([\-0-9.]+)\s*([EW])",
    re.IGNORECASE,
)

COORD_DMS = re.compile(
    r"([0-9]{1,2})[°º]?\s*([0-9]{1,2})[\′']?\s*([0-9.,]+)[\"″]?\s*([NS])\s*[;,]?\s*([0-9]{1,3})[°º]?\s*([0-9]{1,2})[\′']?\s*([0-9.,]+)[\"″]?\s*([EW])",
    re.IGNORECASE,
)


def dms_to_decimal(deg: int, min: int, sec: float, direction: str) -> Optional[float]:
    """Convert degrees/minutes/seconds to decimal."""
    try:
        decimal = deg + min / 60.0 + sec / 3600.0
        if direction in "SW":
            decimal = -decimal
        return round(decimal, 6)
    except:
        return None


def extract_coords_from_text(text: str) -> list:
    """Extract all coordinate pairs found in text.

    Returns list of (lat, lon, format) tuples.
    """
    coords = []

    # Try decimal pattern (most common)
    for match in COORD_DECIMAL_PATTERN.finditer(text):
        try:
            lat = float(match.group(1))
            lat_dir = match.group(2)
            lon = float(match.group(3))
            lon_dir = match.group(4)

            if lat_dir == "S":
                lat = -lat
            if lon_dir == "W":
                lon = -lon

            coords.append((round(lat, 6), round(lon, 6), "decimal"))
        except:
            pass

    # Try S/E pattern
    for match in COORD_S_E_PATTERN.finditer(text):
        try:
            lat = -float(match.group(1))  # Assuming S latitude
            lon = float(match.group(2))  # Assuming E longitude
            coords.append((round(lat, 6), round(lon, 6), "se"))
        except:
            pass

    # Try Lat/Lon pattern
    for match in COORD_LAT_LON_PATTERN.finditer(text):
        try:
            lat = float(match.group(1))
            lat_dir = match.group(2)
            lon = float(match.group(3))
            lon_dir = match.group(4)

            if lat_dir == "S":
                lat = -lat
            if lon_dir == "W":
                lon = -lon

            coords.append((round(lat, 6), round(lon, 6), "latlon"))
        except:
            pass

    # Try DMS pattern
    for match in COORD_DMS.finditer(text):
        try:
            deg_lat = int(match.group(1))
            min_lat = int(match.group(2))
            sec_lat = float(match.group(3).replace(",", "."))
            dir_lat = match.group(4)

            deg_lon = int(match.group(5))
            min_lon = int(match.group(6))
            sec_lon = float(match.group(7).replace(",", "."))
            dir_lon = match.group(8)

            lat = dms_to_decimal(deg_lat, min_lat, sec_lat, dir_lat)
            lon = dms_to_decimal(deg_lon, min_lon, sec_lon, dir_lon)

            if lat and lon:
                coords.append((lat, lon, "dms"))
        except:
            pass

    # Deduplicate and return
    return list(dict.fromkeys(coords))


def infer_region(site_name: str) -> Optional[str]:
    """Try to infer region from site name."""
    site_lower = site_name.lower()

    # Check for specific regions in site name
    for region, center in REGION_CENTER_COORDS.items():
        region_keywords = region.lower().split("|")[-1].strip()
        if region_keywords in site_lower:
            return region

    return None


def process_sites() -> None:
    """Extract coordinates for all high-confidence sites."""
    print("Loading high-confidence sites...")
    with open(INPUT_FILE, encoding="utf-8") as f:
        sites = list(csv.DictReader(f))

    print(f"  Loaded {len(sites)} sites")

    # Prepare output rows
    output_rows = []
    stats = {
        "has_precise": 0,
        "has_approximate": 0,
        "no_coords": 0,
    }

    print("\nExtracting coordinates...")
    for i, site in enumerate(sites):
        site_name = site.get("site_name", "").strip()
        pdf_path = site.get("source_file", "")

        # Try to load OCR text for this PDF
        pdf_filename = Path(pdf_path).stem
        json_path = TEXT_ROOT / f"{pdf_filename}.json"

        coords_found = []
        precision = "none"

        if json_path.exists():
            try:
                ocr_data = json.loads(json_path.read_text(encoding="utf-8"))
                ocr_text = " ".join(ocr_data.get("page_texts", []))

                coords_found = extract_coords_from_text(ocr_text)
            except:
                pass

        if coords_found:
            # Use first (best) coordinate found
            lat, lon, fmt = coords_found[0]
            precision = "precise"
            stats["has_precise"] += 1
        else:
            # Try region center fallback
            region = infer_region(site_name)
            if region and region in REGION_CENTER_COORDS:
                lon, lat = REGION_CENTER_COORDS[region]
                precision = "approximate"
                stats["has_approximate"] += 1
            else:
                lat, lon = None, None
                stats["no_coords"] += 1

        # Add to output
        out_row = {**site}
        out_row["latitude"] = lat if lat else ""
        out_row["longitude"] = lon if lon else ""
        out_row["coord_precision"] = precision
        output_rows.append(out_row)

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(sites)}")

    # Write output
    print(f"\nWriting {len(output_rows)} sites with coordinates...")
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        fieldnames = list(output_rows[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Print statistics
    print(f"\n{'='*60}")
    print(f"Coordinate Extraction Results:")
    print(f"{'='*60}")
    print(f"  Total sites:           {len(output_rows):6d}")
    print(f"  With precise coords:   {stats['has_precise']:6d} ({100*stats['has_precise']/len(output_rows):.1f}%)")
    print(f"  With approximate:      {stats['has_approximate']:6d} ({100*stats['has_approximate']/len(output_rows):.1f}%)")
    print(f"  Without coordinates:   {stats['no_coords']:6d} ({100*stats['no_coords']/len(output_rows):.1f}%)")
    print(f"{'='*60}")

    # Show sample
    print("\nSample sites with coordinates:")
    for row in output_rows[:5]:
        if row["latitude"] and row["longitude"]:
            print(f"  ✓ {row['site_name']:30s} @ ({row['latitude']:8s}, {row['longitude']:9s}) [{row['coord_precision']}]")


if __name__ == "__main__":
    process_sites()
