#!/usr/bin/env python3
"""
Reorganize ipad-sites-combined.csv to group sites by geographic proximity clusters.
Clusters (sites within 10km) will appear first, sorted by cluster size.
Isolated sites will follow at the end.
"""

import csv
import math
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass

ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "data" / "ipad-sites-combined.csv"
OUTPUT_CSV = ROOT / "data" / "ipad-sites-organized.csv"  # New file for verification

# Distance threshold in kilometers
PROXIMITY_THRESHOLD_KM = 10.0


@dataclass
class SiteRow:
    site_id: str
    name: str
    location: str
    longitude: float
    latitude: float
    raw_data: dict  # All CSV columns


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in kilometers."""
    R = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def load_sites_with_data() -> tuple[list[SiteRow], list[dict], list[str]]:
    """Load sites with their full CSV data."""
    sites = []
    all_rows = []
    headers = []

    with INPUT_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        headers = reader.fieldnames or []
        for row in reader:
            all_rows.append(row)
            try:
                lon = float(row["LONGITUDE"]) if row["LONGITUDE"] else None
                lat = float(row["LATITUDE"]) if row["LATITUDE"] else None
                if lon is not None and lat is not None:
                    sites.append(SiteRow(
                        site_id=row["SITE_SOURCE_ID"],
                        name=row["SITE_NAME"],
                        location=row["LOCALISATION"],
                        longitude=lon,
                        latitude=lat,
                        raw_data=row
                    ))
            except (ValueError, KeyError):
                pass

    return sites, all_rows, headers


def find_clusters(sites: list[SiteRow]) -> dict[str, list[SiteRow]]:
    """Group sites by geographic proximity using Union-Find."""
    if not sites:
        return {}

    parent = {site.site_id: site.site_id for site in sites}

    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Build proximity connections
    site_list = list(sites)
    for i in range(len(site_list)):
        for j in range(i + 1, len(site_list)):
            site1, site2 = site_list[i], site_list[j]
            dist = haversine_distance(
                site1.latitude, site1.longitude,
                site2.latitude, site2.longitude
            )
            if dist <= PROXIMITY_THRESHOLD_KM:
                union(site1.site_id, site2.site_id)

    # Group by root parent
    groups = defaultdict(list)
    for site in sites:
        root = find(site.site_id)
        groups[root].append(site)

    return dict(groups)


def organize_csv():
    print("Loading sites from CSV...")
    sites_with_coords, all_rows, headers = load_sites_with_data()
    print(f"Loaded {len(sites_with_coords)} sites with coordinates out of {len(all_rows)} total rows")

    # Build a set of site IDs that have coordinates
    coord_site_ids = {s.site_id for s in sites_with_coords}

    # Separate rows with and without coordinates
    rows_with_coords = []
    rows_without_coords = []

    for row in all_rows:
        site_id = row.get("SITE_SOURCE_ID", "")
        if site_id in coord_site_ids:
            rows_with_coords.append(row)
        else:
            rows_without_coords.append(row)

    print(f"Rows with coordinates: {len(rows_with_coords)}")
    print(f"Rows without coordinates: {len(rows_without_coords)}")

    # Find clusters among sites with coordinates
    print(f"\nFinding clusters within {PROXIMITY_THRESHOLD_KM} km...")
    clusters = find_clusters(sites_with_coords)

    # Separate clusters and singletons
    cluster_groups = []
    singleton_sites = []

    for root_id, group_sites in clusters.items():
        if len(group_sites) > 1:
            cluster_groups.append(group_sites)
        else:
            singleton_sites.extend(group_sites)

    # Sort clusters by size (largest first), then by representative name
    cluster_groups.sort(key=lambda g: (-len(g), g[0].name.lower()))

    print(f"Found {len(cluster_groups)} clusters and {len(singleton_sites)} isolated sites")

    # Build ordered list of site IDs
    ordered_site_ids = []

    # Add clustered sites first
    for group in cluster_groups:
        # Sort sites within cluster by name
        sorted_group = sorted(group, key=lambda s: s.name.lower())
        for site in sorted_group:
            ordered_site_ids.append(site.site_id)

    # Add singleton sites
    sorted_singletons = sorted(singleton_sites, key=lambda s: (s.location or "", s.name.lower()))
    for site in sorted_singletons:
        ordered_site_ids.append(site.site_id)

    # Create lookup from site_id to row data
    site_id_to_row = {row["SITE_SOURCE_ID"]: row for row in rows_with_coords}

    # Build final ordered rows
    ordered_rows = []

    # Add clustered rows in order
    for site_id in ordered_site_ids:
        if site_id in site_id_to_row:
            ordered_rows.append(site_id_to_row[site_id])

    # Add rows without coordinates at the end (sorted by location, name)
    rows_without_coords_sorted = sorted(
        rows_without_coords,
        key=lambda r: (r.get("LOCALISATION", "").lower(), r.get("SITE_NAME", "").lower())
    )
    ordered_rows.extend(rows_without_coords_sorted)

    # Renumber the sites sequentially
    print("\nRenumbering sites...")
    for i, row in enumerate(ordered_rows, start=1):
        row["SITE_SOURCE_ID"] = f"IPAD_{i:03d}"

    # Clean rows to ensure they only contain valid fieldnames
    def clean_row(row):
        return {k: v for k, v in row.items() if k in headers}

    cleaned_rows = [clean_row(row) for row in ordered_rows]

    # Write the reorganized CSV
    print(f"Writing reorganized CSV to {OUTPUT_CSV}...")
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, delimiter=";")
        writer.writeheader()
        writer.writerows(cleaned_rows)

    # Print cluster summary
    print("\n" + "=" * 60)
    print("CLUSTER ORGANIZATION COMPLETE")
    print("=" * 60)
    print(f"Total rows: {len(ordered_rows)}")
    print(f"\nCluster Summary (first 15):")
    print(f"{'Cluster':<45} {'Count':>6} {'New IDs':<15}")
    print("-" * 70)

    counter = 1
    for i, group in enumerate(cluster_groups[:15], 1):
        start_id = counter
        end_id = counter + len(group) - 1
        id_range = f"IPAD_{start_id:03d}-{end_id:03d}" if start_id != end_id else f"IPAD_{start_id:03d}"
        name = group[0].name[:43]
        print(f"{name:<45} {len(group):>6} {id_range:<15}")
        counter += len(group)

    print(f"\n... and {len(cluster_groups) - 15} more clusters")
    print(f"\nIsolated sites: {len(singleton_sites)}")
    print(f"Sites without coordinates: {len(rows_without_coords)}")
    print("=" * 60)


if __name__ == "__main__":
    organize_csv()
