#!/usr/bin/env python3
"""
Group archaeological sites by geographic proximity.
Sites within 10km of each other are considered "near".
"""

import csv
import math
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass

ROOT = Path(__file__).resolve().parents[1]
CSV_FILE = ROOT / "data" / "ipad-sites-combined.csv"
OUTPUT_FILE = ROOT / "data" / "site_proximity_groups.txt"

# Distance threshold in kilometers
PROXIMITY_THRESHOLD_KM = 10.0


@dataclass
class Site:
    site_id: str
    name: str
    location: str
    longitude: float
    latitude: float
    site_type: str


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in kilometers using Haversine formula."""
    R = 6371.0  # Earth's radius in kilometers

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def load_sites() -> list[Site]:
    """Load sites from CSV file."""
    sites = []
    with CSV_FILE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            try:
                lon = float(row["LONGITUDE"]) if row["LONGITUDE"] else None
                lat = float(row["LATITUDE"]) if row["LATITUDE"] else None
                if lon is not None and lat is not None:
                    sites.append(Site(
                        site_id=row["SITE_SOURCE_ID"],
                        name=row["SITE_NAME"],
                        location=row["LOCALISATION"],
                        longitude=lon,
                        latitude=lat,
                        site_type=row.get("CHARAC_LVL2", "Unknown")
                    ))
            except (ValueError, KeyError):
                continue
    return sites


def find_proximity_groups(sites: list[Site]) -> dict[str, list[Site]]:
    """Group sites by geographic proximity using Union-Find algorithm."""
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
    site_map = {site.site_id: site for site in sites}
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


def format_group(group_id: str, sites: list[Site]) -> str:
    """Format a group of sites for output."""
    if len(sites) == 1:
        return None  # Skip singletons

    lines = []
    lines.append(f"{'=' * 60}")
    lines.append(f"SITE CLUSTER: {sites[0].name} region")
    lines.append(f"Location: {sites[0].location}")
    lines.append(f"Sites in cluster: {len(sites)}")
    lines.append(f"{'-' * 60}")

    for site in sorted(sites, key=lambda s: s.name):
        lines.append(f"  • {site.site_id}: {site.name}")
        lines.append(f"    Coordinates: {site.latitude:.6f}, {site.longitude:.6f}")
        lines.append(f"    Type: {site.site_type}")
        lines.append("")

    # Calculate pairwise distances within group
    if len(sites) > 1:
        lines.append("  Inter-site distances:")
        for i in range(len(sites)):
            for j in range(i + 1, len(sites)):
                dist = haversine_distance(
                    sites[i].latitude, sites[i].longitude,
                    sites[j].latitude, sites[j].longitude
                )
                lines.append(f"    • {sites[i].name} ↔ {sites[j].name}: {dist:.2f} km")
        lines.append("")

    return "\n".join(lines)


def main():
    print("Loading sites...")
    sites = load_sites()
    print(f"Loaded {len(sites)} sites with coordinates")

    print(f"\nGrouping sites within {PROXIMITY_THRESHOLD_KM} km...")
    groups = find_proximity_groups(sites)

    # Separate clusters from singletons
    clusters = {k: v for k, v in groups.items() if len(v) > 1}
    singletons = [v[0] for k, v in groups.items() if len(v) == 1]

    print(f"Found {len(clusters)} clusters and {len(singletons)} isolated sites")

    # Sort clusters by size (largest first)
    sorted_clusters = sorted(clusters.values(), key=lambda g: -len(g))

    # Generate report
    output_lines = []
    output_lines.append("ARCHAEOLOGICAL SITE PROXIMITY GROUPS")
    output_lines.append("=" * 60)
    output_lines.append(f"Threshold: {PROXIMITY_THRESHOLD_KM} km")
    output_lines.append(f"Total sites: {len(sites)}")
    output_lines.append(f"Clusters: {len(clusters)}")
    output_lines.append(f"Isolated sites: {len(singletons)}")
    output_lines.append("")

    # Summary table
    output_lines.append("CLUSTER SUMMARY")
    output_lines.append("-" * 60)
    output_lines.append(f"{'Cluster':<40} {'Count':>8} {'Region':<20}")
    output_lines.append("-" * 60)

    for i, group in enumerate(sorted_clusters, 1):
        name = group[0].name[:38]
        count = len(group)
        region = group[0].location[:18] if group[0].location else "Unknown"
        output_lines.append(f"{name:<40} {count:>8} {region:<20}")

    output_lines.append("")
    output_lines.append("DETAILED CLUSTER INFORMATION")
    output_lines.append("=" * 60)
    output_lines.append("")

    for group in sorted_clusters:
        formatted = format_group(group[0].site_id, group)
        if formatted:
            output_lines.append(formatted)

    # Write output
    output_text = "\n".join(output_lines)
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        f.write(output_text)

    print(f"\nReport saved to: {OUTPUT_FILE}")
    print("\nCluster Summary:")
    print("-" * 60)
    for i, group in enumerate(sorted_clusters[:10], 1):
        print(f"{i}. {group[0].name} ({group[0].location}): {len(group)} sites")


if __name__ == "__main__":
    main()
