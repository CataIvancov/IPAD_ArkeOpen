#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEXT_ROOT = ROOT / "data" / "google-drive-text"
OUTPUT_CSV = ROOT / "data" / "site-geolocation-candidates.csv"
OUTPUT_REPORT = ROOT / "data" / "site-geolocation-candidates-report.txt"

MULTISPACE = re.compile(r"\s+")

SITE_PATTERNS = [
    r"\b(?:Leang|Gua|Goa|Ceruk|Liang|Bukit)\s+[A-Z][A-Za-z0-9'’._-]*(?:\s+[A-Z][A-Za-z0-9'’._-]*){0,5}\b",
    r"\b[A-Z][A-Za-z0-9'’._-]*(?:\s+[A-Z][A-Za-z0-9'’._-]*){0,5}\s+(?:Cave|Caves|Site|Sites|Rockshelter|Rock Shelter|Shelter|Complex)\b",
]
SITE_REGEXES = [re.compile(pattern) for pattern in SITE_PATTERNS]
GENERIC_SITE_NAMES = {
    "Archaeological Site",
    "Archaeological Sites",
    "Prehistoric Site",
    "Prehistoric Sites",
    "Protohistoric Site",
    "Protohistoric Sites",
    "Historic Site",
    "Historic Sites",
    "A Prehistoric Site",
    "A Protohistoric Site",
    "A Well Dated Paleolithic Cave",
}

DECIMAL_LAT_LON_REGEX = re.compile(
    r"(?P<lat>\d{1,2}\.\d+)\s*(?:°)?\s*(?P<lat_dir>N|S|North|South)\s+(?:Latitude)?"
    r"(?:\s*(?:and|,)\s*)"
    r"(?P<lon>\d{1,3}\.\d+)\s*(?:°)?\s*(?P<lon_dir>E|W|East|West)\s+(?:Longitude)?",
    re.IGNORECASE,
)
DECIMAL_LON_LAT_REGEX = re.compile(
    r"(?P<lon>\d{1,3}\.\d+)\s*(?P<lon_dir>E|W|East|West)\s+(?:Longitude)?"
    r"(?:\s*(?:and|,)\s*)"
    r"(?P<lat>\d{1,2}\.\d+)\s*(?P<lat_dir>N|S|North|South)\s+(?:Latitude)?",
    re.IGNORECASE,
)
DMS_PAIR_REGEX = re.compile(
    r"(?P<lat_deg>\d{1,2})°\s*(?P<lat_min>\d{1,2})['′]\s*(?P<lat_sec>\d{1,2}(?:\.\d+)?)?[\"]?\s*(?P<lat_dir>[NS])"
    r"[\s,;/]+"
    r"(?P<lon_deg>\d{1,3})°\s*(?P<lon_min>\d{1,2})['′]\s*(?P<lon_sec>\d{1,2}(?:\.\d+)?)?[\"]?\s*(?P<lon_dir>[EW])",
    re.IGNORECASE,
)
UTM_REGEX = re.compile(
    r"\bUTM\s*(?P<zone>\d{1,2}[A-Z]?)\s+(?P<easting>\d{3,8}(?:\.\d+)?)\s+(?P<northing>\d{3,8}(?:\.\d+)?)\b",
    re.IGNORECASE,
)
DECIMAL_PAIR_REGEX = re.compile(
    r"\b(?P<lat>-?\d{1,2}\.\d{3,})\s*[,;/]\s*(?P<lon>-?\d{1,3}\.\d{3,})\b"
)


@dataclass(frozen=True)
class Candidate:
    site_name: str
    latitude: str
    longitude: str
    coordinate_text: str
    coordinate_system: str
    source_file: str
    page: int
    evidence_snippet: str


def clean(text: str) -> str:
    return MULTISPACE.sub(" ", text).strip()


def dms_to_decimal(deg: str, minutes: str, seconds: str | None, direction: str) -> float:
    value = float(deg) + float(minutes) / 60.0
    if seconds:
      value += float(seconds) / 3600.0
    if direction.upper() in {"S", "W"}:
        value *= -1
    return value


def signed_decimal(value: str, direction: str) -> float:
    number = float(value)
    if direction.lower() in {"s", "south", "w", "west"}:
        number *= -1
    return number


def fmt(value: float | None) -> str:
    return "" if value is None else f"{value:.6f}"


def snippet_around(text: str, start: int, end: int, width: int = 220) -> str:
    left = max(0, start - width)
    right = min(len(text), end + width)
    return clean(text[left:right])


def normalize_site_name(value: str) -> str:
    return clean(value).strip(" ,;:.()[]")


def site_names_from_snippet(snippet: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for regex in SITE_REGEXES:
        for match in regex.finditer(snippet):
            name = normalize_site_name(match.group(0))
            key = name.lower()
            if len(name) < 4 or key in seen or name in GENERIC_SITE_NAMES:
                continue
            seen.add(key)
            names.append(name)
    return names


def coordinate_matches(page_text: str) -> list[tuple[int, int, str, str, str, str]]:
    matches: list[tuple[int, int, str, str, str, str]] = []

    for regex, system in (
        (DECIMAL_LAT_LON_REGEX, "wgs84_decimal"),
        (DECIMAL_LON_LAT_REGEX, "wgs84_decimal"),
    ):
        for match in regex.finditer(page_text):
            lat = signed_decimal(match.group("lat"), match.group("lat_dir"))
            lon = signed_decimal(match.group("lon"), match.group("lon_dir"))
            matches.append((match.start(), match.end(), fmt(lat), fmt(lon), clean(match.group(0)), system))

    for match in DMS_PAIR_REGEX.finditer(page_text):
        lat = dms_to_decimal(match.group("lat_deg"), match.group("lat_min"), match.group("lat_sec"), match.group("lat_dir"))
        lon = dms_to_decimal(match.group("lon_deg"), match.group("lon_min"), match.group("lon_sec"), match.group("lon_dir"))
        matches.append((match.start(), match.end(), fmt(lat), fmt(lon), clean(match.group(0)), "wgs84_dms"))

    for match in DECIMAL_PAIR_REGEX.finditer(page_text):
        lat = float(match.group("lat"))
        lon = float(match.group("lon"))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            matches.append((match.start(), match.end(), fmt(lat), fmt(lon), clean(match.group(0)), "decimal_pair"))

    for match in UTM_REGEX.finditer(page_text):
        matches.append((match.start(), match.end(), "", "", clean(match.group(0)), "utm"))

    matches.sort(key=lambda item: item[0])
    return matches


def candidates_for_page(source_file: str, page_no: int, page_text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen: set[tuple[str, str, str, int]] = set()

    for start, end, lat, lon, coord_text, system in coordinate_matches(page_text):
        snippet = snippet_around(page_text, start, end)
        site_names = site_names_from_snippet(snippet) or [""]
        for site_name in site_names:
            key = (site_name.lower(), lat, lon, page_no)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                Candidate(
                    site_name=site_name,
                    latitude=lat,
                    longitude=lon,
                    coordinate_text=coord_text,
                    coordinate_system=system,
                    source_file=source_file,
                    page=page_no,
                    evidence_snippet=snippet,
                )
            )

    # Also collect site-name-only candidates when no coordinates are visible nearby.
    for site_name in site_names_from_snippet(page_text):
        key = (site_name.lower(), "", "", page_no)
        if key in seen:
            continue
        seen.add(key)
        match = re.search(re.escape(site_name), page_text)
        snippet = snippet_around(page_text, match.start(), match.end()) if match else site_name
        candidates.append(
            Candidate(
                site_name=site_name,
                latitude="",
                longitude="",
                coordinate_text="",
                coordinate_system="",
                source_file=source_file,
                page=page_no,
                evidence_snippet=snippet,
            )
        )

    return candidates


def load_text_payloads() -> list[Path]:
    return sorted(TEXT_ROOT.rglob("*.json"))


def mine_candidates() -> list[Candidate]:
    all_candidates: list[Candidate] = []
    for path in load_text_payloads():
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_file = payload.get("source_pdf", str(path))
        page_texts = payload.get("page_texts", [])
        for page_index, page_text in enumerate(page_texts, start=1):
            if not page_text:
                continue
            all_candidates.extend(candidates_for_page(source_file, page_index, page_text))
    return all_candidates


def write_csv(candidates: list[Candidate]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "site_name",
                "latitude",
                "longitude",
                "coordinate_text",
                "coordinate_system",
                "source_file",
                "page",
                "evidence_snippet",
            ],
        )
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(candidate.__dict__)


def write_report(candidates: list[Candidate]) -> None:
    with_coords = [candidate for candidate in candidates if candidate.coordinate_text]
    unique_sites = sorted({candidate.site_name for candidate in candidates if candidate.site_name})
    lines = [
        f"Candidates: {len(candidates)}",
        f"Candidates with coordinates: {len(with_coords)}",
        f"Unique site names: {len(unique_sites)}",
        "Top coordinate systems:",
    ]
    systems: dict[str, int] = {}
    for candidate in with_coords:
        systems[candidate.coordinate_system] = systems.get(candidate.coordinate_system, 0) + 1
    for system, count in sorted(systems.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"  {system}: {count}")
    lines.append("Sample site names:")
    for site_name in unique_sites[:40]:
        lines.append(f"  {site_name}")
    OUTPUT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not TEXT_ROOT.exists():
        print(f"Missing extracted text directory: {TEXT_ROOT}", file=sys.stderr)
        return 1
    candidates = mine_candidates()
    write_csv(candidates)
    write_report(candidates)
    print(f"Wrote {len(candidates)} site/geolocation candidates to {OUTPUT_CSV}")
    print(f"Report: {OUTPUT_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
