#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import pathlib
import subprocess
from collections import Counter, OrderedDict, defaultdict

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

CONTAINER = "arkeopenlocal-postgres"
DB = "arkeopen"
FILE_GLOB = "ipad-sites-*.csv"
EXCLUDED_FILES = {"ipad-sites-organized.csv"}
COLLECTION_NAME = "IPAD Sites"
LICENSE_NAME = "CC-BY-NC-ND-4.0"
LICENSE_URL = "https://spdx.org/licenses/CC-BY-NC-ND-4.0.html#licenseText"
COUNTRY_ID = 0
ROOT_CHRONOLOGY_ID = 970000
CITY_BASE = 990000
CITY_FILE_STRIDE = 1000
UNDETERMINED_LEFT = -2147483648
UNDETERMINED_RIGHT = 2147483647

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

KNOWLEDGE_TYPE_MAP = {
    "documented": "literature",
    "excavated": "dig",
    "excavated.": "dig",
    "foot survey": "prospected_pedestrian",
    "literature": "literature",
}

OCCUPATION_MAP = {
    "not specified": "not_documented",
}

PATH_FIXES = {
    "Pic tograph": "Pictograph",
}

PATH_ALIASES = {
    "Archaeological Sites > Rock art site / Petroglyph / Pictograph / Hand stencil":
        "Stationary Structures > Rock art site / Petroglyph / Pictograph / Hand stencil",
    "Archaeological Sites > Megalithic site":
        "Stationary Structures > Megalithic site / Menhir / Dolmen / Sarcophagus / Waruga",
    "Archaeological Sites > Megalithic site / Burial site":
        "Stationary Structures > Megalithic site / Menhir / Dolmen / Sarcophagus / Waruga",
    "Archaeological Sites > Necropolis / Burial site":
        "Archaeological Sites > Burial / Cemetery / Kubur / Makam",
    "Archaeological Sites > Ceremonial site / Burial site / Habitation site":
        "Archaeological Sites > Open-air site",
    "Stationary Structures > Archaeological site": "Archaeological Sites",
    "Analyses > Dating > Radiocarbon > Reference sample":
        "Analyses > Dating > Radiocarbon",
    # Deep paths (5+ levels) - truncate to 4 levels
    "Human remains > Homo erectus > Cranial > Teeth > Incisor":
        "Human remains > Homo erectus > Cranial > Teeth",
    "Human remains > Homo erectus > Cranial > Teeth > Indeterminate":
        "Human remains > Homo erectus > Cranial > Teeth",
    "Human remains > Homo erectus > Cranial > Teeth > Molar":
        "Human remains > Homo erectus > Cranial > Teeth",
    "Human remains > Homo erectus > Postcranial > Lower limb > Femur":
        "Human remains > Homo erectus > Postcranial > Lower limb",
    "Portable Objects / Artefacts > Stone > Others > Others > Chert":
        "Portable Objects / Artefacts > Stone > Others > Others",
    "Portable Objects / Artefacts > Stone > Others > Others > Limestone":
        "Portable Objects / Artefacts > Stone > Others > Others",
    "Portable Objects / Artefacts > Stone > Others > Others > Other material":
        "Portable Objects / Artefacts > Stone > Others > Others",
    "Portable Objects / Artefacts > Stone > Raw material > Pigment > Ochre":
        "Portable Objects / Artefacts > Stone > Raw material > Pigment",
    "Portable Objects / Artefacts > Stone > Tools > Backed Microlith > Others":
        "Portable Objects / Artefacts > Stone > Tools > Backed Microlith",
    "Portable Objects / Artefacts > Stone > Tools > Flake > Chert":
        "Portable Objects / Artefacts > Stone > Tools > Flake",
    "Portable Objects / Artefacts > Stone > Tools > Maros Point > Others":
        "Portable Objects / Artefacts > Stone > Tools > Maros Point",
    "Stationary Structures > Rock art site / Petroglyph / Pictograph / Hand stencil > Undocumented > Undocumented > Burial":
        "Stationary Structures > Rock art site / Petroglyph / Pictograph / Hand stencil > Undocumented > Undocumented",
}

UPPER_WORDS = {"ntt", "ipad"}

COORDINATE_OVERRIDES = {
    ("Sangiran", "Java"): (110.841, -7.443),
}


def sh(cmd: list[str], *, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def query_value(sql: str) -> str:
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", DB, "-Atc", sql]).stdout.strip()


def exec_sql(sql: str) -> None:
    sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", DB], stdin=sql)


def normalize_ascii(value: str) -> str:
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "î": "i", "ï": "i",
        "ô": "o", "ö": "o",
        "ù": "u", "û": "u", "ü": "u",
        "ç": "c", "œ": "oe", "’": "'", "‘": "'",
        "â": "'", "â": "-", "â": "-",
        "É": "E", "À": "A", "Ç": "C",
    }
    out = value
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    return out


def slug_from_path(path: pathlib.Path) -> str:
    return path.stem.removeprefix("ipad-sites-")


def title_from_slug(slug: str) -> str:
    words = []
    for part in slug.split("-"):
        if part in UPPER_WORDS:
            words.append(part.upper())
        else:
            words.append(part.capitalize())
    return " ".join(words)


def dataset_name_from_slug(slug: str) -> str:
    return f"IPAD sites: {title_from_slug(slug)}"


def subject_from_slug(slug: str) -> str:
    return f"ipad; regional csv import; {slug.replace('-', '; ')}"


def parse_period(value: str, side: str) -> int:
    text = (value or "").strip()
    if not text or text.lower() == "undefined":
        return UNDETERMINED_LEFT if side == "left" else UNDETERMINED_RIGHT
    return int(text)


def normalize_bool(value: str) -> bool:
    return (value or "").strip().lower() in {"oui", "yes", "true"}


def parse_float(value: str) -> float | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def centroid(points: list[tuple[float, float]]) -> tuple[float, float]:
    return (
        sum(lon for lon, _ in points) / len(points),
        sum(lat for _, lat in points) / len(points),
    )


def normalize_dedupe_text(value: str) -> str:
    return " ".join(normalize_ascii(value).lower().split())


def coordinate_mode_rank(mode: str) -> int:
    ranks = {
        "override": 4,
        "direct": 3,
        "same_file": 2,
        "global": 1,
        "zero": 0,
    }
    return ranks.get(mode, -1)


def coordinate_merge_key(longitude: float, latitude: float) -> tuple[int, int] | None:
    if longitude == 0.0 and latitude == 0.0:
        return None
    return (round(longitude * 1_000_000), round(latitude * 1_000_000))


def resource_family_from_path(path: str) -> str:
    return path.split(" > ", 1)[0].strip()


def site_resource_key(ranges: list[dict[str, object]]) -> tuple[str, ...]:
    return tuple(sorted({resource_family_from_path(item["charac_path"]) for item in ranges}))


def collect_source_files() -> list[pathlib.Path]:
    files = sorted(path for path in DATA_DIR.glob(FILE_GLOB) if path.name not in EXCLUDED_FILES)
    return files


def load_rows(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        raw_header = next(reader)
        header = [col.strip() for col in raw_header]
        if header != EXPECTED_HEADERS:
            raise RuntimeError(f"{path.name}: header mismatch: expected {EXPECTED_HEADERS}, got {header}")

        rows = []
        for line_no, values in enumerate(reader, start=2):
            if len(values) > len(header):
                values = values[: len(header) - 1] + ["; ".join(part.strip() for part in values[len(header) - 1:] if part.strip())]
            if len(values) != len(header):
                raise RuntimeError(f"{path.name}: row {line_no} has {len(values)} columns; expected {len(header)}")
            row = {header[idx]: values[idx].strip() for idx in range(len(header))}
            row["_line_no"] = str(line_no)
            # Skip rows without coordinates (as per user requirement)
            lon = row.get("LONGITUDE", "").strip()
            lat = row.get("LATITUDE", "").strip()
            if not lon or not lat:
                print(f"[skip-no-coords] {path.name}:{line_no} {row.get('SITE_SOURCE_ID', '?')} {row.get('SITE_NAME', '?')}")
                continue
            # Skip rows with unsupported characteristic roots (Analyses not in thesaurus)
            main_charac = row.get("MAIN_CHARAC", "").strip()
            if main_charac == "Analyses":
                print(f"[skip-unsupported-charac] {path.name}:{line_no} {row.get('SITE_SOURCE_ID', '?')} {row.get('SITE_NAME', '?')} (Analyses not in thesaurus)")
                continue
            rows.append(row)
    return rows


def load_charac_paths() -> dict[str, int]:
    sql = (
        "WITH RECURSIVE tree AS ("
        "SELECT c.id, c.parent_id, ct.name, ct.name::text AS path "
        "FROM public.charac c "
        "JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode = 'en' "
        "WHERE c.parent_id = 0 "
        "UNION ALL "
        "SELECT c.id, c.parent_id, ct.name, (tree.path || ' > ' || ct.name) "
        "FROM public.charac c "
        "JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode = 'en' "
        "JOIN tree ON c.parent_id = tree.id"
        ") "
        "SELECT id, path FROM tree ORDER BY path;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", DB, "-AtF", "|", "-c", sql]).stdout
    mapping = {}
    for line in result.splitlines():
        if not line:
            continue
        charac_id, path = line.split("|", 1)
        mapping[path] = int(charac_id)
    return mapping


def normalize_path_part(key: str, value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    for src, dst in PATH_FIXES.items():
        text = text.replace(src, dst)
    if key == "MAIN_CHARAC" and text == "Archaeological Site":
        return "Archaeological Sites"
    return text


def resolve_charac(row: dict[str, str], charac_paths: dict[str, int], stats: Counter, source_name: str) -> tuple[int, str]:
    parts = []
    for key in ("MAIN_CHARAC", "CHARAC_LVL1", "CHARAC_LVL2", "CHARAC_LVL3", "CHARAC_LVL4"):
        value = normalize_path_part(key, row.get(key, ""))
        if value:
            parts.append(value)

    if not parts:
        raise RuntimeError(f"{source_name}:{row['_line_no']} has no characteristic path")

    original = " > ".join(parts)
    candidate = PATH_ALIASES.get(original, original)
    if candidate in charac_paths:
        return charac_paths[candidate], candidate

    candidate_parts = candidate.split(" > ")
    for length in range(len(candidate_parts) - 1, 0, -1):
        prefix = " > ".join(candidate_parts[:length])
        prefix = PATH_ALIASES.get(prefix, prefix)
        if prefix in charac_paths:
            stats["charac_truncation_rows"] += 1
            print(
                f"[charac-truncate] {source_name}:{row['_line_no']} "
                f"{row['SITE_SOURCE_ID']} -> {prefix} (from {original})"
            )
            return charac_paths[prefix], prefix

    raise RuntimeError(
        f"{source_name}:{row['_line_no']} could not map characteristic path {original!r} to the local thesaurus"
    )


def build_locality_centroids(rows: list[dict[str, str]]) -> dict[str, tuple[float, float]]:
    grouped: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        lon = parse_float(row["LONGITUDE"])
        lat = parse_float(row["LATITUDE"])
        if lon is None or lat is None:
            continue
        grouped[row["LOCALISATION"]].append((lon, lat))
    return {locality: centroid(points) for locality, points in grouped.items()}


def resolve_coordinates(
    site_rows: list[dict[str, str]],
    same_file_centroids: dict[str, tuple[float, float]],
    global_centroids: dict[str, tuple[float, float]],
    stats: Counter,
    source_name: str,
) -> tuple[float, float, str]:
    first_row = site_rows[0]
    override_key = (first_row["SITE_NAME"], first_row["LOCALISATION"])
    if override_key in COORDINATE_OVERRIDES:
        stats["coordinate_override_sites"] += 1
        return *COORDINATE_OVERRIDES[override_key], "override"

    for row in site_rows:
        lon = parse_float(row["LONGITUDE"])
        lat = parse_float(row["LATITUDE"])
        if lon is not None and lat is not None:
            return lon, lat, "direct"

    locality = site_rows[0]["LOCALISATION"]
    if locality in same_file_centroids:
        stats["coordinate_fallback_same_file_sites"] += 1
        return *same_file_centroids[locality], "same_file"
    if locality in global_centroids:
        stats["coordinate_fallback_global_sites"] += 1
        return *global_centroids[locality], "global"

    stats["coordinate_fallback_zero_sites"] += 1
    print(
        f"[coord-zero] {source_name}:{site_rows[0]['_line_no']} "
        f"{site_rows[0]['SITE_SOURCE_ID']} has no usable coordinates; importing at POINT(0 0)"
    )
    return 0.0, 0.0, "zero"


def normalize_knowledge(value: str) -> str:
    return KNOWLEDGE_TYPE_MAP.get((value or "").strip().lower(), "not_documented")


def normalize_occupation(value: str) -> str:
    return OCCUPATION_MAP.get((value or "").strip().lower(), "not_documented")


def build_site_date_span(ranges: list[dict[str, int]]) -> tuple[int, int, int, int]:
    start1_values = [item["start_date1"] for item in ranges if item["start_date1"] != UNDETERMINED_LEFT]
    start2_values = [item["start_date2"] for item in ranges if item["start_date2"] != UNDETERMINED_RIGHT]
    end1_values = [item["end_date1"] for item in ranges if item["end_date1"] != UNDETERMINED_LEFT]
    end2_values = [item["end_date2"] for item in ranges if item["end_date2"] != UNDETERMINED_RIGHT]
    return (
        min(start1_values) if start1_values else UNDETERMINED_LEFT,
        min(start2_values) if start2_values else UNDETERMINED_RIGHT,
        max(end1_values) if end1_values else UNDETERMINED_LEFT,
        max(end2_values) if end2_values else UNDETERMINED_RIGHT,
    )


def build_dataset(
    path: pathlib.Path,
    file_index: int,
    rows: list[dict[str, str]],
    charac_paths: dict[str, int],
    global_centroids: dict[str, tuple[float, float]],
    run_stats: Counter,
) -> dict[str, object]:
    slug = slug_from_path(path)
    title = title_from_slug(slug)
    source_name = path.name
    same_file_centroids = build_locality_centroids(rows)
    grouped: dict[str, list[dict[str, str]]] = OrderedDict()
    for row in rows:
        grouped.setdefault(row["SITE_SOURCE_ID"], []).append(row)

    site_records = []
    locality_points: dict[str, list[tuple[float, float]]] = defaultdict(list)
    bbox_points: list[tuple[float, float]] = []

    for site_source_id, site_rows in grouped.items():
        site_id = f"ipad-{slug}-{site_source_id}"
        first_row = site_rows[0]
        longitude, latitude, coordinate_mode = resolve_coordinates(
            site_rows, same_file_centroids, global_centroids, run_stats, source_name
        )
        locality = first_row["LOCALISATION"] or first_row["SITE_NAME"]
        locality_points[locality].append((longitude, latitude))
        if coordinate_mode != "zero":
            bbox_points.append((longitude, latitude))

        site_ranges = []
        unique_comments = []
        seen_comments = set()
        for row in site_rows:
            charac_id, resolved_path = resolve_charac(row, charac_paths, run_stats, source_name)
            start = parse_period(row["STARTING_PERIOD"], "left")
            end = parse_period(row["ENDING_PERIOD"], "right")
            comment = row["COMMENTS"]
            if coordinate_mode == "zero":
                note = "Import note: source coordinates unavailable; placeholder POINT(0 0) used."
                comment = f"{comment}\n\n{note}".strip() if comment else note
            if comment and comment not in seen_comments:
                seen_comments.add(comment)
                unique_comments.append(comment)
            site_ranges.append(
                {
                    "start_date1": start,
                    "start_date2": start if start != UNDETERMINED_LEFT else UNDETERMINED_RIGHT,
                    "end_date1": end if end != UNDETERMINED_RIGHT else UNDETERMINED_LEFT,
                    "end_date2": end,
                    "charac_id": charac_id,
                    "charac_path": resolved_path,
                    "knowledge_type": normalize_knowledge(row["STATE_OF_KNOWLEDGE"]),
                    "exceptional": normalize_bool(row["CHARAC_EXP"]),
                    "bibliography": row["BIBLIOGRAPHY"],
                    "comment": comment,
                }
            )

        start_date1, start_date2, end_date1, end_date2 = build_site_date_span(site_ranges)
        site_records.append(
            {
                "id": site_id,
                "code": site_source_id,
                "name": first_row["SITE_NAME"],
                "locality": locality,
                "resource_key": site_resource_key(site_ranges),
                "dedupe_key": (
                    normalize_dedupe_text(first_row["SITE_NAME"]),
                    normalize_dedupe_text(locality),
                    site_resource_key(site_ranges),
                ),
                "coordinate_merge_key": coordinate_merge_key(longitude, latitude),
                "longitude": longitude,
                "latitude": latitude,
                "coordinate_mode": coordinate_mode,
                "altitude": parse_float(first_row["ALTITUDE"]) or 0.0,
                "centroid": normalize_bool(first_row["CITY_CENTROID"]) or coordinate_mode != "direct",
                "occupation": normalize_occupation(first_row["OCCUPATION"]),
                "description": "\n\n".join(unique_comments).strip(),
                "source_rows": site_rows,
                "ranges": site_ranges,
                "start_date1": start_date1,
                "start_date2": start_date2,
                "end_date1": end_date1,
                "end_date2": end_date2,
            }
        )

    city_map = OrderedDict()
    for locality_index, locality in enumerate(sorted(locality_points), start=1):
        lon, lat = centroid(locality_points[locality])
        city_map[locality] = {
            "geonameid": CITY_BASE + (file_index * CITY_FILE_STRIDE) + locality_index,
            "name": locality,
            "name_ascii": normalize_ascii(locality),
            "longitude": lon,
            "latitude": lat,
        }

    for site in site_records:
        site["city_geonameid"] = city_map[site["locality"]]["geonameid"]

    finite_starts = [site["start_date1"] for site in site_records if site["start_date1"] != UNDETERMINED_LEFT]
    finite_ends = [site["end_date2"] for site in site_records if site["end_date2"] != UNDETERMINED_RIGHT]
    if bbox_points:
        min_lon = min(lon for lon, _ in bbox_points)
        max_lon = max(lon for lon, _ in bbox_points)
        min_lat = min(lat for _, lat in bbox_points)
        max_lat = max(lat for _, lat in bbox_points)
    else:
        min_lon = max_lon = min_lat = max_lat = 0.0

    run_stats["datasets_selected"] += 1
    run_stats["source_rows"] += len(rows)
    run_stats["sites_built"] += len(site_records)

    return {
        "slug": slug,
        "title": title,
        "file_index": file_index,
        "path": path,
        "name": dataset_name_from_slug(slug),
        "description": (
            f"Regional IPAD dataset imported from {source_name}. "
            f"This local batch import preserves the CSV as a distinct dataset inside ArkeOpen."
        ),
        "bibliography": f"Local IPAD regional CSV import from {source_name}.",
        "source_description": f"Local CSV import ({source_name})",
        "source_relation": "",
        "subject": subject_from_slug(slug),
        "copyright": "Local working copy",
        "reuse": "Local evaluation import only.",
        "geographical_limit": title,
        "context_description": "Regional IPAD CSV import",
        "bbox": (min_lon, min_lat, max_lon, max_lat),
        "cities": list(city_map.values()),
        "sites": site_records,
        "start_date": min(finite_starts) if finite_starts else -2578050,
        "end_date": max(finite_ends) if finite_ends else 1950,
    }


def merge_descriptions(left: str, right: str) -> str:
    chunks = []
    seen = set()
    for value in [left, right]:
        for piece in [part.strip() for part in value.split("\n\n") if part.strip()]:
            if piece not in seen:
                seen.add(piece)
                chunks.append(piece)
    return "\n\n".join(chunks)


def recompute_site_dates(site: dict[str, object]) -> None:
    start_date1, start_date2, end_date1, end_date2 = build_site_date_span(site["ranges"])
    site["start_date1"] = start_date1
    site["start_date2"] = start_date2
    site["end_date1"] = end_date1
    site["end_date2"] = end_date2


def range_signature(range_record: dict[str, object]) -> tuple[object, ...]:
    return (
        range_record["start_date1"],
        range_record["start_date2"],
        range_record["end_date1"],
        range_record["end_date2"],
        range_record["charac_id"],
        range_record["knowledge_type"],
        range_record["exceptional"],
        range_record["bibliography"],
        range_record["comment"],
    )


def merge_site_records(canonical: dict[str, object], duplicate: dict[str, object], stats: Counter) -> None:
    existing_range_keys = {range_signature(item) for item in canonical["ranges"]}
    for range_record in duplicate["ranges"]:
        key = range_signature(range_record)
        if key not in existing_range_keys:
            canonical["ranges"].append(range_record)
            existing_range_keys.add(key)

    canonical["description"] = merge_descriptions(canonical["description"], duplicate["description"])
    canonical["source_rows"].extend(duplicate["source_rows"])

    if coordinate_mode_rank(duplicate["coordinate_mode"]) > coordinate_mode_rank(canonical["coordinate_mode"]):
        canonical["longitude"] = duplicate["longitude"]
        canonical["latitude"] = duplicate["latitude"]
        canonical["altitude"] = duplicate["altitude"]
        canonical["centroid"] = duplicate["centroid"]
        canonical["coordinate_mode"] = duplicate["coordinate_mode"]
        canonical["locality"] = duplicate["locality"]

    canonical["resource_key"] = site_resource_key(canonical["ranges"])
    canonical["dedupe_key"] = (
        normalize_dedupe_text(canonical["name"]),
        normalize_dedupe_text(canonical["locality"]),
        canonical["resource_key"],
    )
    recompute_site_dates(canonical)
    stats["dedupe_merged_sites"] += 1


def recompute_dataset_fields(dataset: dict[str, object]) -> None:
    sites = dataset["sites"]
    locality_points: dict[str, list[tuple[float, float]]] = defaultdict(list)
    bbox_points: list[tuple[float, float]] = []
    for site in sites:
        locality_points[site["locality"]].append((site["longitude"], site["latitude"]))
        if site["coordinate_mode"] != "zero":
            bbox_points.append((site["longitude"], site["latitude"]))

    city_map = OrderedDict()
    for locality_index, locality in enumerate(sorted(locality_points), start=1):
        lon, lat = centroid(locality_points[locality])
        city_map[locality] = {
            "geonameid": CITY_BASE + (dataset["file_index"] * CITY_FILE_STRIDE) + locality_index,
            "name": locality,
            "name_ascii": normalize_ascii(locality),
            "longitude": lon,
            "latitude": lat,
        }

    for site in sites:
        site["city_geonameid"] = city_map[site["locality"]]["geonameid"]

    finite_starts = [site["start_date1"] for site in sites if site["start_date1"] != UNDETERMINED_LEFT]
    finite_ends = [site["end_date2"] for site in sites if site["end_date2"] != UNDETERMINED_RIGHT]
    if bbox_points:
        min_lon = min(lon for lon, _ in bbox_points)
        max_lon = max(lon for lon, _ in bbox_points)
        min_lat = min(lat for _, lat in bbox_points)
        max_lat = max(lat for _, lat in bbox_points)
    else:
        min_lon = max_lon = min_lat = max_lat = 0.0

    dataset["cities"] = list(city_map.values())
    dataset["bbox"] = (min_lon, min_lat, max_lon, max_lat)
    dataset["start_date"] = min(finite_starts) if finite_starts else -2578050
    dataset["end_date"] = max(finite_ends) if finite_ends else 1950


def merge_duplicate_sites(datasets: list[dict[str, object]], stats: Counter) -> None:
    canonical_by_key: dict[tuple[str, str, tuple[str, ...]], dict[str, object]] = {}
    canonical_by_coord_key: dict[tuple[str, tuple[str, ...], tuple[int, int]], dict[str, object]] = {}
    dedupe_groups: Counter = Counter()
    for dataset in datasets:
        merged_sites = []
        for site in dataset["sites"]:
            key = site["dedupe_key"]
            coordinate_key = site["coordinate_merge_key"]
            canonical = canonical_by_key.get(key)
            if canonical is None and coordinate_key is not None:
                canonical = canonical_by_coord_key.get((key[0], key[2], coordinate_key))
            if canonical is None:
                canonical_by_key[key] = site
                if coordinate_key is not None:
                    canonical_by_coord_key[(key[0], key[2], coordinate_key)] = site
                merged_sites.append(site)
                continue
            merge_site_records(canonical, site, stats)
            dedupe_groups[key] += 1
            print(
                f"[dedupe-merge] {site['name']} / {site['locality']} "
                f"{site['id']} -> {canonical['id']}"
            )
        dataset["sites"] = merged_sites

    stats["dedupe_groups"] = len(dedupe_groups)
    stats["sites_after_dedupe"] = sum(len(dataset["sites"]) for dataset in datasets)
    for dataset in datasets:
        recompute_dataset_fields(dataset)


def ensure_support_rows() -> None:
    next_license_id = int(query_value("SELECT COALESCE(MAX(id), 0) + 1 FROM public.license;"))
    statements = [
        "BEGIN;",
        "CREATE EXTENSION IF NOT EXISTS unaccent;",
        "SET session_replication_role = replica;",
        "INSERT INTO public.country (geonameid, iso_code, geom, created_at, updated_at) "
        f"VALUES ({COUNTRY_ID}, NULL, NULL, now(), now()) ON CONFLICT (geonameid) DO NOTHING;",
        "INSERT INTO public.license (id, name, url) "
        f"SELECT {next_license_id}, {sql_literal(LICENSE_NAME)}, {sql_literal(LICENSE_URL)} "
        "WHERE NOT EXISTS (SELECT 1 FROM public.license WHERE name = "
        f"{sql_literal(LICENSE_NAME)} OR url = {sql_literal(LICENSE_URL)});",
        "SELECT setval('license_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.license), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ]
    exec_sql("\n".join(statements) + "\n")


def ensure_owner_user_id() -> int:
    value = query_value("SELECT id FROM \"user\" WHERE username='IPAD_admin' ORDER BY id LIMIT 1;")
    if not value:
        raise RuntimeError("IPAD_admin user not found in arkeopen")
    return int(value)


def ensure_collection_id() -> int:
    value = query_value(f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;")
    if value:
        return int(value)
    exec_sql(f"INSERT INTO database_collection(name) VALUES ({sql_literal(COLLECTION_NAME)});\n")
    value = query_value(f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;")
    return int(value)


def get_license_id() -> int:
    value = query_value(
        f"SELECT id FROM public.license WHERE name = {sql_literal(LICENSE_NAME)} OR url = {sql_literal(LICENSE_URL)} ORDER BY id LIMIT 1;"
    )
    if not value:
        raise RuntimeError("Required license row not found after ensure_support_rows()")
    return int(value)


def get_dataset_id(name: str) -> int:
    value = query_value(f"SELECT id FROM public.database WHERE name = {sql_literal(name)} LIMIT 1;")
    if value:
        return int(value)
    return int(query_value("SELECT COALESCE(MAX(id), 0) + 1 FROM public.database;"))


def build_dataset_sql(
    dataset: dict[str, object],
    dataset_id: int,
    owner_user_id: int,
    collection_id: int,
    license_id: int,
) -> str:
    prefix = f"ipad-{dataset['slug']}-%"
    bbox_min_lon, bbox_min_lat, bbox_max_lon, bbox_max_lat = dataset["bbox"]
    bbox_wkt = (
        f"POLYGON(({bbox_min_lon} {bbox_min_lat},{bbox_min_lon} {bbox_max_lat},"
        f"{bbox_max_lon} {bbox_max_lat},{bbox_max_lon} {bbox_min_lat},{bbox_min_lon} {bbox_min_lat}))"
    )
    next_range_id = int(query_value("SELECT COALESCE(MAX(id), 0) + 1 FROM public.site_range;"))
    next_src_id = int(query_value("SELECT COALESCE(MAX(id), 0) + 1 FROM public.site_range__charac;"))

    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
        f"DELETE FROM public.database__authors WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database__country WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_context WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_tr WHERE database_id = {dataset_id};",
        "DELETE FROM public.site_range__charac_tr "
        "WHERE site_range__charac_id IN ("
        "SELECT src.id FROM public.site_range__charac src "
        "JOIN public.site_range sr ON sr.id = src.site_range_id "
        "JOIN public.site s ON s.id = sr.site_id "
        f"WHERE s.database_id = {dataset_id} OR s.id LIKE {sql_literal(prefix)});",
        "DELETE FROM public.site_range__charac "
        "WHERE site_range_id IN ("
        "SELECT sr.id FROM public.site_range sr "
        "JOIN public.site s ON s.id = sr.site_id "
        f"WHERE s.database_id = {dataset_id} OR s.id LIKE {sql_literal(prefix)});",
        "DELETE FROM public.site_range "
        "WHERE site_id IN ("
        "SELECT id FROM public.site "
        f"WHERE database_id = {dataset_id} OR id LIKE {sql_literal(prefix)});",
        "DELETE FROM public.site_tr "
        "WHERE site_id IN ("
        "SELECT id FROM public.site "
        f"WHERE database_id = {dataset_id} OR id LIKE {sql_literal(prefix)});",
        f"DELETE FROM public.site WHERE database_id = {dataset_id} OR id LIKE {sql_literal(prefix)};",
        "INSERT INTO public.database "
        "(id, name, scale_resolution, geographical_extent, type, owner, editor, editor_url, contributor, default_language, state, "
        "license_id, published, soft_deleted, geographical_extent_geom, start_date, end_date, declared_creation_date, public, created_at, updated_at, root_chronology_id, illustrations, database_collection_id) "
        f"VALUES ({dataset_id}, {sql_literal(dataset['name'])}, 'region', 'region', 'inventory', {owner_user_id}, "
        f"{sql_literal('IPAD import')}, {sql_literal('')}, {sql_literal('IPAD')}, 'en', 'finished', "
        f"{license_id}, true, false, ST_GeogFromText({sql_literal(bbox_wkt)}), {dataset['start_date']}, {dataset['end_date']}, "
        f"{sql_literal('2026-05-15T00:00:00+00:00')}, true, now(), now(), {ROOT_CHRONOLOGY_ID}, '', {collection_id}) "
        "ON CONFLICT (id) DO UPDATE SET "
        "name = EXCLUDED.name, scale_resolution = EXCLUDED.scale_resolution, geographical_extent = EXCLUDED.geographical_extent, "
        "type = EXCLUDED.type, owner = EXCLUDED.owner, editor = EXCLUDED.editor, editor_url = EXCLUDED.editor_url, contributor = EXCLUDED.contributor, "
        "default_language = EXCLUDED.default_language, state = EXCLUDED.state, license_id = EXCLUDED.license_id, published = EXCLUDED.published, "
        "soft_deleted = EXCLUDED.soft_deleted, geographical_extent_geom = EXCLUDED.geographical_extent_geom, start_date = EXCLUDED.start_date, "
        "end_date = EXCLUDED.end_date, declared_creation_date = EXCLUDED.declared_creation_date, public = EXCLUDED.public, updated_at = now(), "
        "root_chronology_id = EXCLUDED.root_chronology_id, illustrations = EXCLUDED.illustrations, database_collection_id = EXCLUDED.database_collection_id;",
        "INSERT INTO public.database_tr "
        "(database_id, lang_isocode, description, geographical_limit, bibliography, context_description, source_description, source_relation, copyright, subject, re_use) "
        f"VALUES ({dataset_id}, 'en', {sql_literal(dataset['description'])}, {sql_literal(dataset['geographical_limit'])}, {sql_literal(dataset['bibliography'])}, "
        f"{sql_literal(dataset['context_description'])}, {sql_literal(dataset['source_description'])}, {sql_literal(dataset['source_relation'])}, "
        f"{sql_literal(dataset['copyright'])}, {sql_literal(dataset['subject'])}, {sql_literal(dataset['reuse'])}) "
        "ON CONFLICT (database_id, lang_isocode) DO UPDATE SET "
        "description = EXCLUDED.description, geographical_limit = EXCLUDED.geographical_limit, bibliography = EXCLUDED.bibliography, "
        "context_description = EXCLUDED.context_description, source_description = EXCLUDED.source_description, source_relation = EXCLUDED.source_relation, "
        "copyright = EXCLUDED.copyright, subject = EXCLUDED.subject, re_use = EXCLUDED.re_use;",
        f"INSERT INTO public.database__authors (database_id, user_id) VALUES ({dataset_id}, {owner_user_id}) ON CONFLICT (database_id, user_id) DO NOTHING;",
        f"INSERT INTO public.database_context (id, database_id, context) VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM public.database_context), {dataset_id}, 'academic-work');",
        f"INSERT INTO public.database__country (database_id, country_geonameid) VALUES ({dataset_id}, {COUNTRY_ID}) ON CONFLICT (database_id, country_geonameid) DO NOTHING;",
    ]

    for city in dataset["cities"]:
        statements.extend([
            "INSERT INTO public.city (geonameid, country_geonameid, geom, geom_centroid, created_at, updated_at) "
            f"VALUES ({city['geonameid']}, {COUNTRY_ID}, NULL, ST_GeogFromText('POINT({city['longitude']} {city['latitude']})'), now(), now()) "
            "ON CONFLICT (geonameid) DO UPDATE SET "
            "country_geonameid = EXCLUDED.country_geonameid, geom_centroid = EXCLUDED.geom_centroid, updated_at = now();",
            "INSERT INTO public.city_tr (city_geonameid, lang_isocode, name, name_ascii) "
            f"VALUES ({city['geonameid']}, 'en', {sql_literal(city['name'])}, {sql_literal(city['name_ascii'])}) "
            "ON CONFLICT (city_geonameid, lang_isocode) DO UPDATE SET name = EXCLUDED.name, name_ascii = EXCLUDED.name_ascii;",
        ])

    for site in dataset["sites"]:
        statements.extend([
            "INSERT INTO public.site "
            "(id, code, name, city_name, city_geonameid, geom, geom_3d, centroid, occupation, database_id, created_at, updated_at, altitude, start_date1, start_date2, end_date1, end_date2) "
            f"VALUES ({sql_literal(site['id'])}, {sql_literal(site['code'])}, {sql_literal(site['name'])}, {sql_literal(site['locality'])}, "
            f"{site['city_geonameid']}, ST_GeogFromText('POINT({site['longitude']} {site['latitude']})'), "
            f"ST_Force3DZ(ST_GeomFromText('POINT({site['longitude']} {site['latitude']})', 4326), {site['altitude']})::geography, "
            f"{'true' if site['centroid'] else 'false'}, '{site['occupation']}', {dataset_id}, now(), now(), {site['altitude']}, "
            f"{site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}) "
            "ON CONFLICT (id) DO UPDATE SET "
            "code = EXCLUDED.code, name = EXCLUDED.name, city_name = EXCLUDED.city_name, city_geonameid = EXCLUDED.city_geonameid, "
            "geom = EXCLUDED.geom, geom_3d = EXCLUDED.geom_3d, centroid = EXCLUDED.centroid, occupation = EXCLUDED.occupation, database_id = EXCLUDED.database_id, "
            "updated_at = now(), altitude = EXCLUDED.altitude, start_date1 = EXCLUDED.start_date1, start_date2 = EXCLUDED.start_date2, "
            "end_date1 = EXCLUDED.end_date1, end_date2 = EXCLUDED.end_date2;",
            f"INSERT INTO public.site_tr (site_id, lang_isocode, description) VALUES ({sql_literal(site['id'])}, 'en', {sql_literal(site['description'])}) "
            "ON CONFLICT (site_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description;",
        ])

        for range_record in site["ranges"]:
            statements.extend([
                "INSERT INTO public.site_range (id, site_id, start_date1, start_date2, end_date1, end_date2, created_at, updated_at) "
                f"VALUES ({next_range_id}, {sql_literal(site['id'])}, {range_record['start_date1']}, {range_record['start_date2']}, "
                f"{range_record['end_date1']}, {range_record['end_date2']}, now(), now());",
                "INSERT INTO public.site_range__charac (id, site_range_id, charac_id, exceptional, knowledge_type, web_images) "
                f"VALUES ({next_src_id}, {next_range_id}, {range_record['charac_id']}, {'true' if range_record['exceptional'] else 'false'}, "
                f"'{range_record['knowledge_type']}', '');",
                "INSERT INTO public.site_range__charac_tr (site_range__charac_id, lang_isocode, comment, bibliography) "
                f"VALUES ({next_src_id}, 'en', {sql_literal(range_record['comment'])}, {sql_literal(range_record['bibliography'])});",
            ])
            next_range_id += 1
            next_src_id += 1

    statements.extend([
        "SELECT setval('database_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.database), 1), true);",
        "SELECT setval('database_context_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.database_context), 1), true);",
        "SELECT setval('site_range_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range), 1), true);",
        "SELECT setval('site_range__charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range__charac), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ])
    return "\n".join(statements) + "\n"


def verify_dataset(dataset_name: str) -> tuple[int, int]:
    sql = (
        "SELECT d.id, COUNT(s.id) "
        "FROM public.database d "
        "LEFT JOIN public.site s ON s.database_id = d.id "
        f"WHERE d.name = {sql_literal(dataset_name)} "
        "GROUP BY d.id;"
    )
    value = query_value(sql)
    if not value:
        raise RuntimeError(f"Dataset {dataset_name!r} was not found after import")
    dataset_id_text, site_count_text = value.split("|", 1)
    return int(dataset_id_text), int(site_count_text)


def print_summary(stats: Counter, files: list[pathlib.Path]) -> None:
    print("")
    print("IPAD regional import summary")
    print(f"  files imported: {len(files)}")
    print(f"  datasets updated: {stats['datasets_selected']}")
    print(f"  source rows: {stats['source_rows']}")
    print(f"  sites built: {stats['sites_built']}")
    print(f"  dedupe groups: {stats['dedupe_groups']}")
    print(f"  sites merged away: {stats['dedupe_merged_sites']}")
    print(f"  sites after dedupe: {stats['sites_after_dedupe']}")
    print(f"  charac truncation rows: {stats['charac_truncation_rows']}")
    print(f"  coordinate override sites: {stats['coordinate_override_sites']}")
    print(f"  coordinate fallback sites (same file): {stats['coordinate_fallback_same_file_sites']}")
    print(f"  coordinate fallback sites (global locality): {stats['coordinate_fallback_global_sites']}")
    print(f"  coordinate fallback sites (POINT 0 0): {stats['coordinate_fallback_zero_sites']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Import regional IPAD CSVs into local arkeopen")
    parser.add_argument("--dry-run", action="store_true", help="Parse and normalize without writing to the database")
    args = parser.parse_args()

    files = collect_source_files()
    if not files:
        raise RuntimeError("No matching IPAD CSV files were found")

    run_stats: Counter = Counter()
    run_stats["datasets_selected"] = 0
    run_stats["source_rows"] = 0
    run_stats["sites_built"] = 0
    run_stats["dedupe_groups"] = 0
    run_stats["dedupe_merged_sites"] = 0
    run_stats["sites_after_dedupe"] = 0
    run_stats["charac_truncation_rows"] = 0
    run_stats["coordinate_override_sites"] = 0
    run_stats["coordinate_fallback_same_file_sites"] = 0
    run_stats["coordinate_fallback_global_sites"] = 0
    run_stats["coordinate_fallback_zero_sites"] = 0

    ensure_support_rows()
    owner_user_id = ensure_owner_user_id()
    collection_id = ensure_collection_id()
    license_id = get_license_id()
    charac_paths = load_charac_paths()

    file_rows: OrderedDict[pathlib.Path, list[dict[str, str]]] = OrderedDict()
    all_rows: list[dict[str, str]] = []
    for path in files:
        rows = load_rows(path)
        file_rows[path] = rows
        all_rows.extend(rows)

    global_centroids = build_locality_centroids(all_rows)
    datasets = []
    for file_index, path in enumerate(files, start=1):
        dataset = build_dataset(path, file_index, file_rows[path], charac_paths, global_centroids, run_stats)
        datasets.append(dataset)

    merge_duplicate_sites(datasets, run_stats)

    if len(files) != 40:
        raise RuntimeError(f"Expected 40 regional files, found {len(files)}")
    if run_stats["source_rows"] != 1011:
        raise RuntimeError(f"Expected 1011 source rows, found {run_stats['source_rows']}")

    if args.dry_run:
        print("Dry run only; no database changes were made.")
        print_summary(run_stats, files)
        return 0

    for dataset in datasets:
        dataset_id = get_dataset_id(dataset["name"])
        sql = build_dataset_sql(dataset, dataset_id, owner_user_id, collection_id, license_id)
        exec_sql(sql)
        verified_dataset_id, verified_site_count = verify_dataset(dataset["name"])
        print(f"[imported] {dataset['name']} -> dataset_id={verified_dataset_id} sites={verified_site_count}")

    print_summary(run_stats, files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
