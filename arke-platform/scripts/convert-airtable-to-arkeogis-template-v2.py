#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
import re
from collections import Counter


BASE_DIR = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform")
INPUT_CSV = BASE_DIR / "data" / "Imported table-Grid view.csv"
OUTPUT_CSV = BASE_DIR / "data" / "airtable-to-arkeogis-v2.csv"
REPORT_TXT = BASE_DIR / "data" / "airtable-to-arkeogis-v2-report.txt"

OUTPUT_HEADERS = [
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

CHRONOLOGY_MAP = {
    "Pleistocene": (-2580000, -9700),
    "Holocene": (-9700, 1950),
    "Palaeolithic": (-3300000, -10000),
    "Neolithic": (-10000, -2000),
    "Megalithic": (-4000, 1500),
    "EMP (Early Metal Phase": (-2500, 500),
    "Bronze Age": (-3300, -1200),
    "Iron Age": (-1200, 500),
    "Palaeometallic": (-2500, 500),
}

# Each mapping resolves to exact ArkeoGIS import labels where available.
TOKEN_PATHS = {
    "burial place": ("Realestate", "Funerary", "Undocumented", "Undocumented", "Burial"),
    "cave art": ("Realestate", "Ritual", "", "", ""),
    "rock art": ("Realestate", "Ritual", "", "", ""),
    "sarcophagus": ("Furniture", "Stone", "Others", "Sarcophagus", ""),
    "stone mortar": ("Furniture", "Stone", "", "", ""),
    "lithic": ("Furniture", "Stone", "", "", ""),
    "shards": ("Furniture", "Ceramic", "", "", ""),
    "statue": ("Furniture", "Others", "", "", ""),
    "urn": ("Furniture", "Ceramic", "Cockery", "Urn", ""),
    "iron smelting": ("Production", "", "", "", ""),
    "charcoal": ("Production", "", "", "", ""),
    "settlement": ("Realestate", "Settlement", "", "", ""),
    "stone seat": ("Realestate", "Ritual", "", "", ""),
    "menhir": ("Realestate", "Ritual", "", "", ""),
    "monolith": ("Realestate", "Ritual", "", "", ""),
    "jar": ("Furniture", "Ceramic", "Cockery", "Jar", ""),
    "artefact": ("Furniture", "Others", "", "", ""),
    "artifact": ("Furniture", "Others", "", "", ""),
    "cave": ("Realestate", "Unknown", "", "", ""),
    "rockshelter": ("Realestate", "Unknown", "", "", ""),
    "rock shelter": ("Realestate", "Unknown", "", "", ""),
    "cliff": ("Realestate", "Unknown", "", "", ""),
    "excavation": ("Realestate", "Unknown", "", "", ""),
    "megalithic": ("Realestate", "Ritual", "", "", ""),
    "boulder": ("Realestate", "Unknown", "", "", ""),
}


def normalize_text(value: str) -> str:
    text = (value or "").replace(";", ",").replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def parse_point_wkt(value: str) -> tuple[str, str]:
    text = (value or "").strip()
    match = re.match(r"POINT\s*\(\s*([^\s]+)\s+([^\s]+)\s*\)", text, re.IGNORECASE)
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def sanitize_source_id(row: dict[str, str]) -> str:
    candidate = (row.get("UUID") or row.get("ID") or row.get("Site_Name") or "").strip()
    if not candidate:
        return ""
    return re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-_")


def build_localisation(row: dict[str, str]) -> str:
    values = []
    seen = set()
    for key in ["Administrative_region", "Address", "Archaeo_complex"]:
        value = normalize_text(row.get(key, ""))
        if value and value not in seen:
            values.append(value)
            seen.add(value)
    return " | ".join(values)


def split_tokens(value: str) -> list[str]:
    raw = normalize_text(value)
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def chronology_range(row: dict[str, str], unresolved: Counter[str]) -> tuple[str, str]:
    tokens = split_tokens(row.get("Chronology", "")) + split_tokens(row.get("Sub-cultural Period", ""))
    if not tokens:
        return "Undefined", "Undefined"

    starts = []
    ends = []
    for token in tokens:
        mapped = CHRONOLOGY_MAP.get(token)
        if mapped is None:
            unresolved[token] += 1
            continue
        starts.append(mapped[0])
        ends.append(mapped[1])

    if not starts:
        return "Undefined", "Undefined"
    return str(min(starts)), str(max(ends))


def resolve_path(tokens: list[str], unresolved: Counter[str]) -> tuple[str, str, str, str, str]:
    for token in tokens:
        lowered = token.lower()
        if lowered in TOKEN_PATHS:
            return TOKEN_PATHS[lowered]
    for token in tokens:
        unresolved[token] += 1
    return ("Realestate", "Unknown", "", "", "")


def row_paths(row: dict[str, str], unresolved: Counter[str]) -> list[tuple[str, str, str, str, str]]:
    subtype_tokens = split_tokens(row.get("Subtype", ""))
    type_tokens = split_tokens(row.get("Type", ""))

    paths = []
    if subtype_tokens:
        for token in subtype_tokens:
            path = resolve_path([token], unresolved)
            if path not in paths:
                paths.append(path)

    if not paths and type_tokens:
        for token in type_tokens:
            path = resolve_path([token], unresolved)
            if path not in paths:
                paths.append(path)

    if not paths and type_tokens:
        paths.append(resolve_path(type_tokens, unresolved))

    if not paths:
        paths.append(("Realestate", "Unknown", "", "", ""))
    return paths


def state_of_knowledge(row: dict[str, str]) -> str:
    if normalize_text(row.get("Remains", "")).lower() == "checked":
        return "Foot survey"
    return "Not documented"


def build_comments(row: dict[str, str]) -> str:
    parts = []
    for label in ["Description", "Keywords", "Sub-cultural Period", "Alternative_Name"]:
        value = normalize_text(row.get(label, ""))
        if value:
            parts.append(f"{label}: {value}")
    return " | ".join(parts)


def build_output_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, Counter[str]]]:
    unresolved = {"chronology": Counter(), "charac": Counter()}
    output_rows = []

    for row in rows:
        source_id = sanitize_source_id(row)
        if not source_id:
            continue

        longitude, latitude = parse_point_wkt(row.get("Coord_WKT", ""))
        start_period, end_period = chronology_range(row, unresolved["chronology"])

        base = {
            "SITE_SOURCE_ID": source_id,
            "SITE_NAME": normalize_text(row.get("Site_Name", "")) or normalize_text(row.get("Alternative_Name", "")),
            "LOCALISATION": build_localisation(row),
            "GEONAME_ID": "",
            "PROJECTION_SYSTEM": "4326" if longitude and latitude else "",
            "LONGITUDE": longitude,
            "LATITUDE": latitude,
            "ALTITUDE": "",
            "CITY_CENTROID": "No" if longitude and latitude else "Yes",
            "STATE_OF_KNOWLEDGE": state_of_knowledge(row),
            "OCCUPATION": "Not specified",
            "STARTING_PERIOD": start_period,
            "ENDING_PERIOD": end_period,
            "CHARAC_EXP": "No",
            "BIBLIOGRAPHY": normalize_text(row.get("Biblio_ref", "")),
            "COMMENTS": build_comments(row),
            "WEB_IMAGES": "",
        }

        for main_charac, lvl1, lvl2, lvl3, lvl4 in row_paths(row, unresolved["charac"]):
            output_rows.append(
                {
                    **base,
                    "MAIN_CHARAC": main_charac,
                    "CHARAC_LVL1": lvl1,
                    "CHARAC_LVL2": lvl2,
                    "CHARAC_LVL3": lvl3,
                    "CHARAC_LVL4": lvl4,
                }
            )

    return output_rows, unresolved


def write_csv(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def write_report(rows: list[dict[str, str]], unresolved: dict[str, Counter[str]]) -> None:
    type_counter = Counter(row["MAIN_CHARAC"] for row in rows)
    lines_per_site = Counter(row["SITE_SOURCE_ID"] for row in rows)

    lines = [
        "Airtable -> ArkeoGIS v2 conversion report",
        "",
        f"Input file: {INPUT_CSV}",
        f"Output file: {OUTPUT_CSV}",
        f"Rows written: {len(rows)}",
        f"Distinct sites: {len(lines_per_site)}",
        f"Sites expanded to multiple rows: {sum(1 for count in lines_per_site.values() if count > 1)}",
        "",
        "MAIN_CHARAC counts:",
    ]
    for key, count in sorted(type_counter.items()):
        lines.append(f"- {key}: {count}")

    lines.extend(["", "Unresolved chronology labels:"])
    if unresolved["chronology"]:
        for key, count in unresolved["chronology"].most_common():
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")

    lines.extend(["", "Fallback/unresolved characterisation tokens:"])
    if unresolved["charac"]:
        for key, count in unresolved["charac"].most_common():
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")

    REPORT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    output_rows, unresolved = build_output_rows(rows)
    write_csv(output_rows)
    write_report(output_rows, unresolved)
    print(f"Wrote {len(output_rows)} rows to {OUTPUT_CSV}")
    print(f"Wrote report to {REPORT_TXT}")


if __name__ == "__main__":
    main()
