#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
import re
from collections import Counter, defaultdict


BASE_DIR = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform")
INPUT_CSV = BASE_DIR / "data" / "Imported table-Grid view.csv"
OUTPUT_CSV = BASE_DIR / "data" / "airtable-to-arkeogis-first-pass.csv"
REPORT_TXT = BASE_DIR / "data" / "airtable-to-arkeogis-first-pass-report.txt"

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
}

# Exact ArkeoGIS/ArkeOpen English thesaurus labels found in the local DB.
TYPE_PATHS = {
    "Settlement": ("Realestate", "Settlement", "", "", ""),
    "Jar": ("Furniture", "Ceramic", "Cockery", "Jar", ""),
    "Artefact": ("Furniture", "Others", "", "", ""),
    "Cave": ("Realestate", "Unknown", "", "", ""),
    "cliff": ("Realestate", "Unknown", "", "", ""),
    "rockshelter": ("Realestate", "Unknown", "", "", ""),
    "Excavation": ("Realestate", "Unknown", "", "", ""),
    "megalithic": ("Realestate", "Ritual", "", "", ""),
    "Boulder": ("Realestate", "Unknown", "", "", ""),
}

SUBTYPE_PATHS = {
    "Burial place": ("Realestate", "Funerary", "Undocumented", "Undocumented", "Burial"),
    "rock art": ("Realestate", "Ritual", "", "", ""),
    "Cave Art": ("Realestate", "Ritual", "", "", ""),
    "Sarcophagus": ("Furniture", "Stone", "Others", "Sarcophagus", ""),
    "Sarcophagus,Stone Mortar": ("Furniture", "Stone", "Others", "Sarcophagus", ""),
    "Lithic": ("Furniture", "Stone", "", "", ""),
    "Shards": ("Furniture", "Ceramic", "", "", ""),
    "Iron Smelting": ("Production", "", "", "", ""),
    "Iron Smelting,Charcoal": ("Production", "", "", "", ""),
    "Settlement,Iron Smelting": ("Realestate", "Settlement", "", "", ""),
    "Statue,Shards": ("Furniture", "Others", "", "", ""),
    "Urn,Sarcophagus": ("Furniture", "Ceramic", "Cockery", "Urn", ""),
    "Pottery production,Lithic": ("Production", "", "", "", ""),
    "Urn": ("Furniture", "Ceramic", "Cockery", "Urn", ""),
    "stone seat": ("Realestate", "Ritual", "", "", ""),
    "menhir,monolith": ("Realestate", "Ritual", "", "", ""),
    "rock art,Burial place": ("Realestate", "Funerary", "Undocumented", "Undocumented", "Burial"),
    "Lithic,Burial place": ("Realestate", "Funerary", "Undocumented", "Undocumented", "Burial"),
    "Burial place,Ceramics": ("Realestate", "Funerary", "Undocumented", "Undocumented", "Burial"),
}


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
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-_")
    return sanitized or ""


def normalize_text(value: str) -> str:
    text = (value or "").replace(";", ",").replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def build_localisation(row: dict[str, str]) -> str:
    pieces = [
        normalize_text(row.get("Administrative_region", "")),
        normalize_text(row.get("Address", "")),
        normalize_text(row.get("Archaeo_complex", "")),
    ]
    deduped = []
    seen = set()
    for piece in pieces:
        if piece and piece not in seen:
            deduped.append(piece)
            seen.add(piece)
    return " | ".join(deduped)


def chronology_range(value: str, unresolved: Counter[str]) -> tuple[str, str]:
    raw = normalize_text(value)
    if not raw:
        return "Undefined", "Undefined"

    parts = [part.strip() for part in raw.split(",") if part.strip()]
    starts = []
    ends = []
    for part in parts:
        mapped = CHRONOLOGY_MAP.get(part)
        if mapped is None:
            unresolved[part] += 1
            continue
        starts.append(mapped[0])
        ends.append(mapped[1])

    if not starts or not ends:
        return "Undefined", "Undefined"
    return str(min(starts)), str(max(ends))


def charac_path(row: dict[str, str], unresolved: Counter[str]) -> tuple[str, str, str, str, str]:
    subtype = normalize_text(row.get("Subtype", ""))
    type_name = normalize_text(row.get("Type", ""))

    if subtype in SUBTYPE_PATHS:
        return SUBTYPE_PATHS[subtype]
    if type_name in TYPE_PATHS:
        return TYPE_PATHS[type_name]

    if subtype:
        unresolved[f"Subtype: {subtype}"] += 1
    if type_name:
        unresolved[f"Type: {type_name}"] += 1

    return ("Realestate", "Unknown", "", "", "")


def state_of_knowledge(row: dict[str, str]) -> str:
    remains = normalize_text(row.get("Remains", "")).lower()
    if remains == "checked":
        return "Foot survey"
    return "Not documented"


def build_comments(row: dict[str, str]) -> str:
    parts = []
    for label in ["Description", "Keywords", "Sub-cultural Period", "Alternative_Name"]:
        value = normalize_text(row.get(label, ""))
        if value:
            parts.append(f"{label}: {value}")
    return " | ".join(parts)


def build_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, Counter[str]]]:
    output_rows = []
    unresolved = {
        "chronology": Counter(),
        "charac": Counter(),
    }

    for row in rows:
        source_id = sanitize_source_id(row)
        if not source_id:
            continue

        lon, lat = parse_point_wkt(row.get("Coord_WKT", ""))
        start_period, end_period = chronology_range(row.get("Chronology", ""), unresolved["chronology"])
        main_charac, lvl1, lvl2, lvl3, lvl4 = charac_path(row, unresolved["charac"])

        output_rows.append(
            {
                "SITE_SOURCE_ID": source_id,
                "SITE_NAME": normalize_text(row.get("Site_Name", "")) or normalize_text(row.get("Alternative_Name", "")),
                "LOCALISATION": build_localisation(row),
                "GEONAME_ID": "",
                "PROJECTION_SYSTEM": "4326" if lon and lat else "",
                "LONGITUDE": lon,
                "LATITUDE": lat,
                "ALTITUDE": "",
                "CITY_CENTROID": "No" if lon and lat else "Yes",
                "STATE_OF_KNOWLEDGE": state_of_knowledge(row),
                "OCCUPATION": "Not specified",
                "STARTING_PERIOD": start_period,
                "ENDING_PERIOD": end_period,
                "MAIN_CHARAC": main_charac,
                "CHARAC_LVL1": lvl1,
                "CHARAC_LVL2": lvl2,
                "CHARAC_LVL3": lvl3,
                "CHARAC_LVL4": lvl4,
                "CHARAC_EXP": "No",
                "BIBLIOGRAPHY": normalize_text(row.get("Biblio_ref", "")),
                "COMMENTS": build_comments(row),
                "WEB_IMAGES": "",
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
    chronology_counter = Counter((row["STARTING_PERIOD"], row["ENDING_PERIOD"]) for row in rows)

    lines = [
        "Airtable -> ArkeoGIS first-pass conversion report",
        "",
        f"Input file: {INPUT_CSV}",
        f"Output file: {OUTPUT_CSV}",
        f"Rows written: {len(rows)}",
        "",
        "MAIN_CHARAC counts:",
    ]
    for key, count in sorted(type_counter.items()):
        lines.append(f"- {key}: {count}")

    lines.extend(["", "Chronology ranges used:"])
    for (start, end), count in chronology_counter.most_common():
        lines.append(f"- {start} -> {end}: {count}")

    lines.extend(["", "Unresolved chronology labels:"])
    if unresolved["chronology"]:
        for value, count in unresolved["chronology"].most_common():
            lines.append(f"- {value}: {count}")
    else:
        lines.append("- none")

    lines.extend(["", "Fallback/unresolved characterisation values:"])
    if unresolved["charac"]:
        for value, count in unresolved["charac"].most_common():
            lines.append(f"- {value}: {count}")
    else:
        lines.append("- none")

    REPORT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    output_rows, unresolved = build_rows(rows)
    write_csv(output_rows)
    write_report(output_rows, unresolved)

    print(f"Wrote {len(output_rows)} rows to {OUTPUT_CSV}")
    print(f"Wrote report to {REPORT_TXT}")


if __name__ == "__main__":
    main()
