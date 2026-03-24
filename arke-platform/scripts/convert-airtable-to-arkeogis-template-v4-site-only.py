#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
import re
from collections import Counter


BASE_DIR = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform")
INPUT_CSV = BASE_DIR / "data" / "Imported table-Grid view.csv"
OUTPUT_CSV = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only.csv"
REPORT_TXT = BASE_DIR / "data" / "airtable-to-arkeogis-v4-site-only-report.txt"

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
    "Pleistocene": (-2578050, -9700),
    "Holocene": (-9700, 1950),
    "Palaeolithic": (-2578050, -10000),
    "Neolithic": (-10000, -2000),
    "Megalithic": (-4000, 1500),
    "EMP (Early Metal Phase": (-2500, 500),
    "Bronze Age": (-3300, -1200),
    "Iron Age": (-1200, 500),
    "Palaeometallic": (-2500, 500),
}

SITE_TYPE_PATHS = {
    "cave": ("Realestate", "Unknown", "", "", ""),
    "rockshelter": ("Realestate", "Unknown", "", "", ""),
    "rock shelter": ("Realestate", "Unknown", "", "", ""),
    "cliff": ("Realestate", "Unknown", "", "", ""),
    "excavation": ("Realestate", "Unknown", "", "", ""),
    "megalithic": ("Realestate", "Ritual", "", "", ""),
    "settlement": ("Realestate", "Settlement", "", "", ""),
    "boulder": ("Realestate", "Unknown", "", "", ""),
}

SITE_SUBTYPE_PATHS = {
    "burial place": ("Realestate", "Funerary", "Undocumented", "Undocumented", "Burial"),
    "cave art": ("Realestate", "Ritual", "", "", ""),
    "rock art": ("Realestate", "Ritual", "", "", ""),
    "settlement": ("Realestate", "Settlement", "", "", ""),
    "iron smelting": ("Production", "", "", "", ""),
    "charcoal": ("Production", "", "", "", ""),
    "settlement,iron smelting": ("Realestate", "Settlement", "", "", ""),
    "stone seat": ("Realestate", "Ritual", "", "", ""),
    "menhir,monolith": ("Realestate", "Ritual", "", "", ""),
}

OBJECT_LIKE_TOKENS = {
    "sarcophagus",
    "stone mortar",
    "lithic",
    "shards",
    "statue",
    "urn",
    "pottery production",
    "ceramics",
    "jar",
    "artefact",
    "artifact",
}

ART_RE = re.compile(r"rock art|cave art|painting|paintings|pictograph|image|images|hand stencil|stencil|artwork|artworks", re.I)
SETTLEMENT_RE = re.compile(r"settlement|habitation|occupation sequence|occupied|village", re.I)


def normalize_text(value: str) -> str:
    text = (value or "").replace(";", ",").replace("\r", " ").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def split_tokens(value: str) -> list[str]:
    raw = normalize_text(value)
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


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


def classify_row(row: dict[str, str], unresolved: Counter[str], excluded: Counter[str], heuristic: Counter[str]) -> tuple[str, str, str, str, str]:
    subtype_raw = normalize_text(row.get("Subtype", ""))
    type_raw = normalize_text(row.get("Type", ""))
    text_blob = " ".join(normalize_text(row.get(key, "")) for key in ["Subtype", "Keywords", "Description", "Site_Name"])

    if subtype_raw:
        lowered_subtype = subtype_raw.lower()
        if lowered_subtype in SITE_SUBTYPE_PATHS:
            return SITE_SUBTYPE_PATHS[lowered_subtype]

        subtype_tokens = split_tokens(subtype_raw)
        site_tokens = [token for token in subtype_tokens if token.lower() not in OBJECT_LIKE_TOKENS]
        object_tokens = [token for token in subtype_tokens if token.lower() in OBJECT_LIKE_TOKENS]

        for token in object_tokens:
            excluded[token] += 1

        joined_site_tokens = ",".join(site_tokens).lower()
        if joined_site_tokens in SITE_SUBTYPE_PATHS:
            return SITE_SUBTYPE_PATHS[joined_site_tokens]

        for token in site_tokens:
            if token.lower() in SITE_SUBTYPE_PATHS:
                return SITE_SUBTYPE_PATHS[token.lower()]

    lowered_type = type_raw.lower()
    if lowered_type:
        if lowered_type in OBJECT_LIKE_TOKENS:
            excluded[type_raw] += 1
        elif lowered_type in {"cave", "rockshelter", "rock shelter", "cliff"} and ART_RE.search(text_blob):
            heuristic[f"{type_raw}:art->ritual"] += 1
            return ("Realestate", "Ritual", "", "", "")
        elif lowered_type == "excavation" and SETTLEMENT_RE.search(text_blob):
            heuristic["Excavation:settlement-text->settlement"] += 1
            return ("Realestate", "Settlement", "", "", "")
        elif lowered_type in SITE_TYPE_PATHS:
            return SITE_TYPE_PATHS[lowered_type]
        else:
            unresolved[type_raw] += 1

    return ("Realestate", "Unknown", "", "", "")


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


def build_output_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], Counter[str], Counter[str], Counter[str], Counter[str]]:
    output_rows = []
    unresolved_chronology = Counter()
    unresolved_charac = Counter()
    excluded_tokens = Counter()
    heuristics = Counter()

    for row in rows:
        source_id = sanitize_source_id(row)
        if not source_id:
            continue

        longitude, latitude = parse_point_wkt(row.get("Coord_WKT", ""))
        start_period, end_period = chronology_range(row, unresolved_chronology)
        main_charac, lvl1, lvl2, lvl3, lvl4 = classify_row(row, unresolved_charac, excluded_tokens, heuristics)

        output_rows.append(
            {
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

    return output_rows, unresolved_chronology, unresolved_charac, excluded_tokens, heuristics


def write_csv(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def write_report(rows: list[dict[str, str]], unresolved_chronology: Counter[str], unresolved_charac: Counter[str], excluded_tokens: Counter[str], heuristics: Counter[str]) -> None:
    type_counter = Counter(row["MAIN_CHARAC"] for row in rows)
    level1_counter = Counter((row["MAIN_CHARAC"], row["CHARAC_LVL1"]) for row in rows)
    lines = [
        "Airtable -> ArkeoGIS v4 site-only conversion report",
        "",
        f"Input file: {INPUT_CSV}",
        f"Output file: {OUTPUT_CSV}",
        f"Rows written: {len(rows)}",
        "",
        "MAIN_CHARAC counts:",
    ]
    for key, count in sorted(type_counter.items()):
        lines.append(f"- {key}: {count}")
    lines.extend(["", "MAIN_CHARAC / CHARAC_LVL1 counts:"])
    for (main_charac, lvl1), count in sorted(level1_counter.items()):
        lines.append(f"- {main_charac} > {lvl1 or '(blank)'}: {count}")
    lines.extend(["", "Applied heuristics:"])
    if heuristics:
        for key, count in heuristics.most_common():
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")
    lines.extend(["", "Excluded object/find-like subtype tokens:"])
    if excluded_tokens:
        for key, count in excluded_tokens.most_common():
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")
    lines.extend(["", "Unresolved chronology labels:"])
    if unresolved_chronology:
        for key, count in unresolved_chronology.most_common():
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")
    lines.extend(["", "Unresolved site characterisation values:"])
    if unresolved_charac:
        for key, count in unresolved_charac.most_common():
            lines.append(f"- {key}: {count}")
    else:
        lines.append("- none")
    REPORT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    output_rows, unresolved_chronology, unresolved_charac, excluded_tokens, heuristics = build_output_rows(rows)
    write_csv(output_rows)
    write_report(output_rows, unresolved_chronology, unresolved_charac, excluded_tokens, heuristics)
    print(f"Wrote {len(output_rows)} rows to {OUTPUT_CSV}")
    print(f"Wrote report to {REPORT_TXT}")


if __name__ == "__main__":
    main()
