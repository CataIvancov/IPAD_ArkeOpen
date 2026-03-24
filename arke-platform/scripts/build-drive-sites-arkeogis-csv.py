#!/usr/bin/env python3

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
TEXT_ROOT = ROOT / "data" / "google-drive-text"
CANDIDATES_CSV = ROOT / "data" / "site-geolocation-candidates.csv"
EXISTING_SITES_CSV = ROOT / "data" / "airtable-to-arkeogis-v4-site-only.csv"
OUTPUT_CSV = ROOT / "data" / "drive-sites-to-arkeogis.csv"
OUTPUT_REPORT = ROOT / "data" / "drive-sites-to-arkeogis-report.txt"

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

DOI_REGEX = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)
LOCALITY_INFO_NAME_REGEX = re.compile(r"\bName:\s*(.+?)\s*(?:Synonyms:|Country:)", re.IGNORECASE | re.DOTALL)
LOCALITY_INFO_REGION_REGEX = re.compile(r"\bRegion:\s*(.+?)\s*(?:Coordinates:|Type:)", re.IGNORECASE | re.DOTALL)
LOCALITY_INFO_COUNTRY_REGEX = re.compile(r"\bCountry:\s*(.+?)\s*(?:Region:|Coordinates:|Type:)", re.IGNORECASE | re.DOTALL)
LOCALITY_INFO_COORDS_REGEX = re.compile(
    r"\bCoordinates:\s*([0-9.]+)\s*([NS])\s*,\s*([0-9.]+)\s*([EW])",
    re.IGNORECASE,
)
LOCALITY_INFO_TYPE_REGEX = re.compile(r"\bType:\s*(.+?)\s*(?:Summary:|Comments:|Assemblages:)", re.IGNORECASE | re.DOTALL)
LOCALITY_INFO_SUMMARY_REGEX = re.compile(r"\bSummary:\s*(.+?)\s*(?:Assemblages:|Age:|Comments:)", re.IGNORECASE | re.DOTALL)
LOCALITY_INFO_COMMENTS_REGEX = re.compile(r"\bComments:\s*(.+?)\s*(?:Assemblages:|Age:)", re.IGNORECASE | re.DOTALL)
MIN_AGE_REGEX = re.compile(r"\bMin Age\s*:?\s*([0-9,]{3,7})", re.IGNORECASE)
MAX_AGE_REGEX = re.compile(r"\bMax Age\s*:?\s*([0-9,]{3,7})", re.IGNORECASE)
DECIMAL_COORD_REGEX = re.compile(r"\bCoordinates:\s*([0-9.]+)\s*([NS])\s*,\s*([0-9.]+)\s*([EW])", re.IGNORECASE)
ADMIN_REGEX = re.compile(
    r"\b([A-Z][A-Za-z'’._-]+(?:\s+[A-Z][A-Za-z'’._-]+){0,4}\s+(?:Village|Regency|District|Province|Sub-district|Island|Islands|Karsts|karsts|coast|coastal area|River))\b"
)

GENERIC_BAD_STARTS = (
    "A ",
    "An ",
    "The ",
)
GENERIC_BAD_CONTAINS = {
    "Archaeological Site",
    "Archaeological Sites",
    "Archaeological Sites",
    "Prehistoric Site",
    "Prehistoric Sites",
    "Protohistoric Site",
    "Protohistoric Sites",
    "Historic Site",
    "Historic Sites",
    "Potential World Heritage Site",
    "Industrial Complex",
    "Heritage Site",
    "Village",
    "Regency",
    "No Sites",
    "Sites",
    "Rockshelter Sites",
    "Cultural Complex",
    "Archipelago Sites",
    "East Coast",
    "Meanwhile",
    "Astronomical",
    "Techniques",
    "Potential",
}
SITE_TOKEN_HINTS = (
    "Leang",
    "Gua",
    "Goa",
    "Liang",
    "Ceruk",
    "Cave",
    "Rockshelter",
    "Rock Shelter",
    "Shelter",
    "Site",
    "Complex",
    "Trinil",
    "Sangiran",
    "Semedo",
    "Topogaro",
    "Mata Menge",
    "Liang Bua",
    "Laili",
    "Mololo",
    "Golo",
    "Talepu",
    "Trinil",
    "Pedawa",
)

GENERIC_BAD_EXACT = {
    "No Sites",
    "Rockshelter Sites",
    "Matsu Archipelago Sites",
    "Lapita Cultural Complex",
    "Nos Techniques Sites",
}

BAD_WORDS = {
    "meanwhile",
    "sites",
    "archaeological",
    "prehistoric",
    "protohistoric",
    "historic",
    "techniques",
    "astronomical",
    "potential",
    "complex",
}

LOCALITY_SITE_TYPES = (
    "rock art",
    "shell midden",
    "lithic workshop",
    "workshop",
    "cave",
    "rockshelter",
    "rock shelter",
    "shelter",
    "river terrace",
    "river",
    "karst",
    "megalithic",
    "burial",
    "settlement",
    "occupation",
    "open site",
    "surface scatter",
    "excavation",
    "survey",
)

SITE_TYPE_MAPPINGS = [
    (("rock art", "hand stencil", "painting"), "Rock art site", ("Realestate", "Archaeological site", "Rock art site", "", "")),
    (("shell midden", "midden"), "Shell midden", ("Realestate", "Archaeological site", "Shell midden", "", "")),
    (("megalith", "menhir", "dolmen", "sarcophagus"), "Megalithic site", ("Realestate", "Archaeological site", "Megalithic site", "", "")),
    (("burial", "grave", "cemetery", "jar burial"), "Burial site", ("Realestate", "Archaeological site", "Burial site", "", "")),
    (("lithic workshop", "workshop"), "Workshop site", ("Realestate", "Archaeological site", "Workshop site", "", "")),
    (("surface survey", "survey"), "Surveyed site", ("Realestate", "Archaeological site", "Surveyed site", "", "")),
    (("excavation", "excavated", "trench", "test pit"), "Excavated site", ("Realestate", "Archaeological site", "Excavated site", "", "")),
    (("rockshelter", "rock shelter", "shelter"), "Rockshelter", ("Realestate", "Archaeological site", "Rockshelter", "", "")),
    (("collapsed cave", "cave"), "Cave", ("Realestate", "Archaeological site", "Cave", "", "")),
    (("open site", "open-air"), "Open-air site", ("Realestate", "Archaeological site", "Open-air site", "", "")),
    (("river terrace", "river"), "Riverine site", ("Realestate", "Archaeological site", "Riverine site", "", "")),
    (("settlement", "occupation"), "Occupation site", ("Realestate", "Archaeological site", "Occupation site", "", "")),
]

INDONESIA_TERMS = {
    "indonesia",
    "indonesian",
    "sumatra",
    "jawa",
    "java",
    "sulawesi",
    "bali",
    "lombok",
    "sumbawa",
    "flores",
    "alor",
    "timor",
    "timor barat",
    "kalimantan",
    "papua",
    "irian",
    "maluku",
    "moluccas",
    "nusa tenggara",
    "halmahera",
    "seram",
    "aru",
    "gebe",
    "talaud",
    "minahasa",
    "maros",
    "sangiran",
    "trinil",
    "topogaro",
    "mata menge",
    "blora",
    "ponorogo",
}

FOREIGN_TERMS = {
    "philippines",
    "philippine",
    "palawan",
    "malaysia",
    "sarawak",
    "sabah",
    "thailand",
    "vietnam",
    "laos",
    "cambodia",
    "timor-leste",
    "east timor",
    "papua new guinea",
    "new guinea",
    "china",
    "taiwan",
    "japan",
    "russia",
    "africa",
    "australia",
    "europe",
    "india",
    "korea",
}

INDONESIA_LOCALISATION_PATTERNS = [
    (("south sulawesi", "sulawesi selatan"), "Indonesia | South Sulawesi"),
    (("central sulawesi", "sulawesi tengah"), "Indonesia | Central Sulawesi"),
    (("southeast sulawesi", "sulawesi tenggara"), "Indonesia | Southeast Sulawesi"),
    (("north sulawesi", "sulawesi utara"), "Indonesia | North Sulawesi"),
    (("west sulawesi", "sulawesi barat"), "Indonesia | West Sulawesi"),
    (("sulawesi", "maros", "minahasa", "talaud"), "Indonesia | Sulawesi"),
    (("west java", "jawa barat"), "Indonesia | West Java"),
    (("central java", "jawa tengah"), "Indonesia | Central Java"),
    (("east java", "jawa timur"), "Indonesia | East Java"),
    (("java", "jawa", "blora", "ponorogo", "trinil", "sangiran"), "Indonesia | Java"),
    (("sumatra", "sumatera", "south sumatra", "sumatera selatan"), "Indonesia | Sumatra"),
    (("kalimantan timur", "east kalimantan"), "Indonesia | East Kalimantan"),
    (("kalimantan selatan", "south kalimantan"), "Indonesia | South Kalimantan"),
    (("kalimantan", "borneo"), "Indonesia | Kalimantan"),
    (("flores",), "Indonesia | Flores"),
    (("nusa tenggara", "alor", "lombok", "sumbawa"), "Indonesia | Nusa Tenggara"),
    (("maluku", "moluccas", "halmahera", "seram", "aru", "gebe"), "Indonesia | Maluku Islands"),
    (("papua", "irian", "new guinea"), "Indonesia | Papua"),
    (("bali",), "Indonesia | Bali"),
]

PERIOD_MAP = {
    "Early Pleistocene": (-2500000, -498050),
    "Middle Pleistocene": (-498050, -127050),
    "Late Pleistocene": (-127050, -11700),
    "Pleistocene": (-2580000, -11700),
    "Early Holocene": (-11700, -8200),
    "Mid Holocene": (-8200, -4200),
    "Late Holocene": (-4200, 1950),
    "Holocene": (-11700, 1950),
    "Palaeolithic": (-2000000, -4000),
    "Paleolithic": (-2000000, -4000),
    "Neolithic": (-4000, -350),
    "Early Metal Age": (-550, 400),
    "Paleometallic": (-550, 400),
    "Protohistory": (400, 1500),
    "Megalithic": (-4000, 1500),
    "Bronze Age": (-3300, -1200),
    "Iron Age": (-1200, 500),
}


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\u0000", " ")).strip()


def norm_site_key(name: str) -> str:
    lowered = clean(name).lower()
    lowered = lowered.replace("gua ", "leang ") if lowered.startswith("gua ") else lowered
    lowered = lowered.replace("goa ", "leang ") if lowered.startswith("goa ") else lowered
    lowered = lowered.replace("liang ", "leang ") if lowered.startswith("liang ") else lowered
    lowered = lowered.replace("rock shelter", "rockshelter")
    lowered = lowered.replace(" cave", "")
    lowered = lowered.replace(" site", "")
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


def stable_id(site_name: str) -> str:
    digest = hashlib.sha1(site_name.encode("utf-8")).hexdigest()
    return f"drive-{digest[:12]}"


def signed_decimal(value: str, direction: str) -> float:
    number = float(value)
    if direction.upper() in {"S", "W"}:
        number *= -1
    return number


def format_float(value: float | None) -> str:
    return "" if value is None else f"{value:.6f}".rstrip("0").rstrip(".")


def prefer_name(names: Counter[str]) -> str:
    if not names:
        return ""
    return sorted(names.items(), key=lambda item: (-item[1], len(item[0]), item[0]))[0][0]


def citation_from_source(source_file: str, text: str) -> str:
    dois = []
    for doi in DOI_REGEX.findall(text):
        cleaned = doi.rstrip(").,;")
        suffix = cleaned.split("/", 1)[1] if "/" in cleaned else ""
        if len(suffix) < 6:
            continue
        dois.append(cleaned)
    if dois:
        normalized = sorted(set(dois))
        return " | ".join(f"https://doi.org/{doi}" for doi in normalized)
    stem = Path(source_file).stem.replace("_", " ").replace("+", " ").strip()
    stem = re.sub(r"\s+", " ", stem)
    author = ""
    author_match = re.match(r"([A-Z][A-Za-z'’.-]+(?:\s+[A-Z][A-Za-z'’.-]+){0,2})", stem)
    if author_match:
        author = author_match.group(1)
    title = re.sub(r"^\d+[A-Za-z-]*\s*", "", stem).strip(" -")
    if author and author.lower() not in title.lower():
        return f"{title} | {author}"
    return title


def bibliography_value(values: set[str]) -> str:
    filtered = []
    for value in values:
        value = clean(value)
        if not value:
            continue
        if value.startswith("https://doi.org/"):
            filtered.append(value)
            continue
        if len(value) < 8:
            continue
        if re.fullmatch(r"[\d.\s-]+", value):
            continue
        if value.lower().startswith(("article text", "text-", "admin,")):
            continue
        filtered.append(value)

    def sort_key(value: str) -> tuple[int, str]:
        return (0 if value.startswith("https://doi.org/") else 1, value.lower())

    return " | ".join(sorted(set(filtered), key=sort_key)[:10])


def infer_state_of_knowledge(text: str) -> str:
    lowered = text.lower()
    if "excavat" in lowered or "trench" in lowered or "test pit" in lowered:
        return "Excavated"
    if "survey" in lowered or "surface find" in lowered or "foot survey" in lowered:
        return "Foot survey"
    if "reported" in lowered or "described" in lowered or "publication" in lowered:
        return "Literature"
    return "Not documented"


def infer_charac(text: str) -> tuple[str, str, str, str, str]:
    lowered = text.lower()
    for patterns, _, value in SITE_TYPE_MAPPINGS:
        if any(pattern in lowered for pattern in patterns):
            return value
    return ("Realestate", "Archaeological site", "Unknown site type", "", "")


def infer_type_label(text: str) -> str:
    lowered = text.lower()
    for patterns, label, _ in SITE_TYPE_MAPPINGS:
        if any(pattern in lowered for pattern in patterns):
            return label
    for value in LOCALITY_SITE_TYPES:
        if value in lowered:
            return value.title()
    return ""


def text_term_hits(text: str, terms: set[str]) -> int:
    lowered = clean(text).lower()
    return sum(1 for term in terms if term in lowered)


def infer_occupation(text: str) -> str:
    lowered = text.lower()
    if "continuous" in lowered or "long-term" in lowered or "spanning" in lowered:
        return "Continuous"
    if "multiple phase" in lowered or "several phase" in lowered:
        return "Multiple"
    return "Not specified"


def infer_periods(text: str) -> tuple[str, str]:
    starts = []
    ends = []
    for label, (start, end) in PERIOD_MAP.items():
        if label.lower() in text.lower():
            starts.append(start)
            ends.append(end)
    for match in MIN_AGE_REGEX.finditer(text):
        starts.append(1950 - int(match.group(1).replace(",", "")))
    for match in MAX_AGE_REGEX.finditer(text):
        ends.append(1950 - int(match.group(1).replace(",", "")))
    if not starts and not ends:
        return "", ""
    return str(min(starts or ends)), str(max(ends or starts))


def infer_localisation(text: str) -> str:
    values = []
    seen = set()
    for match in ADMIN_REGEX.finditer(text):
        value = clean(match.group(1))
        key = value.lower()
        if key not in seen:
            seen.add(key)
            values.append(value)
    return " | ".join(values[:4])


def infer_indonesian_localisation(text: str, site_name: str = "") -> str:
    lowered = clean(f"{site_name} {text}").lower()
    for patterns, label in INDONESIA_LOCALISATION_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return label
    return ""


def is_plausible_site_name(name: str) -> bool:
    name = clean(name)
    if not name or len(name) < 4:
        return False
    if any(name.startswith(prefix) for prefix in GENERIC_BAD_STARTS):
        return False
    if any(bad in name for bad in GENERIC_BAD_CONTAINS):
        return False
    return any(token.lower() in name.lower() for token in SITE_TOKEN_HINTS)


@dataclass
class SiteRecord:
    names: Counter[str] = field(default_factory=Counter)
    localisations: Counter[str] = field(default_factory=Counter)
    bibliographies: set[str] = field(default_factory=set)
    comments: list[str] = field(default_factory=list)
    state_of_knowledge: Counter[str] = field(default_factory=Counter)
    occupation: Counter[str] = field(default_factory=Counter)
    charac: Counter[tuple[str, str, str, str, str]] = field(default_factory=Counter)
    starting_periods: list[int] = field(default_factory=list)
    ending_periods: list[int] = field(default_factory=list)
    coordinates: list[tuple[float, float]] = field(default_factory=list)
    source_files: set[str] = field(default_factory=set)
    type_labels: Counter[str] = field(default_factory=Counter)
    locality_info_hits: int = 0
    indonesia_hits: int = 0
    foreign_hits: int = 0


def normalize_candidate_name(name: str) -> str:
    name = clean(name).strip(" ,;:.()[]")
    name = re.sub(r"\bSite\b$", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"\s+[A-Z]{2,4}\d*$", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def is_noise_name(name: str) -> bool:
    lowered = clean(name).lower()
    if lowered in {value.lower() for value in GENERIC_BAD_EXACT}:
        return True
    if any(bad in lowered for bad in (value.lower() for value in GENERIC_BAD_CONTAINS)):
        return True
    words = re.findall(r"[a-z0-9']+", lowered)
    if not words:
        return True
    if any(word in BAD_WORDS for word in words):
        return True
    if len(words) > 6:
        return True
    if len(words) >= 4 and not any(token.lower() in lowered for token in SITE_TOKEN_HINTS):
        return True
    return False


def has_site_signal(name: str) -> bool:
    lowered = clean(name).lower()
    return any(token.lower() in lowered for token in SITE_TOKEN_HINTS)


def has_indonesian_site_signal(name: str) -> bool:
    lowered = clean(name).lower()
    if lowered.startswith(("gua ", "goa ", "leang ", "ceruk ")):
        return True
    return any(term in lowered for term in ("trinil", "sangiran", "mata menge", "topogaro", "liang bua", "semedo", "pedawa"))


def site_support_score(record: SiteRecord) -> int:
    score = 0
    score += sum(record.names.values())
    score += len(record.source_files)
    score += len(record.coordinates) * 3
    score += record.locality_info_hits * 5
    return score


def is_record_worth_exporting(site_name: str, record: SiteRecord) -> bool:
    if is_noise_name(site_name):
        return False
    indonesia_score = record.indonesia_hits + (2 if has_indonesian_site_signal(site_name) else 0)
    if indonesia_score <= record.foreign_hits:
        return False
    if record.locality_info_hits:
        return True
    if len(record.coordinates) > 0:
        return True
    if sum(record.names.values()) >= 3:
        return True
    if len(record.source_files) >= 2 and has_site_signal(site_name):
        return True
    if sum(record.names.values()) >= 2 and has_site_signal(site_name):
        return True
    return False


def add_comment(record: SiteRecord, snippet: str) -> None:
    snippet = clean(snippet)
    if not snippet:
        return
    if snippet not in record.comments:
        record.comments.append(snippet[:700])


def score_comment(snippet: str, site_name: str) -> tuple[int, int, int]:
    lowered = clean(snippet).lower()
    site_lower = clean(site_name).lower()
    score = 0
    if site_lower and site_lower in lowered:
        score += 6
    if any(word in lowered for word in ("excavat", "occupation", "burial", "rock art", "dated", "holocene", "pleistocene", "cave", "shelter", "site")):
        score += 4
    score += text_term_hits(lowered, INDONESIA_TERMS)
    score -= len(DOI_REGEX.findall(snippet)) * 3
    score -= lowered.count("et al") * 2
    score -= sum(lowered.count(token) for token in (" vol. ", " pp.", "doi:", "journal", "reference", "eds.")) * 2
    return (score, -abs(320 - len(snippet)), -len(snippet))


def site_type_from_name(site_name: str) -> tuple[str, tuple[str, str, str, str, str]]:
    lowered = clean(site_name).lower()
    if lowered.startswith("ceruk "):
        return ("Rockshelter", ("Realestate", "Archaeological site", "Rockshelter", "", ""))
    if lowered.startswith(("gua ", "goa ")) or " cave" in lowered:
        return ("Cave", ("Realestate", "Archaeological site", "Cave", "", ""))
    if lowered.startswith(("leang ", "liang ")) or " shelter" in lowered or "rockshelter" in lowered:
        return ("Rockshelter", ("Realestate", "Archaeological site", "Rockshelter", "", ""))
    if lowered in {"trinil"} or " river" in lowered:
        return ("Riverine site", ("Realestate", "Archaeological site", "Riverine site", "", ""))
    return ("Unknown site type", ("Realestate", "Archaeological site", "Unknown site type", "", ""))


def load_existing_site_keys() -> set[str]:
    if not EXISTING_SITES_CSV.exists():
        return set()
    keys = set()
    with EXISTING_SITES_CSV.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            site_name = clean(row.get("SITE_NAME", ""))
            if site_name:
                keys.add(norm_site_key(site_name))
    return keys


def load_candidates() -> tuple[dict[str, SiteRecord], dict[str, set[str]]]:
    records: dict[str, SiteRecord] = {}
    source_to_keys: dict[str, set[str]] = defaultdict(set)
    with CANDIDATES_CSV.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            site_name = normalize_candidate_name(row["site_name"])
            if not is_plausible_site_name(site_name):
                continue
            if is_noise_name(site_name):
                continue
            key = norm_site_key(site_name)
            record = records.setdefault(key, SiteRecord())
            record.names[site_name] += 1
            record.source_files.add(row["source_file"])
            record.bibliographies.add(citation_from_source(row["source_file"], row["evidence_snippet"]))
            source_to_keys[row["source_file"]].add(key)
            add_comment(record, row["evidence_snippet"])
            record.indonesia_hits += text_term_hits(row["evidence_snippet"], INDONESIA_TERMS)
            record.foreign_hits += text_term_hits(row["evidence_snippet"], FOREIGN_TERMS)
            localisation = infer_indonesian_localisation(row["evidence_snippet"], site_name)
            if localisation:
                record.localisations[localisation] += 1
            start_period, end_period = infer_periods(row["evidence_snippet"])
            if start_period:
                record.starting_periods.append(int(start_period))
            if end_period:
                record.ending_periods.append(int(end_period))
            record.state_of_knowledge[infer_state_of_knowledge(row["evidence_snippet"])] += 1
            record.occupation[infer_occupation(row["evidence_snippet"])] += 1
            record.charac[infer_charac(row["evidence_snippet"])] += 1
            type_label = infer_type_label(row["evidence_snippet"])
            if type_label:
                record.type_labels[type_label] += 1
            if row["latitude"] and row["longitude"]:
                try:
                    record.coordinates.append((float(row["longitude"]), float(row["latitude"])))
                except ValueError:
                    pass
    return records, source_to_keys


def enrich_from_text(records: dict[str, SiteRecord], source_to_keys: dict[str, set[str]]) -> None:
    for path in sorted(TEXT_ROOT.rglob("*.json")):
        if not path.name.startswith("localityInfoPDF"):
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_file = payload.get("source_pdf", str(path))
        joined = clean(" ".join(payload.get("page_texts", [])))
        citation = citation_from_source(source_file, joined)

        # High-confidence locality sheets.
        name_match = LOCALITY_INFO_NAME_REGEX.search(joined)
        if name_match:
            site_name = normalize_candidate_name(name_match.group(1))
            key = norm_site_key(site_name)
            record = records.setdefault(key, SiteRecord())
            record.names[site_name] += 5
            record.source_files.add(source_file)
            record.bibliographies.add(citation)
            record.locality_info_hits += 1

            region = ""
            country = ""
            region_match = LOCALITY_INFO_REGION_REGEX.search(joined)
            if region_match:
                region = clean(region_match.group(1))
            country_match = LOCALITY_INFO_COUNTRY_REGEX.search(joined)
            if country_match:
                country = clean(country_match.group(1))
            localisation = " | ".join(part for part in [country, region] if part)
            if localisation:
                record.localisations[localisation] += 3
            if country.lower() == "indonesia":
                record.indonesia_hits += 10
            else:
                record.foreign_hits += text_term_hits(country, FOREIGN_TERMS) * 10
            record.indonesia_hits += text_term_hits(region, INDONESIA_TERMS) * 3
            record.foreign_hits += text_term_hits(region, FOREIGN_TERMS) * 3

            coords_match = LOCALITY_INFO_COORDS_REGEX.search(joined)
            if coords_match:
                lat = signed_decimal(coords_match.group(1), coords_match.group(2))
                lon = signed_decimal(coords_match.group(3), coords_match.group(4))
                record.coordinates.append((lon, lat))

            type_match = LOCALITY_INFO_TYPE_REGEX.search(joined)
            if type_match:
                clean_type = clean(type_match.group(1))
                record.charac[infer_charac(clean_type)] += 3
                type_label = infer_type_label(clean_type)
                if type_label:
                    record.type_labels[type_label] += 3

            summary_bits = []
            for regex in (LOCALITY_INFO_SUMMARY_REGEX, LOCALITY_INFO_COMMENTS_REGEX):
                match = regex.search(joined)
                if match:
                    summary_bits.append(clean(match.group(1)))
            if summary_bits:
                add_comment(record, " ".join(summary_bits))
            record.indonesia_hits += text_term_hits(joined, INDONESIA_TERMS)
            record.foreign_hits += text_term_hits(joined, FOREIGN_TERMS)
            fallback_localisation = infer_indonesian_localisation(joined, site_name)
            if fallback_localisation:
                record.localisations[fallback_localisation] += 2

            start_period, end_period = infer_periods(joined)
            if start_period:
                record.starting_periods.append(int(start_period))
            if end_period:
                record.ending_periods.append(int(end_period))
            record.state_of_knowledge[infer_state_of_knowledge(joined)] += 2
            record.occupation[infer_occupation(joined)] += 1


def choose_coord(coords: list[tuple[float, float]]) -> tuple[str, str]:
    if not coords:
        return "", ""
    lon, lat = coords[0]
    return format_float(lon), format_float(lat)


def summarize_comments(site_name: str, record: SiteRecord) -> str:
    deduped = []
    for snippet in record.comments:
        if snippet not in deduped:
            deduped.append(snippet)
    ranked = sorted(deduped, key=lambda snippet: score_comment(snippet, site_name), reverse=True)
    description = " ".join(ranked[:2]).strip()
    description = re.sub(r"\s+", " ", description)
    description = re.sub(r"https?://\S+", "", description).strip()
    type_hint = record.type_labels.most_common(1)[0][0] if record.type_labels else ""
    parts = []
    if description:
        parts.append(f"Description: {description[:900]}")
    if type_hint:
        parts.append(f"Keywords: {type_hint}")
    return " | ".join(parts[:3])


def build_rows(records: dict[str, SiteRecord], existing_site_keys: set[str]) -> list[dict[str, str]]:
    rows = []
    for key, record in records.items():
        site_name = prefer_name(record.names)
        if not site_name:
            continue
        if key in existing_site_keys:
            continue
        if not is_record_worth_exporting(site_name, record):
            continue
        longitude, latitude = choose_coord(record.coordinates)
        localisation = prefer_name(record.localisations) if record.localisations else infer_indonesian_localisation(" ".join(record.comments), site_name)
        bibliography = bibliography_value(record.bibliographies)
        comment = summarize_comments(site_name, record)
        charac = record.charac.most_common(1)[0][0] if record.charac else ("Realestate", "Archaeological site", "Unknown site type", "", "")
        if charac[2] == "Unknown site type":
            inferred_label, inferred_charac = site_type_from_name(site_name)
            if inferred_label != "Unknown site type":
                charac = inferred_charac
                record.type_labels[inferred_label] += 1
        start = str(min(record.starting_periods)) if record.starting_periods else ""
        end = str(max(record.ending_periods)) if record.ending_periods else ""
        rows.append(
            {
                "SITE_SOURCE_ID": stable_id(site_name),
                "SITE_NAME": site_name,
                "LOCALISATION": localisation,
                "GEONAME_ID": "",
                "PROJECTION_SYSTEM": "4326" if longitude and latitude else "",
                "LONGITUDE": longitude,
                "LATITUDE": latitude,
                "ALTITUDE": "",
                "CITY_CENTROID": "No",
                "STATE_OF_KNOWLEDGE": record.state_of_knowledge.most_common(1)[0][0] if record.state_of_knowledge else "Not documented",
                "OCCUPATION": record.occupation.most_common(1)[0][0] if record.occupation else "Not specified",
                "STARTING_PERIOD": start,
                "ENDING_PERIOD": end,
                "MAIN_CHARAC": charac[0],
                "CHARAC_LVL1": charac[1],
                "CHARAC_LVL2": charac[2],
                "CHARAC_LVL3": charac[3],
                "CHARAC_LVL4": charac[4],
                "CHARAC_EXP": "No",
                "BIBLIOGRAPHY": bibliography,
                "COMMENTS": comment,
                "WEB_IMAGES": "",
            }
        )
    rows.sort(key=lambda row: (-site_support_score(records[norm_site_key(row["SITE_NAME"])]), row["SITE_NAME"].lower()))
    return rows


def write_csv(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def write_report(rows: list[dict[str, str]]) -> None:
    with_coords = sum(1 for row in rows if row["LONGITUDE"] and row["LATITUDE"])
    lines = [
        f"Rows: {len(rows)}",
        f"Rows with coordinates: {with_coords}",
        f"Output: {OUTPUT_CSV}",
        "Sample rows:",
    ]
    for row in rows[:25]:
        lines.append(
            f"- {row['SITE_NAME']} | {row['LOCALISATION']} | {row['LONGITUDE']},{row['LATITUDE']} | {row['BIBLIOGRAPHY'][:120]}"
        )
    OUTPUT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    records, source_to_keys = load_candidates()
    existing_site_keys = load_existing_site_keys()
    enrich_from_text(records, source_to_keys)
    rows = build_rows(records, existing_site_keys)
    write_csv(rows)
    write_report(rows)
    print(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")
    print(f"Report: {OUTPUT_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
