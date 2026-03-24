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
OUTPUT_CSV = ROOT / "data" / "chronology-candidates-first-pass.csv"
OUTPUT_REPORT = ROOT / "data" / "chronology-candidates-first-pass-report.txt"


LABEL_PATTERNS = [
    r"Lower Palaeolithic",
    r"Middle Palaeolithic",
    r"Upper Palaeolithic",
    r"Lower Paleolithic",
    r"Middle Paleolithic",
    r"Upper Paleolithic",
    r"Late Pleistocene",
    r"Middle Pleistocene",
    r"Early Pleistocene",
    r"Late Holocene",
    r"Middle Holocene",
    r"Early Holocene",
    r"Late Quaternary",
    r"Middle Quaternary",
    r"Early Quaternary",
    r"Late Miocene",
    r"Middle Miocene",
    r"Early Miocene",
    r"Late Pliocene",
    r"Middle Pliocene",
    r"Early Pliocene",
    r"Palaeolithic",
    r"Paleolithic",
    r"Mesolithic",
    r"Epipalaeolithic",
    r"Epipaleolithic",
    r"Neolithic",
    r"Protohistoric",
    r"Protohistory",
    r"Prehistory",
    r"Prehistoric",
    r"Megalithic",
    r"Bronze Age",
    r"Iron Age",
    r"Metal Age",
    r"Historical period",
    r"Historic period",
    r"Historical",
    r"Historic",
    r"Classical period",
    r"Classical",
    r"Colonial period",
    r"Colonial",
    r"Islamic period",
    r"Islamic",
    r"Hindu-Buddhist period",
    r"Hindu-Buddhist",
    r"Pleistocene",
    r"Holocene",
    r"Quaternary",
    r"Miocene",
    r"Pliocene",
    r"Last Glacial Maximum",
]

LABEL_REGEX = re.compile(r"\b(" + "|".join(LABEL_PATTERNS) + r")\b", re.IGNORECASE)
YEAR_RANGE_REGEX = re.compile(
    r"(?P<start>\d{1,3}(?:,\d{3})?|\d{4,7})\s*(?P<start_era>BP|BC|BCE|AD|CE|cal BP|cal\. BP|ka|kya)?"
    r"\s*(?:-|–|—|to)\s*"
    r"(?P<end>\d{1,3}(?:,\d{3})?|\d{4,7})\s*(?P<end_era>BP|BC|BCE|AD|CE|cal BP|cal\. BP|ka|kya)?",
    re.IGNORECASE,
)
YEAR_SINGLE_REGEX = re.compile(
    r"(?P<value>\d{1,3}(?:,\d{3})?|\d{4,7})\s*(?P<era>BP|BC|BCE|AD|CE|cal BP|cal\. BP|ka|kya)",
    re.IGNORECASE,
)
YEARS_AGO_REGEX = re.compile(
    r"(?P<value>\d{1,3}(?:[.,]\d{3})?|\d{4,7})\s+years?\s+ago",
    re.IGNORECASE,
)
THOUSAND_YEARS_AGO_REGEX = re.compile(
    r"(?P<value>\d+(?:[.,]\d+)?)\s+thousand\s+years?\s+ago",
    re.IGNORECASE,
)
PAGE_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
MULTISPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class Candidate:
    label: str
    category: str
    start_date: str
    end_date: str
    source_file: str
    page: int
    evidence_snippet: str


def clean(text: str) -> str:
    return MULTISPACE.sub(" ", text).strip()


def normalize_label(label: str) -> str:
    label = clean(label)
    return label[:1].upper() + label[1:]


def classify(label: str, snippet: str) -> str:
    label_lower = label.lower()
    if any(term == label_lower or f" {term}" in label_lower or f"{term} " in label_lower for term in ["pleistocene", "holocene", "quaternary", "miocene", "pliocene"]):
        return "geological"
    if label_lower in {"historical", "historic", "historical period", "historic period", "classical", "classical period", "colonial", "colonial period", "islamic", "islamic period", "hindu-buddhist", "hindu-buddhist period", "medieval"}:
        return "historical"
    if label_lower in {"migration", "maritime"}:
        return "thematic"
    return "archaeological"


def normalize_year(value: str, era: str | None) -> int | None:
    raw = value.replace(",", "")
    try:
        number = float(raw)
    except ValueError:
        return None

    era_normalized = (era or "").lower().replace(".", "").strip()
    if era_normalized in {"bc", "bce"}:
        return -int(number)
    if era_normalized in {"ad", "ce"}:
        return int(number)
    if era_normalized in {"bp", "cal bp"}:
        return 1950 - int(number)
    if era_normalized in {"ka", "kya"}:
        return 1950 - int(number * 1000)
    if not era_normalized:
        return int(number)
    return None


def extract_date_span(snippet: str) -> tuple[str, str]:
    range_match = YEAR_RANGE_REGEX.search(snippet)
    if range_match:
        start_era = range_match.group("start_era")
        end_era = range_match.group("end_era")
        if not start_era and not end_era:
            start_raw = int(range_match.group("start").replace(",", ""))
            end_raw = int(range_match.group("end").replace(",", ""))
            if max(start_raw, end_raw) < 1000:
                return "", ""
        start = normalize_year(range_match.group("start"), range_match.group("start_era"))
        end = normalize_year(range_match.group("end"), range_match.group("end_era"))
        if start is not None and end is not None:
            ordered = sorted((start, end))
            return str(ordered[0]), str(ordered[1])

    thousand_match = THOUSAND_YEARS_AGO_REGEX.search(snippet)
    if thousand_match:
        try:
            value = float(thousand_match.group("value").replace(",", "."))
        except ValueError:
            value = None
        if value is not None:
            year = 1950 - int(value * 1000)
            return str(year), str(year)

    years_ago_match = YEARS_AGO_REGEX.search(snippet)
    if years_ago_match:
        try:
            value = int(years_ago_match.group("value").replace(",", "").replace(".", ""))
        except ValueError:
            value = None
        if value is not None:
            year = 1950 - value
            return str(year), str(year)

    singles = []
    for match in YEAR_SINGLE_REGEX.finditer(snippet):
        normalized = normalize_year(match.group("value"), match.group("era"))
        if normalized is not None:
            singles.append(normalized)
        if len(singles) == 2:
            ordered = sorted(singles)
            return str(ordered[0]), str(ordered[1])

    if singles:
        return str(singles[0]), str(singles[0])
    return "", ""


def snippet_around(text: str, start: int, end: int, width: int = 220) -> str:
    left = max(0, start - width)
    right = min(len(text), end + width)
    return clean(text[left:right])


def iter_candidates_for_page(source_file: str, page_no: int, text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen: set[tuple[str, int, str]] = set()
    for match in LABEL_REGEX.finditer(text):
        label = normalize_label(match.group(1))
        snippet = snippet_around(text, match.start(), match.end())
        start_date, end_date = extract_date_span(snippet)
        category = classify(label, snippet)
        key = (label.lower(), page_no, snippet.lower())
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            Candidate(
                label=label,
                category=category,
                start_date=start_date,
                end_date=end_date,
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
            all_candidates.extend(iter_candidates_for_page(source_file, page_index, page_text))
    return all_candidates


def write_csv(candidates: list[Candidate]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["label", "category", "start_date", "end_date", "source_file", "page", "evidence_snippet"],
        )
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(
                {
                    "label": candidate.label,
                    "category": candidate.category,
                    "start_date": candidate.start_date,
                    "end_date": candidate.end_date,
                    "source_file": candidate.source_file,
                    "page": candidate.page,
                    "evidence_snippet": candidate.evidence_snippet,
                }
            )


def write_report(candidates: list[Candidate]) -> None:
    by_category: dict[str, int] = {}
    by_label: dict[str, int] = {}
    dated = 0
    for candidate in candidates:
        by_category[candidate.category] = by_category.get(candidate.category, 0) + 1
        by_label[candidate.label] = by_label.get(candidate.label, 0) + 1
        if candidate.start_date or candidate.end_date:
            dated += 1

    top_labels = sorted(by_label.items(), key=lambda item: (-item[1], item[0]))[:30]
    lines = [
        f"Candidates: {len(candidates)}",
        f"Candidates with dates: {dated}",
        "By category:",
    ]
    for category, count in sorted(by_category.items()):
        lines.append(f"  {category}: {count}")
    lines.append("Top labels:")
    for label, count in top_labels:
        lines.append(f"  {label}: {count}")
    OUTPUT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not TEXT_ROOT.exists():
        print(f"Missing extracted text directory: {TEXT_ROOT}", file=sys.stderr)
        return 1
    candidates = mine_candidates()
    write_csv(candidates)
    write_report(candidates)
    print(f"Wrote {len(candidates)} chronology candidates to {OUTPUT_CSV}")
    print(f"Report: {OUTPUT_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
