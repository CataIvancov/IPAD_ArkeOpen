#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEXT_ROOT = ROOT / "data" / "google-drive-text"
OUTPUT_CSV = ROOT / "data" / "technoculture-candidates-first-pass.csv"
OUTPUT_REPORT = ROOT / "data" / "technoculture-candidates-first-pass-report.txt"

MULTISPACE = re.compile(r"\s+")

SEEDED_LABELS = [
    "Toalean",
    "Hoabinhian",
    "Pacitanian",
    "Sampungian",
    "Cabengian",
    "Dong Son",
    "Son Vi",
    "Bac Son",
    "Hoa Binh",
    "Sa Huynh-Kalanay",
    "Acheulean",
    "Acheulian",
    "Mousterian",
    "Aurignacian",
    "Gravettian",
    "Magdalenian",
    "Clactonian",
    "Levallois",
    "Microlithic",
    "Oldowan",
    "Soanian",
    "Szeletian",
    "Solutrean",
    "Epigravettian",
    "Aterian",
    "Paleoindian",
    "Mesolithic",
    "Neolithic",
]

LABEL_REGEX = re.compile(r"\b(" + "|".join(re.escape(label) for label in SEEDED_LABELS) + r")\b", re.IGNORECASE)
CONTEXT_LABEL_REGEX = re.compile(
    r"\b([A-Z][A-Za-z'’-]{3,30}(?:ian|an))\b(?=[^.!?\n]{0,120}\b(?:industry|industries|technocomplex|techno-complex|technological complex|lithic tradition|stone tool tradition|culture|complex|tradition)\b)",
    re.IGNORECASE,
)
REVERSE_CONTEXT_REGEX = re.compile(
    r"\b(?:industry|industries|technocomplex|techno-complex|technological complex|lithic tradition|stone tool tradition|culture|complex|tradition)\b[^.!?\n]{0,80}\b([A-Z][A-Za-z'’-]{3,30}(?:ian|an))\b",
    re.IGNORECASE,
)
DEFINITION_REGEX = re.compile(
    r"\b(lithic technology|stone tool technology|lithic industry|lithic industries|technoculture|technocultures|technocomplex|techno-complex)\b",
    re.IGNORECASE,
)
PAGE_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")

GENERIC_BAD = {
    "Indonesian",
    "Malaysian",
    "Australian",
    "European",
    "Asian",
    "African",
    "Southeast Asian",
    "South Asian",
    "Melanesian",
    "Polynesian",
    "American",
    "Indian",
    "Korean",
    "Taiwan",
    "Palawan",
    "Kalimantan",
    "Sangiran",
    "Obsidian",
    "Technocomplex",
    "Techno-complex",
}


@dataclass(frozen=True)
class Candidate:
    label: str
    category: str
    context_type: str
    source_file: str
    page: int
    evidence_snippet: str


def clean(text: str) -> str:
    return MULTISPACE.sub(" ", text).strip()


def normalize_label(label: str) -> str:
    label = clean(label).strip(" ,;:.()[]")
    return label[:1].upper() + label[1:]


def classify(snippet: str) -> tuple[str, str]:
    lowered = snippet.lower()
    if any(term in lowered for term in ("industry", "industries", "lithic tradition", "stone tool tradition", "technocomplex", "techno-complex")):
        return ("lithic_industry", "industry")
    if any(term in lowered for term in ("culture", "tradition", "complex", "assemblage")):
        return ("technoculture", "culture")
    if any(term in lowered for term in ("lithic technology", "stone tool technology")):
        return ("lithic_technology", "definition")
    return ("technoculture", "mention")


def snippet_around(text: str, start: int, end: int, width: int = 220) -> str:
    left = max(0, start - width)
    right = min(len(text), end + width)
    return clean(text[left:right])


def plausible_label(label: str, snippet: str) -> bool:
    normalized = normalize_label(label)
    if len(normalized) < 4:
        return False
    if normalized in GENERIC_BAD:
        return False
    if normalized.endswith("nese") or normalized.endswith("esian"):
        return False
    lowered = snippet.lower()
    if normalized.lower() not in lowered and normalized.lower().replace("-", " ") not in lowered:
        return False
    context_terms = ("industry", "industries", "lithic", "stone tool", "technocomplex", "techno-complex", "assemblage", "tradition", "complex")
    if normalized not in SEEDED_LABELS and not any(term in lowered for term in context_terms):
        return False
    if normalized in {"Mesolithic", "Neolithic"} and "industry" not in lowered and "tradition" not in lowered and "lithic" not in lowered:
        return False
    return True


def iter_candidates_for_page(source_file: str, page_no: int, text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen: set[tuple[str, str, int, str]] = set()

    for regex, context_type in (
        (LABEL_REGEX, "seeded"),
        (CONTEXT_LABEL_REGEX, "contextual"),
        (REVERSE_CONTEXT_REGEX, "reverse_contextual"),
    ):
        for match in regex.finditer(text):
            label = normalize_label(match.group(1))
            snippet = snippet_around(text, match.start(), match.end())
            if not plausible_label(label, snippet):
                continue
            category, inferred_context = classify(snippet)
            key = (label.lower(), category, page_no, snippet.lower())
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                Candidate(
                    label=label,
                    category=category,
                    context_type=context_type if context_type != "seeded" else inferred_context,
                    source_file=source_file,
                    page=page_no,
                    evidence_snippet=snippet,
                )
            )

    for match in DEFINITION_REGEX.finditer(text):
        snippet = snippet_around(text, match.start(), match.end())
        key = (match.group(1).lower(), "definition", page_no, snippet.lower())
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            Candidate(
                label=normalize_label(match.group(1)),
                category="definition",
                context_type="definition",
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
            fieldnames=["label", "category", "context_type", "source_file", "page", "evidence_snippet"],
        )
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(candidate.__dict__)


def write_report(candidates: list[Candidate]) -> None:
    counts: dict[str, int] = {}
    labels: dict[str, int] = {}
    for candidate in candidates:
        counts[candidate.category] = counts.get(candidate.category, 0) + 1
        labels[candidate.label] = labels.get(candidate.label, 0) + 1

    lines = [
        f"Candidates: {len(candidates)}",
        "By category:",
    ]
    for category, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"  {category}: {count}")
    lines.append("Top labels:")
    for label, count in sorted(labels.items(), key=lambda item: (-item[1], item[0]))[:50]:
        lines.append(f"  {label}: {count}")
    OUTPUT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    candidates = mine_candidates()
    write_csv(candidates)
    write_report(candidates)
    print(f"Wrote {len(candidates)} candidates to {OUTPUT_CSV}")
    print(f"Report: {OUTPUT_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
