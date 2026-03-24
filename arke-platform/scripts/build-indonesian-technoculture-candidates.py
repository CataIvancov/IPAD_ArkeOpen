#!/usr/bin/env python3

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "data" / "technoculture-candidates-first-pass.csv"
OUTPUT_CSV = ROOT / "data" / "indonesian-technoculture-candidates.csv"
OUTPUT_REPORT = ROOT / "data" / "indonesian-technoculture-candidates-report.txt"

INDONESIA_TERMS = {
    "indonesia",
    "indonesian",
    "sulawesi",
    "java",
    "jawa",
    "sumatra",
    "sumatera",
    "kalimantan",
    "flores",
    "timor",
    "bali",
    "maluku",
    "moluccas",
    "papua",
    "irian",
    "halmahera",
    "seram",
    "aru",
    "gebe",
    "maros",
    "sangiran",
    "trinil",
    "pacitan",
    "sampung",
    "sulawesi selatan",
    "sulawesi tengah",
    "sulawesi tenggara",
    "jawa timur",
    "jawa tengah",
    "jawa barat",
}

ALLOWLIST = {
    "Toalean": ("technoculture", "Indonesia-focused lithic technoculture of Sulawesi"),
    "Hoabinhian": ("technoculture", "Southeast Asian technoculture discussed in Indonesian context"),
    "Pacitanian": ("lithic_industry", "Indonesian lithic industry/technology tradition associated with Java"),
    "Sampungian": ("lithic_industry", "Indonesian lithic/bone industry tradition associated with Java"),
    "Cabengian": ("lithic_industry", "Indonesian lithic industry candidate"),
    "Dong Son": ("technoculture", "Technocultural horizon referenced in Indonesian archaeology"),
    "Levallois": ("lithic_technology", "Lithic reduction strategy discussed for Indonesian assemblages"),
    "Acheulean": ("lithic_industry", "Lithic industry term in Indonesian comparative context"),
    "Acheulian": ("lithic_industry", "Lithic industry term in Indonesian comparative context"),
}

ALIASES = {
    "Toalian": "Toalean",
    "Oalean": "Toalean",
    "Patjitan": "Pacitanian",
    "Pacitan": "Pacitanian",
    "Patjitanian": "Pacitanian",
    "Hoa Binh": "Hoabinhian",
    "Acheulean": "Acheulian",
}


def clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def normalize_label(label: str) -> str:
    label = clean(label)
    return ALIASES.get(label, label)


def is_indonesian_context(snippet: str, source_file: str) -> bool:
    lowered = clean(f"{snippet} {source_file}").lower()
    return any(term in lowered for term in INDONESIA_TERMS)


def preferred_category(label: str, fallback: str) -> str:
    return ALLOWLIST.get(label, (fallback, ""))[0]


def write_outputs(rows: list[dict[str, str]], summary: list[str]) -> None:
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "label",
                "normalized_label",
                "category",
                "match_count",
                "source_count",
                "sources",
                "notes",
                "sample_evidence",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    OUTPUT_REPORT.write_text("\n".join(summary) + "\n", encoding="utf-8")


def main() -> int:
    grouped: dict[str, dict[str, object]] = {}

    with INPUT_CSV.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            original_label = clean(row["label"])
            normalized_label = normalize_label(original_label)
            if normalized_label not in ALLOWLIST:
                continue
            if not is_indonesian_context(row["evidence_snippet"], row["source_file"]):
                continue

            entry = grouped.setdefault(
                normalized_label,
                {
                    "label": original_label,
                    "normalized_label": normalized_label,
                    "category": preferred_category(normalized_label, row["category"]),
                    "match_count": 0,
                    "sources": set(),
                    "snippets": [],
                },
            )
            entry["match_count"] += 1
            entry["sources"].add(clean(row["source_file"]))
            snippets = entry["snippets"]
            snippet = clean(row["evidence_snippet"])
            if snippet and snippet not in snippets:
                snippets.append(snippet[:500])

    rows = []
    for normalized_label, entry in grouped.items():
        sources = sorted(entry["sources"])
        rows.append(
            {
                "label": entry["label"],
                "normalized_label": normalized_label,
                "category": entry["category"],
                "match_count": str(entry["match_count"]),
                "source_count": str(len(sources)),
                "sources": " | ".join(sources[:20]),
                "notes": ALLOWLIST[normalized_label][1],
                "sample_evidence": " | ".join(entry["snippets"][:3]),
            }
        )

    rows.sort(key=lambda row: (-int(row["match_count"]), row["normalized_label"].lower()))

    summary = [
        f"Normalized candidates: {len(rows)}",
        "Top labels:",
    ]
    for row in rows:
        summary.append(f"  {row['normalized_label']}: {row['match_count']} matches across {row['source_count']} sources")

    write_outputs(rows, summary)
    print(f"Wrote {len(rows)} normalized candidates to {OUTPUT_CSV}")
    print(f"Report: {OUTPUT_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
