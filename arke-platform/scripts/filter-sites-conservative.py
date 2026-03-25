#!/usr/bin/env python3
"""
Conservative filtering of site candidates to remove false positives.

This script applies strict validation to the site-geolocation-candidates.csv
to produce a high-precision list of valid archaeological sites.

Target: Reduce 16,431 candidates to ~5,000 high-confidence sites.

Usage:
    python3 scripts/filter-sites-conservative.py
"""

import csv
import re
from pathlib import Path
from typing import Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
CANDIDATES_FILE = ROOT / "data" / "site-geolocation-candidates.csv"
OUTPUT_FILE = ROOT / "data" / "site-geolocation-candidates-filtered.csv"
REJECTED_FILE = ROOT / "data" / "site-geolocation-candidates-rejected.csv"

# Validation patterns (matching build-drive-sites-arkeogis-csv.py)
SITE_TOKEN_HINTS = {
    "leang", "leang-", "gua", "goa", "liang", "liang-", "ceruk",
    "cave", "rockshelter", "rock shelter", "shelter",
    "site", "complex", "trinil", "sangiran", "semedo", "topogaro",
    "mata menge", "liang bua", "laili", "mololo", "golo", "talepu",
    "pedawa", "gunung", "bukit", "situs", "pantai", "sungai", "pulau",
    "danau", "teluk", "selat", "hutan", "taman", "lembah", "pegunungan",
    "gua-", "lhoong", "ngalau", "jambi", "sumbawa", "kisar", "wetar",
    "alor", "rote", "sabu", "sawu", "banda", "ambon", "seram", "buru",
    "morotai", "ternate", "tidore", "halmahera", "tanimbar", "kai", "aru",
    "biak", "numfor", "yapen", "w papua", "lene hara", "jerimalai",
    "matja kuru", "asitau kuru", "bui ceri", "ili"
}

# Words/patterns that indicate false positives
BAD_EXACT_MATCHES = {
    "no sites", "rockshelter sites", "matsu archipelago sites",
    "lapita cultural complex", "nos techniques sites", "oldest sites",
    "at sites", "leang the", "babar timor"
}

BAD_KEYWORDS = {
    "general information", "synonyms", "country", "region", "coordinates",
    "type", "summary", "comments", "locality", "u-series", "u series",
    "sulawesi u", "corrected u-series", "and the", "the and",
    "gebe island", "no no. col", "col category", "col siding",
    "bhl97", "bhl98", "note", "scale", "konsentrasi", "kondisi",
    "dalam", "na'u", "archaeological site", "archaeological sites",
    "prehistoric site", "prehistoric sites", "protohistoric site",
    "historic site", "historic sites", "potential world heritage",
    "industrial complex", "heritage site", "village", "regency",
    "sites", "cultural complex", "archipelago sites", "east coast",
    "meanwhile", "astronomical", "techniques", "potential"
}


def clean_name(text: str) -> str:
    """Clean and normalize site name."""
    return re.sub(r"\s+", " ", (text or "").replace("\u0000", " ")).strip()


def has_site_hint(name: str) -> bool:
    """Check if name contains at least one site-related keyword."""
    normalized = name.lower()
    return any(hint in normalized for hint in SITE_TOKEN_HINTS)


def is_bad_word(name: str) -> bool:
    """Check if name contains disqualifying words."""
    normalized = name.lower()

    # Exact matches are always rejected
    if normalized in BAD_EXACT_MATCHES:
        return True

    # Check for bad keywords
    if any(bad in normalized for bad in BAD_KEYWORDS):
        return True

    return False


def is_fragment(name: str) -> bool:
    """Detect sentence fragments (e.g., 'Ceruk Pampini. Meanwhile,')."""
    # Ends with comma but no site hint
    if name.rstrip().endswith(',') and not has_site_hint(name):
        return True

    # Ends with period but too short
    if name.rstrip().endswith('.') and len(name) < 8:
        return True

    # Starts with lowercase (OCR fragment)
    if name and name[0].islower():
        return True

    return False


def is_ocr_artifact(name: str) -> bool:
    """Detect obvious OCR errors/artifacts."""
    # Too many words (likely captured a paragraph)
    if name.count(' ') > 8:
        return True

    # Very long without multiple proper nouns
    if len(name) > 100:
        return True

    # Contains lot of numbers/punctuation (corrupted)
    punct_count = sum(1 for c in name if c in '0123456789!@#$%^&*()')
    if len(name) > 20 and punct_count / len(name) > 0.15:
        return True

    return False


def is_valid_site(name: str) -> Tuple[bool, str]:
    """
    Conservative validation: all checks must pass.

    Returns: (is_valid, rejection_reason)
    """
    if not name:
        return False, "empty"

    name = clean_name(name)

    if len(name) < 3:
        return False, "too_short"

    if is_fragment(name):
        return False, "sentence_fragment"

    if is_ocr_artifact(name):
        return False, "ocr_artifact"

    if is_bad_word(name):
        return False, "bad_keyword"

    if not has_site_hint(name):
        return False, "no_site_hint"

    return True, "valid"


def filter_candidates() -> None:
    """Filter site candidates and produce output files."""
    print(f"Loading candidates from {CANDIDATES_FILE.name}...")

    with open(CANDIDATES_FILE, encoding="utf-8") as f:
        candidates = list(csv.DictReader(f))

    print(f"  Total candidates: {len(candidates)}")

    # Apply validation
    valid_sites = []
    rejected_sites = []
    rejection_stats = {}

    print("\nValidating candidates...")
    for i, cand in enumerate(candidates):
        site_name = cand.get("site_name", "").strip()
        is_valid, reason = is_valid_site(site_name)

        if is_valid:
            valid_sites.append(cand)
        else:
            rejected_sites.append({**cand, "rejection_reason": reason})
            rejection_stats[reason] = rejection_stats.get(reason, 0) + 1

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(candidates)}")

    # Write valid sites
    print(f"\nWriting {len(valid_sites)} valid sites to {OUTPUT_FILE.name}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        if valid_sites:
            writer = csv.DictWriter(f, fieldnames=candidates[0].keys())
            writer.writeheader()
            writer.writerows(valid_sites)

    # Write rejected sites with reason
    print(f"Writing {len(rejected_sites)} rejected sites to {REJECTED_FILE.name}...")
    with open(REJECTED_FILE, "w", encoding="utf-8", newline="") as f:
        if rejected_sites:
            rejected_keys = list(candidates[0].keys()) + ["rejection_reason"]
            writer = csv.DictWriter(f, fieldnames=rejected_keys)
            writer.writeheader()
            writer.writerows(rejected_sites)

    # Print statistics
    print(f"\n{'='*60}")
    print(f"Filtering Results:")
    print(f"{'='*60}")
    print(f"  Input candidates:     {len(candidates):6d}")
    print(f"  Valid sites (kept):   {len(valid_sites):6d} ({100*len(valid_sites)/len(candidates):.1f}%)")
    print(f"  Rejected sites:       {len(rejected_sites):6d} ({100*len(rejected_sites)/len(candidates):.1f}%)")
    print(f"\nRejection Breakdown:")
    for reason in sorted(rejection_stats.keys(), key=lambda x: -rejection_stats[x]):
        count = rejection_stats[reason]
        pct = 100 * count / len(rejected_sites)
        print(f"  {reason:20s}: {count:5d} ({pct:5.1f}%)")
    print(f"{'='*60}")


if __name__ == "__main__":
    filter_candidates()
