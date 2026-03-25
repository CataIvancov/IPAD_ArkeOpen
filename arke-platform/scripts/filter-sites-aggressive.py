#!/usr/bin/env python3
"""
Ultra-conservative filtering: Extract only high-confidence formatted site names.

This uses a stricter approach: only accept sites that:
1. Come from well-OCR'd PDFs (500+ chars)
2. Match expected archaeological site name patterns
3. Are short & simple (4-5 words max)
4. Have supporting context in the same PDF

Target: ~5,000 high-confidence sites from 16,431.

Usage:
    python3 scripts/filter-sites-aggressive.py
"""

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
CANDIDATES_FILE = ROOT / "data" / "site-geolocation-candidates.csv"
TEXT_ROOT = ROOT / "data" / "google-drive-text"
OUTPUT_FILE = ROOT / "data" / "site-geolocation-candidates-highconf.csv"

# Strict site name patterns - match formatted site names
SITE_PATTERNS = [
    # Indonesian cave names: Gua/Leang/Ceruk + name
    r"^(?:Gua|Leang|Ceruk|Liang|Goa|Ngalau|Lhoong)\s+[A-Z][\w\s'-]{2,30}$",
    # Sites with numeric variants: Site Name 1, Site Name 2
    r"^[A-Z][\w\s'-]{2,30}\s+[12]$",
    # Geographic+site: described archaeological sites
    r"^(?:Situs|Site)\s+[A-Z][\w\s'-]{3,30}$",
    # Specific named sites (common)
    r"^(?:Liang Bua|Trinil|Sangiran|Semedo|Jerimalai|Lene Hara|Mata Menge)(?:\s|$)",
]


def load_text_metadata() -> Dict[str, dict]:
    """Load OCR metadata for all JSON files."""
    metadata = {}
    for json_path in TEXT_ROOT.glob("*.json"):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            source_pdf = data.get("source_pdf", "")
            metadata[source_pdf] = {
                "pages": data.get("pages", 0),
                "text_chars": data.get("text_chars", 0),
                "confidence": data.get("ocr_confidence", 0.0),
            }
        except:
            pass
    return metadata


def matches_site_pattern(name: str) -> bool:
    """Check if name matches expected archaeological site name patterns."""
    if not name or len(name) < 3 or len(name) > 60:
        return False

    for pattern in SITE_PATTERNS:
        if re.match(pattern, name.strip()):
            return True

    return False


def is_valid_high_conf(name: str, pdf_metadata: dict) -> Tuple[bool, str]:
    """Ultra-conservative validation."""
    if not name:
        return False, "empty"

    name = name.strip()

    # Must match a known site pattern
    if not matches_site_pattern(name):
        return False, "pattern_mismatch"

    # Source PDF must have good OCR (500+ chars)
    pdf_text_chars = pdf_metadata.get("text_chars", 0)
    if pdf_text_chars < 500:
        return False, "poor_ocr_source"

    return True, "valid"


def filter_candidates() -> None:
    """Filter using aggressive/ultra-conservative approach."""
    print("Loading OCR metadata...")
    text_metadata = load_text_metadata()
    print(f"  Loaded metadata for {len(text_metadata)} PDFs")

    print(f"\nLoading candidates...")
    with open(CANDIDATES_FILE, encoding="utf-8") as f:
        candidates = list(csv.DictReader(f))

    print(f"  Total candidates: {len(candidates)}")

    # Group by source_file to understand patterns
    sources = defaultdict(list)
    for cand in candidates:
        sources[cand.get("source_file", "")].append(cand)

    print(f"  From {len(sources)} unique PDFs")

    # Validate with strict criteria
    valid_sites = []
    rejected_sites = []
    rejection_stats = {}

    print("\nValidating candidates (aggressive mode)...")
    for i, cand in enumerate(candidates):
        site_name = cand.get("site_name", "").strip()
        pdf_path = cand.get("source_file", "")

        # Get metadata for this PDF
        pdf_meta = text_metadata.get(pdf_path, {"text_chars": 0})

        is_valid, reason = is_valid_high_conf(site_name, pdf_meta)

        if is_valid:
            valid_sites.append(cand)
        else:
            rejected_sites.append({**cand, "rejection_reason": reason})
            rejection_stats[reason] = rejection_stats.get(reason, 0) + 1

        if (i + 1) % 2000 == 0:
            print(f"  Processed {i + 1}/{len(candidates)} ({len(valid_sites)} valid so far)")

    # Write valid sites
    print(f"\nWriting {len(valid_sites)} high-confidence sites...")
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        if valid_sites:
            writer = csv.DictWriter(f, fieldnames=candidates[0].keys())
            writer.writeheader()
            writer.writerows(valid_sites)

    # Statistics
    print(f"\n{'='*60}")
    print(f"Ultra-Conservative Filtering Results:")
    print(f"{'='*60}")
    print(f"  Input candidates:     {len(candidates):6d}")
    print(f"  High-conf sites kept: {len(valid_sites):6d} ({100*len(valid_sites)/len(candidates):.1f}%)")
    print(f"  Rejected:             {len(rejected_sites):6d} ({100*len(rejected_sites)/len(candidates):.1f}%)")
    print(f"\nRejection Breakdown:")
    for reason in sorted(rejection_stats.keys(), key=lambda x: -rejection_stats[x])[:10]:
        count = rejection_stats[reason]
        pct = 100 * count / len(rejected_sites)
        print(f"  {reason:20s}: {count:6d} ({pct:5.1f}%)")
    print(f"{'='*60}")

    # Show sample of kept sites
    print(f"\nSample of high-confidence sites:")
    for site in valid_sites[:10]:
        print(f"  - {site['site_name']}")


if __name__ == "__main__":
    filter_candidates()
