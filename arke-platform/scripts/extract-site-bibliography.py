#!/usr/bin/env python3
"""
Extract bibliography and DOI references from OCR text and link to sites.

Creates a bibliography lookup table and enriches site data with DOI references
found in their source PDFs.

Usage:
    python3 scripts/extract-site-bibliography.py
"""

import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Set


ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = ROOT / "data" / "site-geolocation-candidates-highconf.csv"
TEXT_ROOT = ROOT / "data" / "google-drive-text"
OUTPUT_FILE = ROOT / "data" / "site-geolocation-with-bibliography.csv"
BIBLIOGRAPHY_FILE = ROOT / "data" / "site-bibliography-sources.csv"

# DOI pattern (from build-drive-sites-arkeogis-csv.py)
DOI_REGEX = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)

# URL patterns
URL_PATTERNS = [
    re.compile(r"https?://[\w\-\.]+(\.[\w\-\.]+)+([\w,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?"),
]


def extract_dois_from_text(text: str) -> List[str]:
    """Extract all unique DOIs from text."""
    dois = []
    for match in DOI_REGEX.finditer(text):
        doi = match.group(0)
        # Normalize to https://doi.org/ format
        if not doi.startswith("https://"):
            doi = f"https://doi.org/{doi}"
        dois.append(doi)

    return list(dict.fromkeys(dois))  # Deduplicate


def extract_urls_from_text(text: str) -> List[str]:
    """Extract URLs from text."""
    urls = []
    for pattern in URL_PATTERNS:
        for match in pattern.finditer(text):
            urls.append(match.group(0))

    return list(dict.fromkeys(urls))


def process_sites() -> None:
    """Extract bibliography for all high-confidence sites."""
    print("Loading high-confidence sites...")
    with open(INPUT_FILE, encoding="utf-8") as f:
        sites = list(csv.DictReader(f))

    print(f"  Loaded {len(sites)} sites")

    # Prepare output and bibliography
    output_rows = []
    bibliography_rows = []
    stats = {
        "with_dois": 0,
        "with_urls": 0,
        "without_bibliography": 0,
    }

    # Track unique DOIs globally
    all_dois: Dict[str, Set[str]] = {}  # doi -> set of site_names

    print("\nExtracting bibliography...")
    for i, site in enumerate(sites):
        site_name = site.get("site_name", "").strip()
        pdf_path = site.get("source_file", "")

        # Try to load OCR text
        pdf_filename = Path(pdf_path).stem
        json_path = TEXT_ROOT / f"{pdf_filename}.json"

        bibliography = []

        if json_path.exists():
            try:
                ocr_data = json.loads(json_path.read_text(encoding="utf-8"))
                ocr_text = " ".join(ocr_data.get("page_texts", []))

                # Extract DOIs
                dois = extract_dois_from_text(ocr_text)
                if dois:
                    bibliography.extend(dois)
                    stats["with_dois"] += 1

                    # Track DOI associations
                    for doi in dois:
                        if doi not in all_dois:
                            all_dois[doi] = set()
                        all_dois[doi].add(site_name)

                # Fallback: extract URLs if no DOIs
                if not dois:
                    urls = extract_urls_from_text(ocr_text)
                    if urls:
                        bibliography.extend(urls[:3])  # Limit to 3 URLs
                        stats["with_urls"] += 1

            except Exception as e:
                pass

        if not bibliography:
            stats["without_bibliography"] += 1

        # Add to output
        out_row = {**site}
        out_row["BIBLIOGRAPHY"] = " | ".join(bibliography) if bibliography else ""
        output_rows.append(out_row)

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(sites)}")

    # Write enriched site data
    print(f"\nWriting {len(output_rows)} sites with bibliography...")
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        fieldnames = list(output_rows[0].keys())
        if "BIBLIOGRAPHY" not in fieldnames:
            fieldnames.append("BIBLIOGRAPHY")
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Create bibliography lookup table
    print(f"\nCreating bibliography lookup table...")
    for doi, site_names in sorted(all_dois.items()):
        for site_name in sorted(site_names):
            bibliography_rows.append({
                "site_name": site_name,
                "doi": doi,
                "source_url": doi,  # DOI is also a resolvable URL
            })

    with open(BIBLIOGRAPHY_FILE, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["site_name", "doi", "source_url"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(bibliography_rows)

    # Print statistics
    total_with_bibliography = stats["with_dois"] + stats["with_urls"]
    print(f"\n{'='*60}")
    print(f"Bibliography Extraction Results:")
    print(f"{'='*60}")
    print(f"  Total sites:           {len(output_rows):6d}")
    print(f"  With DOIs:             {stats['with_dois']:6d} ({100*stats['with_dois']/len(output_rows):.1f}%)")
    print(f"  With URLs only:        {stats['with_urls']:6d} ({100*stats['with_urls']/len(output_rows):.1f}%)")
    print(f"  With bibliography:     {total_with_bibliography:6d} ({100*total_with_bibliography/len(output_rows):.1f}%)")
    print(f"  Without bibliography:  {stats['without_bibliography']:6d} ({100*stats['without_bibliography']/len(output_rows):.1f}%)")
    print(f"\n  Bibliography lookup entries: {len(bibliography_rows)}")
    print(f"  Unique DOIs:           {len(all_dois)}")
    print(f"{'='*60}")

    # Show sample
    print("\nSample sites with bibliography:")
    for row in output_rows[:10]:
        if row.get("BIBLIOGRAPHY"):
            bib = row["BIBLIOGRAPHY"][:70] + "..." if len(row.get("BIBLIOGRAPHY", "")) > 70 else row.get("BIBLIOGRAPHY")
            print(f"  ✓ {row['site_name']:20s} -> {bib}")


if __name__ == "__main__":
    process_sites()
