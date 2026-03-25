#!/usr/bin/env python3
"""
Aggressive cleaning of high-confidence sites before merging.

Uses edit distance (Levenshtein) to find near-duplicates, identifies malformed
names, and produces a deduplicated/cleaned dataset ready for merging.

Usage:
    python3 scripts/clean-sites-for-merge.py
"""

import csv
from pathlib import Path
from difflib import SequenceMatcher
from typing import Dict, List, Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = ROOT / "data" / "site-geolocation-candidates-highconf.csv"
EXISTING_FILE = ROOT / "data" / "drive-sites-to-arkeogis.csv"
OUTPUT_CLEANED = ROOT / "data" / "site-geolocation-cleaned.csv"
OUTPUT_SUSPICIOUS = ROOT / "data" / "site-geolocation-suspicious.csv"
OUTPUT_DUPLICATES = ROOT / "data" / "site-geolocation-duplicates.csv"
OUTPUT_MERGE_READY = ROOT / "data" / "site-geolocation-merge-ready.csv"


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity_ratio(s1: str, s2: str) -> float:
    """Calculate similarity ratio (0-1) between two strings."""
    matcher = SequenceMatcher(None, s1.lower(), s2.lower())
    return matcher.ratio()


def is_suspicious(name: str) -> Tuple[bool, str]:
    """Check if site name has quality issues."""
    if not name or len(name) < 3:
        return True, "too_short"

    # Multiple years/numbers (likely OCR artifact)
    year_count = sum(1 for c in name if c.isdigit() and len([x for x in name.split() if x.isdigit()]) > 1)
    if year_count > 1:
        return True, "multiple_numbers"

    # Concatenated sites (space after period but not ending)
    if ". " in name and not name.endswith("."):
        return True, "likely_concatenated"

    # Repeated words (OCR duplication)
    words = name.lower().split()
    if len(words) > len(set(words)):
        # Has repeated words
        word_freq = {}
        for w in words:
            word_freq[w] = word_freq.get(w, 0) + 1
        repeated = [w for w, c in word_freq.items() if c > 1]
        if any(len(w) > 3 for w in repeated):  # Ignore short repeated words
            return True, "repeated_words"

    # Very long (likely paragraph capture)
    if len(name) > 80:
        return True, "too_long"

    # Contains weird patterns
    if "  " in name:  # Double space
        return True, "double_space"

    if any(pattern in name.lower() for pattern in [
        " and ", " or ", " the ", "archaeological", "prehistoric",
        "historic", "survey", "excavation", "based on", "located"
    ]):
        return True, "descriptive_phrase"

    return False, "ok"


def find_near_duplicates(names: List[str], threshold: float = 0.85) -> List[Tuple[str, str, float]]:
    """Find similar site names using edit distance."""
    duplicates = []
    seen = set()

    for i, name1 in enumerate(names):
        for name2 in names[i + 1:]:
            if name1.lower() == name2.lower():
                continue  # Exact match already handled

            pair_key = tuple(sorted([name1.lower(), name2.lower()]))
            if pair_key in seen:
                continue
            seen.add(pair_key)

            similarity = similarity_ratio(name1, name2)
            if similarity >= threshold:
                duplicates.append((name1, name2, similarity))

    return duplicates


def load_existing_sites() -> Set[str]:
    """Load normalized names from existing CSV."""
    existing = set()
    try:
        with open(EXISTING_FILE, encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                site_name = row.get('SITE_NAME', '').strip().lower()
                if site_name:
                    existing.add(site_name)
    except:
        pass
    return existing


def clean_sites() -> None:
    """Clean and deduplicate high-confidence sites."""
    print("Loading sites...")
    with open(INPUT_FILE, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_sites = list(reader)

    print(f"  Loaded {len(all_sites)} high-confidence sites")

    # Load existing sites for comparison
    existing_sites = load_existing_sites()
    print(f"  Existing sites in merge target: {len(existing_sites)}")

    # Categorize sites
    suspect_sites = []
    clean_sites = []
    suspicious_reasons = {}

    print("\nAnalyzing site quality...")
    for site in all_sites:
        site_name = site.get('site_name', '').strip()
        is_suspect, reason = is_suspicious(site_name)

        if is_suspect:
            suspect_sites.append({**site, "reason": reason})
            suspicious_reasons[reason] = suspicious_reasons.get(reason, 0) + 1
        else:
            clean_sites.append(site)

    print(f"  Clean sites: {len(clean_sites)}")
    print(f"  Suspicious sites: {len(suspect_sites)}")
    print(f"\nSuspicious breakdown:")
    for reason in sorted(suspicious_reasons.keys(), key=lambda x: -suspicious_reasons[x]):
        count = suspicious_reasons[reason]
        pct = 100 * count / len(suspect_sites)
        print(f"    {reason:20s}: {count:4d} ({pct:5.1f}%)")

    # Find near-duplicates in clean sites
    print("\nSearching for near-duplicates (similarity >= 0.85)...")
    clean_names = [s['site_name'].strip() for s in clean_sites]
    near_dups = find_near_duplicates(clean_names, threshold=0.85)

    print(f"  Found {len(near_dups)} near-duplicate pairs")

    # Mark duplicates for manual review
    dup_map = {}  # name -> list of similar names
    for name1, name2, similarity in near_dups:
        if name1 not in dup_map:
            dup_map[name1] = []
        if name2 not in dup_map:
            dup_map[name2] = []
        dup_map[name1].append((name2, similarity))
        dup_map[name2].append((name1, similarity))

    # Deduplicate: keep first occurrence, mark others
    seen_normalized = {}
    dedup_sites = []
    duplicate_records = []

    for site in clean_sites:
        site_name = site['site_name'].strip()
        normalized = site_name.lower()

        if normalized not in seen_normalized:
            dedup_sites.append(site)
            seen_normalized[normalized] = site_name
        else:
            # Duplicate of existing entry
            duplicate_records.append({
                **site,
                "duplicate_of": seen_normalized[normalized],
                "status": "duplicate"
            })

    # Check overlap with existing sites
    new_sites = []
    overlap_sites = []

    for site in dedup_sites:
        site_name = site['site_name'].strip().lower()
        if site_name in existing_sites:
            overlap_sites.append({**site, "status": "overlap_with_existing"})
        else:
            # Mark if has near-dup pair
            if site['site_name'].strip() in dup_map:
                site["similar_names"] = " | ".join([f"{n} ({s:.2f})" for n, s in dup_map[site['site_name'].strip()]])
            new_sites.append(site)

    print(f"\nDeduplication Results:")
    print(f"  Clean, unique sites: {len(dedup_sites)}")
    print(f"  Duplicates within this batch: {len(duplicate_records)}")
    print(f"  Overlapping with existing: {len(overlap_sites)}")
    print(f"  Truly new sites: {len(new_sites)}")

    # Write output files
    print(f"\nWriting output files...")

    # Cleaned sites (no suspicious, no exact duplicates)
    with open(OUTPUT_CLEANED, 'w', encoding='utf-8', newline='') as f:
        if dedup_sites:
            fieldnames = list(all_sites[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            # Clean up any extra fields added during processing
            for row in dedup_sites:
                for key in list(row.keys()):
                    if key not in fieldnames:
                        del row[key]
            writer.writerows(dedup_sites)
    print(f"  ✓ {OUTPUT_CLEANED.name} ({len(dedup_sites)} sites)")

    # Suspicious sites for manual review
    with open(OUTPUT_SUSPICIOUS, 'w', encoding='utf-8', newline='') as f:
        if suspect_sites:
            fieldnames = list(all_sites[0].keys()) + ["reason"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(suspect_sites)
    print(f"  ✓ {OUTPUT_SUSPICIOUS.name} ({len(suspect_sites)} sites)")

    # Duplicates for review
    with open(OUTPUT_DUPLICATES, 'w', encoding='utf-8', newline='') as f:
        if duplicate_records + overlap_sites:
            all_dup = duplicate_records + overlap_sites
            fieldnames = list(all_sites[0].keys()) + ["duplicate_of", "status", "similar_names"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in all_dup:
                for key in fieldnames:
                    if key not in row:
                        row[key] = ""
            writer.writerows(all_dup)
    print(f"  ✓ {OUTPUT_DUPLICATES.name} ({len(duplicate_records) + len(overlap_sites)} records)")

    # Merge-ready: New sites only, no existing overlaps
    with open(OUTPUT_MERGE_READY, 'w', encoding='utf-8', newline='') as f:
        if new_sites:
            fieldnames = list(all_sites[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(new_sites)
    print(f"  ✓ {OUTPUT_MERGE_READY.name} ({len(new_sites)} sites - READY TO MERGE)")

    # Final summary
    print(f"\n{'='*60}")
    print(f"Cleaning Complete")
    print(f"{'='*60}")
    print(f"Input sites:              {len(all_sites):6d}")
    print(f"├─ Suspicious:            {len(suspect_sites):6d} (manual review)")
    print(f"├─ Exact duplicates:      {len(duplicate_records):6d} (within batch)")
    print(f"├─ Overlapping existing:  {len(overlap_sites):6d} (skip - already have)")
    print(f"└─ NEW & CLEAN:           {len(new_sites):6d} ⭐ MERGE READY")
    print(f"{'='*60}")

    # Show sample of new sites
    print(f"\nSample NEW sites ready for merge:")
    for site in new_sites[:10]:
        print(f"  ✓ {site['site_name']}")

    if len(new_sites) > 10:
        print(f"  ... and {len(new_sites) - 10} more")

    # Show sample of suspicious for manual review
    if suspect_sites:
        print(f"\nSample SUSPICIOUS sites (needs review):")
        for site in suspect_sites[:5]:
            reason = site.get('reason', 'unknown')
            print(f"  ⚠ {site['site_name'][:50]:50s} [{reason}]")
        if len(suspect_sites) > 5:
            print(f"  ... and {len(suspect_sites) - 5} more in {OUTPUT_SUSPICIOUS.name}")


if __name__ == "__main__":
    clean_sites()
