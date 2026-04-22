#!/usr/bin/env python3
# ==============================
# Archaeological Dataset Pipeline (ARK-READY, DETERMINISTIC IDs)
# ==============================

import csv
import json
import hashlib
from pathlib import Path

INPUT_FILE = "/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/drive-sites-to-arkeogis-fixed.csv"
OUTPUT_FILE = "/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/sites_ark_ready.json"

# ------------------------------
# CONFIG (NAAN placeholder)
# ------------------------------
NAAN = "11633"  # replace with real NAAN when ready
SHOULDER = "s1"

# ------------------------------
# Deterministic ID generator (ARK-ready)
# ------------------------------

def generate_stable_id(source_id: str):
    """Generate 10-char stable ID from source_id using MD5 hash."""
    base = hashlib.md5(source_id.encode()).hexdigest()[:10]
    return f"{SHOULDER}{base}"


def generate_ark_id(stable_id: str):
    """Generate full ARK identifier."""
    return f"ark:/{NAAN}/{stable_id}"

# ------------------------------
# Helpers
# ------------------------------

def clean_coord(value):
    """Clean coordinate value, handle comma decimals."""
    if not value or str(value).strip() == '':
        return None
    value = str(value).replace(",", ".").strip()
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def clean_date(value):
    """Clean date value."""
    if not value or str(value).strip() == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ------------------------------
# Transform
# ------------------------------

def transform():
    results = []
    skipped = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        
        # Strip whitespace from column names
        reader.fieldnames = [fn.strip() if fn else fn for fn in reader.fieldnames]
        
        for i, row in enumerate(reader, start=1):
            # Strip whitespace from all values
            row = {k.strip() if k else k: (v.strip() if v else v) for k, v in row.items()}
            source_id = str(row.get("SITE_SOURCE_ID", ""))
            
            if not source_id:
                skipped += 1
                continue
            
            # Clean coordinates
            lat = clean_coord(row.get("LATITUDE"))
            lon = clean_coord(row.get("LONGITUDE"))
            
            if lat is None or lon is None:
                skipped += 1
                continue
            
            # Clean dates
            start_date = clean_date(row.get("STARTING_PERIOD"))
            end_date = clean_date(row.get("ENDING_PERIOD"))
            
            # Generate stable/ARK IDs
            stable_id = generate_stable_id(source_id)
            ark_id = generate_ark_id(stable_id)
            
            # Build record
            site = {
                "id": stable_id,
                "ark_id": ark_id,
                "source_id": source_id,
                "name": row.get("SITE_NAME", "").strip(),
                "localisation": row.get("LOCALISATION", "").strip(),
                "latitude": lat,
                "longitude": lon,
                "altitude": clean_coord(row.get("ALTITUDE")),
                "start_date": start_date,
                "end_date": end_date,
                "starting_period": row.get("STARTING_PERIOD", "").strip() if start_date else None,
                "ending_period": row.get("ENDING_PERIOD", "").strip() if end_date else None,
                "state_of_knowledge": row.get("STATE_OF_KNOWLEDGE", "").strip(),
                "occupation": row.get("OCCUPATION", "").strip(),
                "main_charac": row.get("MAIN_CHARAC", "").strip(),
                "charac_lvl1": row.get("CHARAC_LVL1", "").strip(),
                "bibliography": row.get("BIBLIOGRAPHY", "").strip(),
                "comments": row.get("COMMENTS", "").strip()
            }
            
            results.append(site)
    
    # Write JSON output
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Generated {len(results)} ARK-ready records")
    print(f"✓ Skipped {skipped} rows (missing coords or ID)")
    print(f"✓ Output: {OUTPUT_FILE}")
    
    # Show sample
    if results:
        print(f"\nSample record:")
        print(f"  Source ID: {results[0]['source_id']}")
        print(f"  Stable ID: {results[0]['id']}")
        print(f"  ARK ID: {results[0]['ark_id']}")


if __name__ == "__main__":
    transform()
