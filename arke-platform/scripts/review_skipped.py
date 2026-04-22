#!/usr/bin/env python3
import csv

INPUT_FILE = "/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/drive-sites-to-arkeogis-fixed.csv"

def clean_coord(value):
    if not value or str(value).strip() == '':
        return None
    value = str(value).replace(",", ".").strip()
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

skipped_no_id = []
skipped_no_coords = []
processed = []

with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter=';')
    reader.fieldnames = [fn.strip() if fn else fn for fn in reader.fieldnames]
    
    for i, row in enumerate(reader, start=1):
        row = {k.strip() if k else k: (v.strip() if v else v) for k, v in row.items()}
        source_id = str(row.get("SITE_SOURCE_ID", ""))
        
        if not source_id:
            skipped_no_id.append((i, row.get("SITE_NAME", "N/A")))
            continue
        
        lat = clean_coord(row.get("LATITUDE"))
        lon = clean_coord(row.get("LONGITUDE"))
        
        if lat is None or lon is None:
            skipped_no_coords.append((i, source_id, row.get("SITE_NAME", "N/A"), 
                                     row.get("LATITUDE", ""), row.get("LONGITUDE", "")))
        else:
            processed.append((i, source_id))

print(f"=== REVIEW SUMMARY ===")
print(f"Total rows: {len(skipped_no_id) + len(skipped_no_coords) + len(processed)}")
print(f"Processed: {len(processed)}")
print(f"Skipped (no ID): {len(skipped_no_id)}")
print(f"Skipped (no coords): {len(skipped_no_coords)}")

if skipped_no_id:
    print(f"\n=== Skipped - No SITE_SOURCE_ID ({len(skipped_no_id)}) ===")
    for line, name in skipped_no_id[:10]:
        print(f"  Line {line}: {name}")
    if len(skipped_no_id) > 10:
        print(f"  ... and {len(skipped_no_id) - 10} more")

if skipped_no_coords:
    print(f"\n=== Skipped - Missing Coordinates ({len(skipped_no_coords)}) ===")
    for line, sid, name, lat, lon in skipped_no_coords[:20]:
        print(f"  Line {line}: {sid} | {name}")
        print(f"           LAT: '{lat}' | LON: '{lon}'")
    if len(skipped_no_coords) > 20:
        print(f"  ... and {len(skipped_no_coords) - 20} more")

print(f"\n=== First 5 Processed ===")
for line, sid in processed[:5]:
    print(f"  Line {line}: {sid}")
