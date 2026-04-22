#!/usr/bin/env python3
import csv
import re

input_file = '/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/drive-sites-to-arkeogis-fixed.csv'

# Read with proper CSV parsing
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    rows = list(reader)

print(f"Read {len(rows)} rows")

# Fix newlines within cells and excessive quotes
for row in rows:
    for i in range(len(row)):
        # Replace newlines/tabs with spaces
        row[i] = row[i].replace('\n', ' ').replace('\r', ' ')
        # Replace multiple spaces with single space
        row[i] = re.sub(r' +', ' ', row[i])
        # Fix excessive quotes
        row[i] = re.sub(r'"{3,}', '""', row[i])

# Write back
with open(input_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows")
