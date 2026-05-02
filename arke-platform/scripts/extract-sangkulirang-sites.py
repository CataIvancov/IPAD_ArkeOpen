#!/usr/bin/env python3
"""Extract Sangkulirang-Mangkalihat sites from ipad-sites-combined.csv"""

import csv

input_file = '/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-combined.csv'
output_file = '/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-sangkulirang-mangkalihat.csv'

with open(input_file, 'r', encoding='utf-8') as infile, \
     open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    
    reader = csv.DictReader(infile, delimiter=';')
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=';')
    writer.writeheader()
    
    count = 0
    for row in reader:
        if 'Sangkulirang-Mangkalihat' in row.get('LOCALISATION', ''):
            writer.writerow(row)
            count += 1

print(f"Extracted {count} Sangkulirang-Mangkalihat sites to {output_file}")
