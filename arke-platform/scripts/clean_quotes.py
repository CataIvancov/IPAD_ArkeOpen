#!/usr/bin/env python3
"""Clean up multiple quote marks in COMMENTS field of CSV file."""

import re

input_file = '/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/drive-sites-to-arkeogis-fixed.csv'
output_file = '/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/drive-sites-to-arkeogis-fixed.csv.tmp'

with open(input_file, 'r', encoding='utf-8') as infile, \
     open(output_file, 'w', encoding='utf-8') as outfile:
    
    for line in infile:
        # Split by semicolon to get fields
        parts = line.rstrip('\n').rsplit(';', 1)  # Split only on last semicolon
        
        if len(parts) == 2:
            prefix, comments = parts
            
            # Strip trailing space from prefix
            prefix = prefix.rstrip()
            
            # Replace 4 or more consecutive quotes with a single quote
            # This handles the """" pattern (4 quotes)
            comments = re.sub(r'"{4,}', '"', comments)
            
            # Replace triple quotes with single quotes
            comments = re.sub(r'"{3}', '"', comments)
            
            # Replace patterns like " " (quote-space-quote) with just a space
            comments = re.sub(r'"\s+"', ' ', comments)
            
            # Remove all single quotes
            comments = comments.replace('"', '')
            
            # Remove leading/trailing spaces
            comments = comments.strip()
            
            outfile.write(prefix + ';' + comments + '\n')
        else:
            # Line doesn't have a semicolon, write as-is
            outfile.write(line)

# Replace original file with cleaned version
import shutil
shutil.move(output_file, input_file)

print("Cleaned up quote marks in COMMENTS field")
