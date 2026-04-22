#!/usr/bin/env python3
"""Fix inconsistent double quotes in CSV file."""

import csv
import re

def normalize_quotes(value):
    """Normalize excessive double quotes in a field value."""
    if not value:
        return value
    
    # Pattern: """ """"""...""" at the beginning - replace with single quote
    # This handles cases like: """ """"""""Description: ...
    value = re.sub(r'^""+\s*"+', '"', value)
    
    # Pattern: """ at the end - replace with single quote
    value = re.sub(r'""+$', '"', value)
    
    # Pattern: multiple consecutive quotes in the middle -> single quote
    # But preserve escaped quotes ("") which are valid in CSV
    value = re.sub(r'"{3,}', '""', value)
    
    # Clean up orphaned quotes
    if value.count('"') % 2 != 0:
        # Odd number of quotes - remove trailing/leading if single
        value = value.strip('"')
    
    return value

def process_csv(input_file, output_file):
    """Process the CSV and fix quoting issues."""
    with open(input_file, 'r', encoding='utf-8') as f:
        # Read all lines and process manually since the CSV has inconsistent quoting
        lines = f.readlines()
    
    fixed_lines = []
    for line in lines:
        # Split by semicolon but respect quotes
        fields = []
        current_field = []
        in_quotes = False
        i = 0
        while i < len(line):
            char = line[i]
            if char == '"':
                in_quotes = not in_quotes
                current_field.append(char)
            elif char == ';' and not in_quotes:
                fields.append(''.join(current_field))
                current_field = []
            else:
                current_field.append(char)
            i += 1
        # Add last field
        fields.append(''.join(current_field))
        
        # Normalize quotes in each field
        fixed_fields = []
        for field in fields:
            fixed = normalize_quotes(field.strip())
            # If field contains semicolon or newline, ensure it's properly quoted
            if ';' in fixed or '\n' in fixed or '\r' in fixed:
                if not (fixed.startswith('"') and fixed.endswith('"')):
                    fixed = '"' + fixed + '"'
            fixed_fields.append(fixed)
        
        fixed_line = ';'.join(fixed_fields)
        fixed_lines.append(fixed_line)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(fixed_lines)

if __name__ == '__main__':
    input_file = '/Users/cataivancov/.windsurf/worktrees/IdeaProjects/IdeaProjects-763283e2/arke-platform/data/drive-sites-to-arkeogis-fixed.csv'
    output_file = input_file
    process_csv(input_file, output_file)
    print("Quote normalization complete.")
