#!/usr/bin/env python3

with open('/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-combined.csv', 'r') as f:
    content = f.read()

lines = content.split('\n')
for i, line in enumerate(lines):
    if line.startswith('IPAD_089;'):
        parts = line.split(';')
        print(f"Found line {i+1}, {len(parts)} parts")
        
        # Join the split comment text with pipe
        comment = parts[15] + '|' + parts[16]
        
        # Rebuild line: keep first 15 parts, empty CHARAC_LVL2/3/4, set CHARAC_EXP=No, 
        # move URLs to BIBLIO, put comment in COMMENTS
        new_parts = parts[:15] + ['', '', '', 'No', parts[20], comment]
        new_line = ';'.join(new_parts)
        lines[i] = new_line
        print("Fixed line")
        print(f"  BIBLIO: {new_parts[19][:60]}...")
        print(f"  COMMENTS: {new_parts[20][:60]}...")
        break

with open('/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-combined.csv', 'w') as f:
    f.write('\n'.join(lines))
    
print("Done")
