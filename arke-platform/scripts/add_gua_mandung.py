import csv

# Read existing data
with open('/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-combined.csv','r') as f:
    r = csv.DictReader(f, delimiter=';')
    flds = r.fieldnames
    rows = list(r)

# New site data
new_row = {
    'SITE_SOURCE_ID': 'IPAD_961',
    'SITE_NAME': 'Gua Mandung',
    'LOCALISATION': 'Java | DI Yogyakarta',
    'GEONAME_ID': '',
    'PROJECTION_SYSTEM': '4326',
    'LONGITUDE': '110.56',
    'LATITUDE': '-8.08',
    'ALTITUDE': '',
    'CITY_CENTROID': 'No',
    'STATE_OF_KNOWLEDGE': 'Documented',
    'OCCUPATION': 'Not specified',
    'STARTING_PERIOD': '-11073',
    'ENDING_PERIOD': '-260',
    'MAIN_CHARAC': 'Archaeological Sites',
    'CHARAC_LVL1': 'Cave / Gua / Liang / Ceruk',
    'CHARAC_LVL2': '',
    'CHARAC_LVL3': '',
    'CHARAC_LVL4': '',
    'CHARAC_EXP': 'No',
    'BIBLIOGRAPHY': 'Kaharudin, H. A. F., Ananda, G. A. R., Prasetya, W. H., Wibisono, M. W., & Yuwono, J. S. E. (2023). Hunter-gatherers in labyrinth karst: An Early Holocene record from Gunung Sewu, Java. Archaeological Research in Asia, 33, 100427. https://doi.org/10.1016/j.ara.2022.100427',
    'COMMENTS': 'The extensive Early Holocene (11,073 cal BP) through to late Holocene occupation record recovered from Gua Mandung confirms its previously suspected archaeological potential. This assemblage is dominated by terrestrial fauna remains and technology indicating an intensive inland subsistence strategy. With a focus on hunting cercopithecids as well as other terrestrial fauna, the early inhabitants of Gua Mandung also used the bones of these animals to make tools including bone points, needles, and spatulas. Such trends in subsistence and tool manufacture reflect other early occupation sites in the region. start 11073 calBP - end 260calBP.'
}

rows.append(new_row)

# Write back
with open('/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-combined.csv','w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=flds, delimiter=';')
    w.writeheader()
    w.writerows(rows)

print(f'Added IPAD_961: Gua Mandung to combined CSV')
print(f'Total rows now: {len(rows)}')
