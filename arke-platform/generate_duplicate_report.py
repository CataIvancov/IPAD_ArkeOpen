import pandas as pd
from itertools import combinations

v3 = pd.read_csv('data/airtable-to-arkeogis-v3-site-only.csv', sep=';')
v4 = pd.read_csv('data/airtable-to-arkeogis-v4-site-only.csv', sep=';')
v3.columns = [c.strip() for c in v3.columns]
v4.columns = [c.strip() for c in v4.columns]

rows = []

def add_row(candidate_type, dataset, sid1, name1, lon1, lat1, sid2, name2, lon2, lat2, notes):
    rows.append({
        'candidate_type': candidate_type,
        'dataset': dataset,
        'SITE_SOURCE_ID_1': sid1,
        'SITE_NAME_1': name1,
        'LONGITUDE_1': lon1,
        'LATITUDE_1': lat1,
        'SITE_SOURCE_ID_2': sid2,
        'SITE_NAME_2': name2,
        'LONGITUDE_2': lon2,
        'LATITUDE_2': lat2,
        'notes': notes,
    })

for label, df in [('V3', v3), ('V4', v4)]:
    dup_names = df[df.duplicated(subset=['SITE_NAME'], keep=False)].sort_values(['SITE_NAME', 'SITE_SOURCE_ID'])
    for name, group in dup_names.groupby('SITE_NAME'):
        records = group.to_dict('records')
        for a, b in combinations(records, 2):
            add_row(
                'same_name_within_dataset',
                label,
                a['SITE_SOURCE_ID'],
                a['SITE_NAME'],
                a.get('LONGITUDE'),
                a.get('LATITUDE'),
                b['SITE_SOURCE_ID'],
                b['SITE_NAME'],
                b.get('LONGITUDE'),
                b.get('LATITUDE'),
                'Same SITE_NAME within dataset',
            )

for label, df in [('V3', v3), ('V4', v4)]:
    dup_coords = df[df.duplicated(subset=['LONGITUDE', 'LATITUDE'], keep=False)].sort_values(['LONGITUDE', 'LATITUDE', 'SITE_SOURCE_ID'])
    for (lon, lat), group in dup_coords.groupby(['LONGITUDE', 'LATITUDE']):
        if pd.isna(lon) or pd.isna(lat):
            continue
        records = group.to_dict('records')
        for a, b in combinations(records, 2):
            if a['SITE_SOURCE_ID'] == b['SITE_SOURCE_ID']:
                continue
            add_row(
                'same_coords_within_dataset',
                label,
                a['SITE_SOURCE_ID'],
                a['SITE_NAME'],
                a['LONGITUDE'],
                a['LATITUDE'],
                b['SITE_SOURCE_ID'],
                b['SITE_NAME'],
                b['LONGITUDE'],
                b['LATITUDE'],
                'Same coordinates within dataset',
            )

v3coords = v3[['SITE_SOURCE_ID', 'SITE_NAME', 'LONGITUDE', 'LATITUDE']].dropna(subset=['LONGITUDE', 'LATITUDE'])
v4coords = v4[['SITE_SOURCE_ID', 'SITE_NAME', 'LONGITUDE', 'LATITUDE']].dropna(subset=['LONGITUDE', 'LATITUDE'])
merged_coords = pd.merge(v3coords, v4coords, on=['LONGITUDE', 'LATITUDE'], suffixes=('_v3', '_v4'))
for _, r in merged_coords.iterrows():
    add_row(
        'cross_dataset_same_coords',
        'V3+V4',
        r['SITE_SOURCE_ID_v3'],
        r['SITE_NAME_v3'],
        r['LONGITUDE'],
        r['LATITUDE'],
        r['SITE_SOURCE_ID_v4'],
        r['SITE_NAME_v4'],
        r['LONGITUDE'],
        r['LATITUDE'],
        'Same coordinates across datasets',
    )

merged_name = pd.merge(v3[['SITE_SOURCE_ID', 'SITE_NAME']], v4[['SITE_SOURCE_ID', 'SITE_NAME']], on='SITE_NAME', suffixes=('_v3', '_v4'))
for _, r in merged_name.iterrows():
    if r['SITE_SOURCE_ID_v3'] != r['SITE_SOURCE_ID_v4']:
        add_row(
            'cross_dataset_same_name_diff_id',
            'V3+V4',
            r['SITE_SOURCE_ID_v3'],
            r['SITE_NAME'],
            None,
            None,
            r['SITE_SOURCE_ID_v4'],
            r['SITE_NAME'],
            None,
            None,
            'Same name, different IDs across datasets',
        )

report = pd.DataFrame(rows)
report.to_csv('data/duplicate-site-candidates.csv', index=False)
print(f'Generated data/duplicate-site-candidates.csv with {len(report)} rows')
