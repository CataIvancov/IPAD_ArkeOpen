import pandas as pd

# Load the CSV files
v3_df = pd.read_csv('/Users/cataivancov/IdeaProjects/arke-platform/data/airtable-to-arkeogis-v3-site-only.csv', sep=';')
v4_df = pd.read_csv('/Users/cataivancov/IdeaProjects/arke-platform/data/airtable-to-arkeogis-v4-site-only.csv', sep=';')

print("V3 dataset shape:", v3_df.shape)
print("V4 dataset shape:", v4_df.shape)

# Check for duplicates within V3 based on SITE_NAME
v3_duplicates = v3_df[v3_df.duplicated(subset=['SITE_NAME'], keep=False)]
print("\nDuplicates in V3 by SITE_NAME:")
if not v3_duplicates.empty:
    print(v3_duplicates[['SITE_SOURCE_ID', 'SITE_NAME', 'LOCALISATION']].to_string())
else:
    print("No duplicates found.")

# Check for duplicates within V4 based on SITE_NAME
v4_duplicates = v4_df[v4_df.duplicated(subset=['SITE_NAME'], keep=False)]
print("\nDuplicates in V4 by SITE_NAME:")
if not v4_duplicates.empty:
    print(v4_duplicates[['SITE_SOURCE_ID', 'SITE_NAME', 'LOCALISATION']].to_string())
else:
    print("No duplicates found.")

# Check for duplicates between V3 and V4 based on SITE_NAME
merged = pd.merge(v3_df, v4_df, on='SITE_NAME', suffixes=('_v3', '_v4'))
print("\nSites present in both V3 and V4:")
if not merged.empty:
    print(merged[['SITE_NAME', 'SITE_SOURCE_ID_v3', 'SITE_SOURCE_ID_v4']].to_string())
else:
    print("No common sites found.")

# Check for potential duplicates based on coordinates (LONGITUDE, LATITUDE)
# First, within V3
v3_coord_duplicates = v3_df[v3_df.duplicated(subset=['LONGITUDE', 'LATITUDE'], keep=False)]
print("\nPotential duplicates in V3 by coordinates:")
if not v3_coord_duplicates.empty:
    print(v3_coord_duplicates[['SITE_SOURCE_ID', 'SITE_NAME', 'LONGITUDE', 'LATITUDE']].to_string())
else:
    print("No duplicates found.")

# Within V4
v4_coord_duplicates = v4_df[v4_df.duplicated(subset=['LONGITUDE', 'LATITUDE'], keep=False)]
print("\nPotential duplicates in V4 by coordinates:")
if not v4_coord_duplicates.empty:
    print(v4_coord_duplicates[['SITE_SOURCE_ID', 'SITE_NAME', 'LONGITUDE', 'LATITUDE']].to_string())
else:
    print("No duplicates found.")

# Between V3 and V4 by coordinates
coord_merged = pd.merge(v3_df, v4_df, on=['LONGITUDE', 'LATITUDE'], suffixes=('_v3', '_v4'))
print("\nSites with same coordinates in both datasets:")
if not coord_merged.empty:
    print(coord_merged[['SITE_NAME_v3', 'SITE_NAME_v4', 'LONGITUDE', 'LATITUDE']].to_string())
else:
    print("No sites with same coordinates found.")