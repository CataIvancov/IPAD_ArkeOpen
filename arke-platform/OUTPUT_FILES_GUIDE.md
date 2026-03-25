# OCR Pipeline - Output Files Quick Reference

## Main Output Files (Ready for Use)

### 1. High-Confidence Sites (PRIMARY)
**File**: `site-geolocation-candidates-highconf.csv`
- **Records**: 5,838 validated archaeological sites
- **Columns**: site_name, latitude, longitude, coordinate_text, coordinate_system, source_file, page, evidence_snippet
- **Purpose**: Clean list of confirmed site names for import to ArkeOGIS
- **Quality**: Ultra-conservative filtering (pattern-matched, good OCR source)

### 2. Sites with Coordinates
**File**: `site-coordinates-extracted.csv`
- **Records**: 5,838 sites (same as above) with coordinate enrichment
- **Additional Columns**: coord_precision (precise/approximate/none)
- **Coordinates**: 577 precise + 44 approximate
- **Purpose**: Geographic data for mapping and spatial analysis

### 3. Sites with Bibliography
**File**: `site-geolocation-with-bibliography.csv`
- **Records**: 5,838 sites with scholarly citations
- **Additional Column**: BIBLIOGRAPHY (pipe-separated DOIs/URLs)
- **Coverage**: 4,591 sites with references (78.6%)
- **Purpose**: Cite sources and link to published research

### 4. Bibliography Lookup Table
**File**: `site-bibliography-sources.csv`
- **Records**: 36,915 site-to-DOI associations
- **Columns**: site_name, doi, source_url
- **Purpose**: Look up all references for a given site or find sites for a DOI
- **Example**: "Leang Buida" → ["https://doi.org/10.1146/...", "https://doi.org/10.1016/..."]

### 5. Updated Text Inventory
**File**: `google-drive-text-inventory.csv`
- **Records**: 586 OCR'd PDFs with metadata
- **Columns**: filename, pdf_path, pages, text_chars, ocr_confidence, date_processed
- **Purpose**: Track which PDFs have been processed and their quality
- **Statistics**: 565 successful, 21 failed

---

## Intermediate Files (Reference/Analysis)

### Conservative Filter (15,554 sites)
**File**: `site-geolocation-candidates-filtered.csv`
- Addresses basic false positives only
- Kept 94.7% of original candidates
- Use if needing broader coverage, lower precision

### Rejected Sites (877 records)
**File**: `site-geolocation-candidates-rejected.csv`
- Contains filtered-out entries with rejection reasons
- Useful for understanding false positive patterns
- Can be manually reviewed for recovery of valid sites

---

## How to Use These Files

### For ArkeOGIS Import
1. Start with: `site-geolocation-candidates-highconf.csv`
2. Map columns to ArkeOGIS schema: site_name → SITE_NAME, etc.
3. Enrich with: `site-geolocation-with-bibliography.csv` for BIBLIOGRAPHY field
4. Manually add: Coordinates from `site-coordinates-extracted.csv` (supplement those with data)

### For Literature Review
1. Load: `site-bibliography-sources.csv`
2. Filter by site name or DOI
3. Resolve DOIs via https://doi.org/[DOI_HERE]

### For Coordinate Verification
1. Load: `site-coordinates-extracted.csv`
2. Filter by: coord_precision = "precise" (confidence 577 sites)
3. Review: Sites marked "approximate" (44 using region centers)
4. Flag: Sites with coord_precision = "none" for manual geolocation

### For PDFs Not Yet Processed
1. Check: `google-drive-text-inventory.csv`
2. If text_chars = 0 → PDF needs retry or manual transcription
3. See Phase 1 in PIPELINE_COMPLETION_SUMMARY.md

---

## Data Quality Summary by File

| File | Records | Quality | Completeness |
|------|---------|---------|---------------|
| site-geolocation-candidates-highconf.csv | 5,838 | 🟢 High | 100% site names |
| site-coordinates-extracted.csv | 5,838 | 🟡 Medium | 10.6% coordinates |
| site-geolocation-with-bibliography.csv | 5,838 | 🟢 High | 78.6% bibliography |
| site-bibliography-sources.csv | 36,915 | 🟢 High | 1,868 unique DOIs |
| google-drive-text-inventory.csv | 586 | 🟢 High | 100% OCR metadata |

Legend: 🟢 High (>75%), 🟡 Medium (25-75%), 🔴 Low (<25%)

---

## Example Usage Queries

### "Find all sites from Talaud Islands with coordinates"
```bash
grep "Talaud\|Leang Buida\|Ceruk" site-coordinates-extracted.csv | \
  awk -F, '$2 != "" && $3 != "" {print}'
```

### "Get bibliography for Leang Buida"
```bash
grep "Leang Buida" site-bibliography-sources.csv
```

### "Count sites by region"
```bash
cut -d, -f1 site-geolocation-candidates-highconf.csv | \
  grep "Gua\|Leang\|Ceruk" | wc -l
```

### "Find sites with precise coordinates"
```bash
grep "precise" site-coordinates-extracted.csv | wc -l
```

---

## CSV Column Reference

### site-geolocation-candidates-highconf.csv
| Column | Example | Notes |
|--------|---------|-------|
| site_name | "Leang Buida" | Cleaned, validated name |
| latitude | (empty) | See site-coordinates-extracted.csv |
| longitude | (empty) | See site-coordinates-extracted.csv |
| coordinate_text | "Talaud Islands" | Original coordinate reference |
| coordinate_system | "" | GPS system (usually WGS84) |
| source_file | "data/...pdf" | Original PDF path |
| page | "2" | Page number in PDF |
| evidence_snippet | "...occupancy sites..." | Context from OCR text |

### site-coordinates-extracted.csv
(All above, PLUS:)

| Column | Values | Meaning |
|--------|--------|---------|
| latitude | "-8.531" | Decimal degrees (negative = South) |
| longitude | "120.444" | Decimal degrees (negative = West) |
| coord_precision | "precise" / "approximate" / "none" | Extraction confidence |

### site-geolocation-with-bibliography.csv
(All above, PLUS:)

| Column | Example | Notes |
|--------|---------|-------|
| BIBLIOGRAPHY | "https://doi.org/10.1016/j.jasrep.2021.103199 \| https://..." | Pipe-separated DOIs/URLs |

### google-drive-text-inventory.csv
| Column | Type | Range | Notes |
|--------|------|-------|-------|
| filename | string | - | PDF filename without extension |
| pdf_path | path | - | Full path to source PDF |
| pages | int | 1-226+ | Number of pages OCR'd |
| text_chars | int | 0-503K+ | Characters extracted (0 = failed) |
| ocr_confidence | float | 0.0-100.0 | Tesseract confidence score |
| date_processed | ISO datetime | - | When OCR was performed |

---

## Tips for Data Cleaning

1. **Remove Duplicates**: Some sites appear multiple times (e.g., "Ceruk Kucing 1" and "Ceruk Kucing 2")
   - Consolidate variants before ArkeOGIS import

2. **Standardize Names**: Indonesian spelling variants (Gua/Leang/Ceruk)
   - Already normalized in high-confidence file

3. **Verify Coordinates**: Manual spot-check for outliers
   - Any coordinate outside Indonesia+Timor-Leste needs review

4. **Enrich Rare Sites**: Sites without bibliography may deserve manual research
   - 1,247 sites (21.4%) missing references

---

## Support / Questions

For issues with data quality or processing:
1. See PIPELINE_COMPLETION_SUMMARY.md for detailed methodology
2. Check individual script docstrings (scripts/*.py)
3. Review rejection reasons in site-geolocation-candidates-rejected.csv
