# Google Drive Inventory

This is the first stage of the PDF research pipeline.

It does **not** download or extract PDFs yet. It only inventories a Google Drive folder recursively into CSV so batches can be chosen safely.

## Setup

1. Enable the Google Drive API in a Google Cloud project.
2. Create an OAuth client of type **Desktop app**.
3. Download the OAuth client JSON.
4. Save it here:

`/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-oauth-client.json`

5. Install Python dependencies:

```bash
python3 -m pip install -r requirements-google-drive.txt
```

## Run

You can pass either a folder id or a shared folder link:

```bash
python3 ./scripts/google-drive-inventory.py 'https://drive.google.com/drive/folders/1Gvb4TRuzuiu8s1jMd-5NY4dPmGdxXdEI?usp=sharing'
```

Or via npm:

```bash
npm run drive:inventory -- 'https://drive.google.com/drive/folders/1Gvb4TRuzuiu8s1jMd-5NY4dPmGdxXdEI?usp=sharing'
```

## Output

The script writes:

`/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-inventory.csv`

Columns:

- `path`
- `name`
- `id`
- `target_id`
- `mime_type`
- `resource_key`
- `is_folder`
- `is_pdf`
- `size`
- `modified_time`
- `web_view_link`

Boolean-style flags are written as `yes` or `no`.

The first run opens a browser for OAuth and stores a reusable token here:

`/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-token.json`

## Notes

- The script uses `drive.metadata.readonly`.
- It supports shared drives and recursive folder listing.
- This stage is intentionally limited to inventory only. Download and extraction should be done in curated batches after reviewing the CSV.
