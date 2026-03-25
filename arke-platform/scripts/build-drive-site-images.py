#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import logging
import os
import re
import shutil
from collections import Counter, OrderedDict, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

from pypdf import PdfReader

logging.getLogger("pypdf").setLevel(logging.ERROR)

ROOT = Path(__file__).resolve().parents[1]
SITES_CSV = ROOT / "data" / "drive-sites-to-arkeogis.csv"
CANDIDATES_CSV = ROOT / "data" / "site-geolocation-candidates.csv"
TEXT_ROOT = ROOT / "data" / "google-drive-text"
OUTPUT_CSV = ROOT / "data" / "drive-site-images-web-images.csv"
OUTPUT_REPORT = ROOT / "data" / "drive-site-images-report.txt"
DETAIL_CSV = ROOT / "data" / "drive-site-images-detail.csv"
APP_PUBLIC_ROOT = ROOT.parent / "arkeopen-upstream" / "web-app" / "public"
PUBLIC_IMAGE_ROOT = APP_PUBLIC_ROOT / "assets" / "drive-site-images"
MANIFEST_ROOT = APP_PUBLIC_ROOT / "iiif" / "manifests" / "sites"
MANIFEST_BASE_URL = os.environ.get("IIIF_MANIFEST_BASE_URL", "/iiif/manifests/sites").rstrip("/")
IIIF_IMAGE_SERVICE_BASE_URL = os.environ.get("IIIF_IMAGE_SERVICE_BASE_URL", "/iiif/3").rstrip("/")
PUBLIC_IMAGE_URL_BASE = os.environ.get("PUBLIC_IMAGE_URL_BASE", "/assets/drive-site-images").rstrip("/")
MAX_PDFS_PER_SITE = 6
MAX_PAGES_PER_PDF = 6
MAX_IMAGES_PER_SITE = 6

OUTPUT_HEADERS = [
    "SITE_SOURCE_ID",
    "SITE_NAME",
    "WEB_IMAGES",
    "IMAGE_COUNT",
    "SOURCE_PDFS",
]

DETAIL_HEADERS = [
    "SITE_SOURCE_ID",
    "SITE_NAME",
    "PDF_FILE",
    "PAGE",
    "IMAGE_NAME",
    "IMAGE_IDENTIFIER",
    "IMAGE_URL",
    "WIDTH",
    "HEIGHT",
    "FORMAT",
    "SCORE",
    "REASONS",
    "MANIFEST_URL",
]

TOKEN_SPLIT_REGEX = re.compile(r"[^a-z0-9]+")
INDONESIA_HINTS = (
    "indonesia",
    "sumatra",
    "java",
    "jawa",
    "sulawesi",
    "flores",
    "timor",
    "maluku",
    "papua",
    "kalimantan",
    "bali",
    "halmahera",
    "seram",
    "maros",
)
POSITIVE_PAGE_TERMS = (
    "figure",
    "fig.",
    "photo",
    "photograph",
    "plate",
    "illustration",
    "excavation",
    "site",
    "cave",
    "shelter",
    "rock art",
    "stratigraphy",
)
NEGATIVE_PAGE_TERMS = (
    "table",
    "appendix",
    "references",
    "bibliography",
    "acknowledg",
    "contents",
)
MAP_TERMS = ("map", "location", "survey area")


@dataclass
class CandidateImage:
    pdf_path: Path
    page_num: int
    image_index: int
    image_name: str
    width: int
    height: int
    image_format: str
    score: int
    reasons: list[str]
    image_obj: object


@dataclass
class SavedImage:
    filename: str
    identifier: str
    static_url: str
    manifest_body_url: str
    service_url: str
    width: int
    height: int
    image_format: str
    candidate: CandidateImage


def normalize_text(value: str) -> str:
    return " ".join(TOKEN_SPLIT_REGEX.split((value or "").lower())).strip()


def normalize_name(value: str) -> str:
    return normalize_text(value)


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return slug or "site"


def clean_output_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def load_sites() -> list[dict[str, str]]:
    with SITES_CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def load_page_text_lookup() -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    for path in sorted(TEXT_ROOT.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        source_pdf = Path(payload.get("source_pdf", "")).name
        page_texts = payload.get("page_texts") or []
        if source_pdf and isinstance(page_texts, list):
            lookup[source_pdf] = [str(text or "") for text in page_texts]
    return lookup


def load_candidate_pages() -> dict[str, dict[str, Counter]]:
    site_sources: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    with CANDIDATES_CSV.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            site_name = normalize_name(row.get("site_name", ""))
            source_file = row.get("source_file", "").strip()
            page = row.get("page", "").strip()
            if not site_name or not source_file or not page.isdigit():
                continue
            pdf_name = Path(source_file).name
            site_sources[site_name][pdf_name][int(page)] += 1
    return site_sources


def get_locality_terms(site_row: dict[str, str]) -> list[str]:
    locality = normalize_text(site_row.get("LOCALISATION", ""))
    return [token for token in locality.split() if len(token) > 3]


def score_page_text(site_name: str, page_text: str, locality_terms: Iterable[str]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    normalized_page = normalize_text(page_text)
    if not normalized_page:
        return score, reasons

    if site_name and site_name in normalized_page:
        score += 18
        reasons.append("site-name-on-page")

    matched_locality = [term for term in locality_terms if term in normalized_page]
    if matched_locality:
        score += min(8, len(matched_locality) * 2)
        reasons.append("locality-context")

    if any(term in normalized_page for term in INDONESIA_HINTS):
        score += 4
        reasons.append("indonesia-context")

    if any(term in normalized_page for term in POSITIVE_PAGE_TERMS):
        score += 6
        reasons.append("figure-photo-context")

    if any(term in normalized_page for term in MAP_TERMS):
        score += 2
        reasons.append("map-context")

    if any(term in normalized_page for term in NEGATIVE_PAGE_TERMS):
        score -= 6
        reasons.append("negative-backmatter-context")

    return score, reasons


def score_image_dimensions(width: int, height: int) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    area = width * height
    short_side = min(width, height)
    long_side = max(width, height)
    aspect_ratio = long_side / short_side if short_side else 999

    if area >= 500_000:
        score += 8
        reasons.append("large-image")
    elif area >= 120_000:
        score += 5
        reasons.append("medium-image")
    elif area < 25_000:
        score -= 8
        reasons.append("tiny-image")

    if short_side < 140:
        score -= 6
        reasons.append("small-short-side")

    if aspect_ratio > 6:
        score -= 5
        reasons.append("extreme-aspect")

    return score, reasons


def format_to_mime(ext: str) -> str:
    return {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "gif": "image/gif",
    }.get(ext.lower(), "image/jpeg")


def save_image(candidate: CandidateImage, site_row: dict[str, str], site_dir: Path) -> SavedImage:
    raw_format = (candidate.image_format or "").upper()
    format_to_ext = {
        "JPEG": "jpg",
        "JPG": "jpg",
        "PNG": "png",
        "WEBP": "webp",
        "GIF": "gif",
        "TIFF": "png",
        "JPEG2000": "png",
        "JP2": "png",
    }
    ext = format_to_ext.get(raw_format, "")
    if not ext:
        ext = Path(candidate.image_name).suffix.lstrip(".").lower() or "png"

    filename = f"p{candidate.page_num:03d}-{candidate.image_index:02d}-{slugify(site_row['SITE_NAME'])}.{ext}"
    output_path = site_dir / filename
    image = candidate.image_obj.image
    if ext in {"jpg", "jpeg"}:
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")
        image.save(output_path, format="JPEG", quality=90)
    else:
        if image.mode in {"CMYK", "P"}:
            image = image.convert("RGB")
        image.save(output_path)
    identifier = f"{site_row['SITE_SOURCE_ID']}/{filename}"
    encoded_identifier = quote(identifier, safe="")
    service_url = f"{IIIF_IMAGE_SERVICE_BASE_URL}/{encoded_identifier}"
    manifest_body_url = f"{service_url}/full/max/0/default.jpg"
    static_url = f"{PUBLIC_IMAGE_URL_BASE}/{site_row['SITE_SOURCE_ID']}/{filename}"
    return SavedImage(
        filename=output_path.name,
        identifier=identifier,
        static_url=static_url,
        manifest_body_url=manifest_body_url,
        service_url=service_url,
        width=candidate.width,
        height=candidate.height,
        image_format=candidate.image_format,
        candidate=candidate,
    )


def build_manifest(site_row: dict[str, str], images: list[SavedImage], used_pdfs: list[str]) -> tuple[str, dict[str, object]]:
    manifest_url = f"{MANIFEST_BASE_URL}/{site_row['SITE_SOURCE_ID']}.json"
    items = []
    for index, image in enumerate(images, start=1):
        body = {
            "id": image.manifest_body_url,
            "type": "Image",
            "format": format_to_mime(Path(image.filename).suffix.lstrip(".")),
            "width": image.width,
            "height": image.height,
            "service": [
                {
                    "id": image.service_url,
                    "type": "ImageService3",
                    "profile": "level2",
                }
            ],
        }
        items.append(
            {
                "id": f"{manifest_url}/canvas/{index}",
                "type": "Canvas",
                "label": {"en": [f"{site_row['SITE_NAME']} image {index}"]},
                "width": image.width,
                "height": image.height,
                "items": [
                    {
                        "id": f"{manifest_url}/page/{index}",
                        "type": "AnnotationPage",
                        "items": [
                            {
                                "id": f"{manifest_url}/annotation/{index}",
                                "type": "Annotation",
                                "motivation": "painting",
                                "target": f"{manifest_url}/canvas/{index}",
                                "body": body,
                            }
                        ],
                    }
                ],
                "rendering": [
                    {
                        "id": image.static_url,
                        "type": "Image",
                        "label": {"en": [f"Static file for {site_row['SITE_NAME']} image {index}"]},
                        "format": format_to_mime(Path(image.filename).suffix.lstrip(".")),
                    }
                ],
            }
        )

    manifest = {
        "@context": "http://iiif.io/api/presentation/3/context.json",
        "id": manifest_url,
        "type": "Manifest",
        "label": {"en": [site_row["SITE_NAME"]]},
        "summary": {"en": [site_row.get("COMMENTS", "")[:1000] or f"Illustrations for {site_row['SITE_NAME']}"]},
        "metadata": [
            {"label": {"en": ["Site source id"]}, "value": {"en": [site_row["SITE_SOURCE_ID"]]}},
            {"label": {"en": ["Localisation"]}, "value": {"en": [site_row.get("LOCALISATION", "") or ""]}},
            {"label": {"en": ["Source PDFs"]}, "value": {"en": used_pdfs or []}},
        ],
        "items": items,
    }
    return manifest_url, manifest


class PdfReaderCache:
    def __init__(self, max_size: int = 8) -> None:
        self.max_size = max_size
        self.cache: OrderedDict[str, PdfReader] = OrderedDict()

    def get(self, path: Path) -> PdfReader:
        key = str(path)
        reader = self.cache.get(key)
        if reader is not None:
            self.cache.move_to_end(key)
            return reader
        reader = PdfReader(str(path))
        self.cache[key] = reader
        self.cache.move_to_end(key)
        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)
        return reader


def choose_pdf_pages(site_key: str, site_row: dict[str, str], candidate_pages: dict[str, dict[str, Counter]]) -> list[tuple[str, list[int]]]:
    by_pdf = candidate_pages.get(site_key, {})
    ranked = sorted(
        by_pdf.items(),
        key=lambda item: (-sum(item[1].values()), -max(item[1].values()), item[0]),
    )[:MAX_PDFS_PER_SITE]
    result: list[tuple[str, list[int]]] = []
    for pdf_name, page_counter in ranked:
        top_pages = [page for page, _count in page_counter.most_common(MAX_PAGES_PER_PDF)]
        result.append((pdf_name, top_pages))
    return result


def build_candidates_for_site(
    site_row: dict[str, str],
    page_text_lookup: dict[str, list[str]],
    candidate_pages: dict[str, dict[str, Counter]],
    reader_cache: PdfReaderCache,
) -> tuple[list[CandidateImage], list[str], int]:
    site_key = normalize_name(site_row["SITE_NAME"])
    pdf_pages = choose_pdf_pages(site_key, site_row, candidate_pages)
    locality_terms = get_locality_terms(site_row)
    extracted: list[CandidateImage] = []
    used_pdfs: list[str] = []
    examined_pages = 0

    for pdf_name, page_nums in pdf_pages:
        pdf_path = ROOT / "data" / "google-drive-pdfs" / pdf_name
        if not pdf_path.exists():
            continue
        used_pdfs.append(pdf_name)
        page_texts = page_text_lookup.get(pdf_name, [])
        try:
            reader = reader_cache.get(pdf_path)
        except Exception:
            continue

        for page_num in page_nums:
            page_index = page_num - 1
            if page_index < 0 or page_index >= len(reader.pages):
                continue
            examined_pages += 1
            page = reader.pages[page_index]
            page_text = page_texts[page_index] if page_index < len(page_texts) else ""
            page_score, page_reasons = score_page_text(site_key, page_text, locality_terms)
            page_hit_count = candidate_pages.get(site_key, {}).get(pdf_name, {}).get(page_num, 0)
            if page_hit_count:
                page_score += min(12, page_hit_count * 4)
                page_reasons.append("site-mention-hits")

            try:
                images = list(page.images)
            except Exception:
                continue

            for image_index, image_obj in enumerate(images, start=1):
                width, height = image_obj.image.size
                score = page_score
                reasons = list(page_reasons)
                dimension_score, dimension_reasons = score_image_dimensions(width, height)
                score += dimension_score
                reasons.extend(dimension_reasons)
                if score < 0:
                    continue
                extracted.append(
                    CandidateImage(
                        pdf_path=pdf_path,
                        page_num=page_num,
                        image_index=image_index,
                        image_name=image_obj.name,
                        width=width,
                        height=height,
                        image_format=getattr(image_obj.image, "format", "") or "",
                        score=score,
                        reasons=reasons,
                        image_obj=image_obj,
                    )
                )

    extracted.sort(
        key=lambda item: (
            -item.score,
            -(item.width * item.height),
            item.pdf_path.name,
            item.page_num,
            item.image_index,
        )
    )
    return extracted, used_pdfs, examined_pages


def write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    sites = load_sites()
    page_text_lookup = load_page_text_lookup()
    candidate_pages = load_candidate_pages()
    clean_output_dir(PUBLIC_IMAGE_ROOT)
    clean_output_dir(MANIFEST_ROOT)
    reader_cache = PdfReaderCache()

    output_rows: list[dict[str, str]] = []
    detail_rows: list[dict[str, str]] = []
    stats = Counter()

    for site_row in sites:
        stats["sites_total"] += 1
        candidates, used_pdfs, examined_pages = build_candidates_for_site(
            site_row,
            page_text_lookup,
            candidate_pages,
            reader_cache,
        )
        stats["pages_examined"] += examined_pages
        site_dir = PUBLIC_IMAGE_ROOT / site_row["SITE_SOURCE_ID"]
        site_dir.mkdir(parents=True, exist_ok=True)

        selected = candidates[:MAX_IMAGES_PER_SITE]
        saved_images: list[SavedImage] = []
        for candidate in selected:
            saved_image = save_image(candidate, site_row, site_dir)
            saved_images.append(saved_image)
            stats["images_written"] += 1
            detail_rows.append(
                {
                    "SITE_SOURCE_ID": site_row["SITE_SOURCE_ID"],
                    "SITE_NAME": site_row["SITE_NAME"],
                    "PDF_FILE": candidate.pdf_path.name,
                    "PAGE": str(candidate.page_num),
                    "IMAGE_NAME": saved_image.filename,
                    "IMAGE_IDENTIFIER": saved_image.identifier,
                    "IMAGE_URL": saved_image.static_url,
                    "WIDTH": str(candidate.width),
                    "HEIGHT": str(candidate.height),
                    "FORMAT": candidate.image_format,
                    "SCORE": str(candidate.score),
                    "REASONS": " | ".join(candidate.reasons),
                    "MANIFEST_URL": "",
                }
            )

        manifest_url = ""
        if saved_images:
            manifest_url, manifest = build_manifest(site_row, saved_images, used_pdfs)
            manifest_path = MANIFEST_ROOT / f"{site_row['SITE_SOURCE_ID']}.json"
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            for detail_row in detail_rows[-len(saved_images) :]:
                detail_row["MANIFEST_URL"] = manifest_url
            stats["sites_with_images"] += 1
        else:
            stats["sites_without_images"] += 1

        output_rows.append(
            {
                "SITE_SOURCE_ID": site_row["SITE_SOURCE_ID"],
                "SITE_NAME": site_row["SITE_NAME"],
                "WEB_IMAGES": manifest_url,
                "IMAGE_COUNT": str(len(saved_images)),
                "SOURCE_PDFS": " | ".join(used_pdfs),
            }
        )

    write_csv(OUTPUT_CSV, OUTPUT_HEADERS, output_rows)
    write_csv(DETAIL_CSV, DETAIL_HEADERS, detail_rows)

    report_lines = [
        f"sites_total: {stats['sites_total']}",
        f"sites_with_images: {stats['sites_with_images']}",
        f"sites_without_images: {stats['sites_without_images']}",
        f"pages_examined: {stats['pages_examined']}",
        f"images_written: {stats['images_written']}",
        f"public_image_root: {PUBLIC_IMAGE_ROOT}",
        f"manifest_root: {MANIFEST_ROOT}",
        f"manifest_base_url: {MANIFEST_BASE_URL}",
        f"iiif_image_service_base_url: {IIIF_IMAGE_SERVICE_BASE_URL}",
        f"output_csv: {OUTPUT_CSV}",
        f"detail_csv: {DETAIL_CSV}",
    ]
    OUTPUT_REPORT.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
