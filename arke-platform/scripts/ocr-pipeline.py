#!/usr/bin/env python3
"""
OCR Pipeline for scanned PDFs in arke-platform.

Converts PDF pages to images and runs Tesseract OCR to extract text.
Saves results in the same JSON format as other locality info files.

Usage:
    python3 scripts/ocr-pipeline.py [pdf_path]
    python3 scripts/ocr-pipeline.py --all
    python3 scripts/ocr-pipeline.py data/google-drive-pdfs/some_file.pdf
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, Tuple
from PIL import Image
import io


ROOT = Path(__file__).resolve().parents[1]
PDF_ROOT = ROOT / "data" / "google-drive-pdfs"
TEXT_ROOT = ROOT / "data" / "google-drive-text"


@dataclass
class OCRResult:
    source_pdf: str
    sha256: str
    pages: int
    text_chars: int
    page_texts: list
    ocr_confidence: float
    error: Optional[str] = None


def get_pdf_sha256(pdf_path: Path) -> str:
    import hashlib
    return hashlib.sha256(pdf_path.read_bytes()).hexdigest()


def check_dependencies() -> Tuple[bool, str]:
    """Check if required dependencies are available."""
    try:
        import pytesseract
        pytesseract_available = True
    except ImportError:
        pytesseract_available = False
    
    try:
        from pdf2image import convert_from_path
        pdf2image_available = True
    except ImportError:
        pdf2image_available = False
    
    try:
        import fitz
        pymupdf_available = True
    except ImportError:
        pymupdf_available = False
    
    return pytesseract_available, pdf2image_available, pymupdf_available


def render_pdf_pages(pdf_path: Path, dpi: int = 300) -> list:
    """Render PDF pages to images using available method."""
    pytesseract_available, pdf2image_available, pymupdf_available = check_dependencies()
    
    if pdf2image_available:
        try:
            from pdf2image import convert_from_path
            return convert_from_path(str(pdf_path), dpi=dpi)
        except Exception as e:
            print(f"    pdf2image failed: {e}")
    
    if pymupdf_available:
        try:
            import fitz
            doc = fitz.open(str(pdf_path))
            pages = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                mat = fitz.Matrix(dpi/72, dpi/72)
                pix = page.get_pixmap(matrix=mat)
                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))
                pages.append(img)
            doc.close()
            return pages
        except Exception as e:
            print(f"    PyMuPDF failed: {e}")
    
    raise RuntimeError("No PDF rendering library available. Install pdf2image or pymupdf.")


def ocr_image(image, lang: str = "eng") -> Tuple[str, float]:
    """Run OCR on an image and return (text, confidence)."""
    import pytesseract
    
    data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT, lang=lang)
    text_parts = []
    confidences = []
    
    for i, conf in enumerate(data['conf']):
        if conf > 0:
            text = data['text'][i].strip()
            if text:
                text_parts.append(text)
            confidences.append(conf)
    
    full_text = ' '.join(text_parts)
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    
    return full_text, avg_confidence


def ocr_pdf(pdf_path: Path, dpi: int = 300, lang: str = "eng") -> OCRResult:
    """OCR a PDF file and return structured results."""
    pytesseract_available, _, _ = check_dependencies()
    
    if not pytesseract_available:
        return OCRResult(
            source_pdf=str(pdf_path),
            sha256=get_pdf_sha256(pdf_path),
            pages=0,
            text_chars=0,
            page_texts=[],
            ocr_confidence=0.0,
            error="Tesseract not installed. Run: pip install pytesseract"
        )
    
    try:
        pages = render_pdf_pages(pdf_path, dpi=dpi)
        page_texts = []
        total_confidence = 0.0
        
        for page_image in pages:
            text, confidence = ocr_image(page_image, lang=lang)
            page_texts.append(text)
            total_confidence += confidence
        
        avg_confidence = total_confidence / len(pages) if pages else 0.0
        
        return OCRResult(
            source_pdf=str(pdf_path),
            sha256=get_pdf_sha256(pdf_path),
            pages=len(pages),
            text_chars=sum(len(t) for t in page_texts),
            page_texts=page_texts,
            ocr_confidence=round(avg_confidence, 2)
        )
        
    except Exception as e:
        return OCRResult(
            source_pdf=str(pdf_path),
            sha256=get_pdf_sha256(pdf_path),
            pages=0,
            text_chars=0,
            page_texts=[],
            ocr_confidence=0.0,
            error=str(e)
        )


def save_result(result: OCRResult, output_path: Path) -> None:
    """Save OCR result to JSON file."""
    output_path.write_text(json.dumps(asdict(result), indent=2, ensure_ascii=False), encoding="utf-8")


def process_pdf(pdf_path: Path, overwrite: bool = False) -> Optional[Path]:
    """Process a single PDF and save results."""
    pdf_path = Path(pdf_path).resolve()
    
    if not pdf_path.exists():
        print(f"  ERROR: File not found: {pdf_path}")
        return None
    
    json_filename = pdf_path.stem + ".json"
    output_path = TEXT_ROOT / json_filename
    
    if output_path.exists() and not overwrite:
        try:
            existing = json.loads(output_path.read_text())
            if existing.get('text_chars', 0) > 100:
                print(f"  SKIP: {output_path.name} already has {existing.get('text_chars', 0)} chars")
                return None
        except:
            pass
    
    print(f"  Processing: {pdf_path.name}")
    result = ocr_pdf(pdf_path)
    
    if result.error:
        print(f"  ERROR: {result.error}")
    else:
        print(f"    Pages: {result.pages}, Chars: {result.text_chars}, Confidence: {result.ocr_confidence}%")
        save_result(result, output_path)
        print(f"    Saved: {output_path.name}")
        
        if result.text_chars > 0:
            print("\n    === Sample text (first 500 chars) ===")
            sample = ' '.join(result.page_texts[:2])[:500]
            print(f"    {sample}...")
    
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="OCR pipeline for arke-platform PDFs")
    parser.add_argument("pdf_path", nargs="?", help="Path to PDF file to process")
    parser.add_argument("--all", action="store_true", help="Process all PDFs that need OCR")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing results")
    parser.add_argument("--lang", default="eng", help="Tesseract language (default: eng)")
    parser.add_argument("--dpi", type=int, default=300, help="DPI for image conversion (default: 300)")
    
    args = parser.parse_args()
    
    pytesseract_available, _, pymupdf_available = check_dependencies()
    
    if not pytesseract_available:
        print("ERROR: pytesseract not installed.")
        print("Run: pip install pytesseract")
        return 1
    
    if not pymupdf_available:
        print("WARNING: pymupdf not installed (recommended for PDF rendering).")
        print("Run: pip install pymupdf")
        print("Or: brew install poppler (for pdf2image)")
    
    if args.all:
        print("Finding PDFs to OCR...")
        pdfs_to_process = list(PDF_ROOT.glob("*.pdf"))
        print(f"Found {len(pdfs_to_process)} PDFs")
    elif args.pdf_path:
        pdfs_to_process = [Path(args.pdf_path)]
    else:
        parser.print_help()
        return 1
    
    processed = 0
    for pdf_path in pdfs_to_process:
        result = process_pdf(pdf_path, overwrite=args.overwrite)
        if result:
            processed += 1
    
    print(f"\nProcessed {processed} PDFs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
