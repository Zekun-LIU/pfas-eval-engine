"""
file_readers.py: turn any uploaded customer file into a ReadDocument.

Each reader reports what it could not read in `problems` rather than dropping
it, because a value that silently goes missing is worse than a failed upload.
The parsers (parser.py, llm_parser.py) decide what to do with the contents.

Claros Product Development Team | Framework Architecture by Zack Liu
"""
from __future__ import annotations

import csv
import email
import email.policy
import io
import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

KIND_SPREADSHEET = "spreadsheet"
KIND_PDF = "pdf"
KIND_DOCUMENT = "document"      # tables and/or prose (Word, HTML, PowerPoint, text ...)
KIND_IMAGE = "image"
KIND_UNREADABLE = "unreadable"

# Claude's high-resolution vision limits. Larger images are downscaled by the
# API, which is what blurs small digits, so stay inside them ourselves.
MAX_LONG_EDGE_PX = 2576
MAX_PIXELS = 3_750_000
MAX_IMAGE_BYTES = 3_500_000
_PNG_PREFERRED_BYTES = 1_500_000

# Below this an embedded picture is a logo or signature, not data.
_MIN_EMBEDDED_LONG_EDGE = 500
_MIN_EMBEDDED_SHORT_EDGE = 200
_LOW_RES_LONG_EDGE = 800

_MAX_PDF_PAGES = 300
_MAX_ARCHIVE_MEMBERS = 50
_MAX_ARCHIVE_BYTES = 300_000_000
_MAX_NESTING = 3

_SPREADSHEET_EXT = {"xlsx", "xlsm", "xltx", "xltm", "xls", "xlsb", "ods", "csv", "tsv"}
_IMAGE_EXT = {"png", "jpg", "jpeg", "tif", "tiff", "bmp", "webp", "gif", "heic", "heif"}
_TEXT_EXT = {"txt", "text", "md", "log", "json", "xml", "yaml", "yml", "dat"}
_NEEDS_RESAVE = {
    "doc": "Word 97-2003 (.doc)",
    "ppt": "PowerPoint 97-2003 (.ppt)",
    "wps": "WPS (.wps)",
    "pages": "Apple Pages",
    "numbers": "Apple Numbers",
    "key": "Apple Keynote",
}
_NEEDS_UNPACK = {"7z", "rar", "tar", "gz"}

ACCEPTED_FORMATS_LABEL = (
    "Excel, CSV, PDF, Word, PowerPoint, images and photos, email (.eml, .msg), "
    "web pages, text, ZIP"
)


@dataclass
class ImagePart:
    label: str
    data: bytes
    media_type: str


@dataclass
class ReadDocument:
    name: str
    fmt: str
    kind: str
    data: bytes = b""
    text: str = ""                                    # everything readable, tables included
    prose: str = ""                                   # text outside tables
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    images: List[ImagePart] = field(default_factory=list)
    problems: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    is_correspondence: bool = False


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINTS
# ═══════════════════════════════════════════════════════════════════════════════

def read_upload(data: bytes, name: str, depth: int = 0) -> List[ReadDocument]:
    """Read one uploaded file. Emails and archives can yield several documents."""
    ext = PurePosixPath(name.lower()).suffix.lstrip(".")
    try:
        return _dispatch(data, name, ext, depth)
    except Exception as exc:
        return [_unreadable(name, ext.upper() or "Unknown", f"could not be opened ({exc}).")]


def split_context_documents(docs: Iterable[ReadDocument]) -> Tuple[str, List[ReadDocument]]:
    """
    Split files attached as customer notes or treatment goals into context text
    and lab data, so a results table or a photo attached to an email still gets
    read as data.
    """
    texts: List[str] = []
    lab_docs: List[ReadDocument] = []
    for doc in docs:
        if doc.kind in (KIND_SPREADSHEET, KIND_IMAGE, KIND_UNREADABLE):
            lab_docs.append(doc)
            continue
        body = doc.text if doc.kind == KIND_PDF else (doc.prose or doc.text)
        if body.strip():
            texts.append(f"[Attached {doc.fmt}: {doc.name}]\n{body.strip()}")
        if doc.tables or doc.images or doc.problems:
            lab_docs.append(ReadDocument(
                name=doc.name, fmt=doc.fmt, kind=KIND_DOCUMENT,
                text=_tables_to_text(doc.tables),
                tables=doc.tables, images=doc.images,
                problems=doc.problems, notes=doc.notes,
            ))
    return "\n\n".join(texts), lab_docs


def load_sheets(data: bytes, name: str) -> Dict[str, pd.DataFrame]:
    """
    Load a spreadsheet as raw string cells. LIMS exports often carry the wrong
    extension (HTML or tab-separated text named .xls), so fall back on content.
    """
    ext = PurePosixPath(name.lower()).suffix.lstrip(".")
    if ext in ("csv", "tsv"):
        return {"Sheet1": _delimited_to_frame(_decode_text(data))}
    try:
        return pd.read_excel(io.BytesIO(data), header=None, dtype=str, sheet_name=None)
    except Exception:
        text = _decode_text(data)
        if _looks_like_html(text):
            tables = _html_tables(text)
            if tables:
                return tables
        if _is_text_bytes(data) and _detect_delimiter(text.splitlines()[:60]):
            return {"Sheet1": _delimited_to_frame(text)}
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# DISPATCH
# ═══════════════════════════════════════════════════════════════════════════════

def _dispatch(data: bytes, name: str, ext: str, depth: int) -> List[ReadDocument]:
    if not data:
        return [_unreadable(name, ext.upper() or "Unknown", "the file is empty.")]
    if ext in _NEEDS_RESAVE:
        return [_unreadable(
            name, _NEEDS_RESAVE[ext],
            f"{_NEEDS_RESAVE[ext]} files cannot be read reliably. "
            "Save the file as PDF or a current Office format and upload that.",
        )]
    if ext in _NEEDS_UNPACK:
        return [_unreadable(
            name, ext.upper(), "this archive type cannot be opened. Unpack it or re-zip as .zip."
        )]

    if ext in _SPREADSHEET_EXT:
        return [_read_spreadsheet(data, name, ext)]
    if ext == "pdf":
        return [_read_pdf(data, name)]
    if ext in ("docx", "docm", "dotx"):
        return [_read_docx(data, name)]
    if ext in ("pptx", "pptm"):
        return [_read_pptx(data, name)]
    if ext == "odt":
        return [_read_odt(data, name)]
    if ext in _IMAGE_EXT:
        return [_read_image(data, name)]
    if ext == "eml":
        return _read_eml(data, name, depth)
    if ext == "msg":
        return _read_msg(data, name, depth)
    if ext == "rtf":
        return [_read_rtf(data, name)]
    if ext in ("htm", "html", "mht", "mhtml"):
        return [_read_html(data, name)]
    if ext == "zip":
        return _read_zip(data, name, depth)
    if ext in _TEXT_EXT:
        return [_read_text(data, name, ext.upper())]
    return _sniff_and_read(data, name, ext, depth)


def _sniff_and_read(data: bytes, name: str, ext: str, depth: int) -> List[ReadDocument]:
    """Unknown or missing extension: decide from the file's contents."""
    if data[:5] == b"%PDF-":
        return [_read_pdf(data, name)]
    if data[:2] == b"PK":
        try:
            members = zipfile.ZipFile(io.BytesIO(data)).namelist()
        except zipfile.BadZipFile:
            members = []
        if any(m.startswith("word/") for m in members):
            return [_read_docx(data, name)]
        if any(m.startswith("xl/") for m in members):
            return [_read_spreadsheet(data, name, "xlsx")]
        if any(m.startswith("ppt/") for m in members):
            return [_read_pptx(data, name)]
        if members:
            return _read_zip(data, name, depth)
    if _pil_can_open(data):
        return [_read_image(data, name)]
    if _is_text_bytes(data):
        return [_read_text(data, name, ext.upper() or "Text")]
    return [_unreadable(
        name, ext.upper() or "Unknown",
        "this file type is not recognised. Save it as PDF or Excel and upload that.",
    )]


def _unreadable(name: str, fmt: str, why: str) -> ReadDocument:
    return ReadDocument(name=name, fmt=fmt, kind=KIND_UNREADABLE, problems=[f"'{name}' was not read: {why}"])


# ═══════════════════════════════════════════════════════════════════════════════
# SPREADSHEETS AND DELIMITED TEXT
# ═══════════════════════════════════════════════════════════════════════════════

def _read_spreadsheet(data: bytes, name: str, ext: str) -> ReadDocument:
    fmt = {"csv": "CSV", "tsv": "TSV", "ods": "OpenDocument sheet"}.get(ext, "Excel")
    doc = ReadDocument(name=name, fmt=fmt, kind=KIND_SPREADSHEET, data=data)
    try:
        sheets = load_sheets(data, name)
    except Exception as exc:
        hint = "it may be password-protected" if "encrypt" in str(exc).lower() or data[:4] == b"\xd0\xcf\x11\xe0" else str(exc)
        return _unreadable(name, fmt, f"could not be opened ({hint}).")
    if all(df is None or df.empty for df in sheets.values()):
        doc.problems.append(f"'{name}' opened but contains no cells.")
    doc.text = _tables_to_text(sheets)
    if ext not in ("csv", "tsv", "xls", "xlsb", "ods"):
        _add_embedded_images(doc, data, "xl/media/")
    return doc


def _decode_text(data: bytes) -> str:
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return data.decode("utf-16", errors="replace")
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace")


def _is_text_bytes(data: bytes) -> bool:
    sample = data[:4096]
    if sample[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return True
    if b"\x00" in sample:
        return False
    controls = sum(1 for b in sample if b < 9 or 13 < b < 32)
    return controls <= len(sample) * 0.02


def _detect_delimiter(lines: List[str], candidates: str = ",;\t|") -> Optional[str]:
    """The delimiter that splits most lines into the same, largest number of columns."""
    best, best_score = None, 0
    for delim in candidates:
        counts = [len(next(csv.reader([ln], delimiter=delim))) for ln in lines if ln.strip()]
        multi = [c for c in counts if c > 1]
        if not multi:
            continue
        typical = max(set(multi), key=multi.count)
        score = multi.count(typical) * typical
        if score > best_score:
            best, best_score = delim, score
    return best


def _delimited_to_frame(text: str, delimiter: Optional[str] = None) -> pd.DataFrame:
    """Rows may differ in length: lab exports put title lines above the table."""
    lines = text.splitlines()
    delim = delimiter or _detect_delimiter(lines[:60]) or ","
    rows = [row for row in csv.reader(lines, delimiter=delim)]
    width = max((len(r) for r in rows), default=0)
    return pd.DataFrame([r + [""] * (width - len(r)) for r in rows], dtype=str)


def _tables_to_text(tables: Dict[str, pd.DataFrame]) -> str:
    parts: List[str] = []
    for title, df in tables.items():
        if df is None or df.empty:
            continue
        parts.append(f"=== {title} ===")
        parts.append(_frame_to_text(df))
    return "\n".join(parts)


def _frame_to_text(df: pd.DataFrame) -> str:
    lines = []
    for _, row in df.iterrows():
        cells = ["(blank)" if str(v).strip().lower() in ("", "nan", "none") else str(v).strip() for v in row]
        if any(c != "(blank)" for c in cells):
            lines.append(" | ".join(cells))
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# PDF
# ═══════════════════════════════════════════════════════════════════════════════

def _page_needs_vision(page, text: str) -> bool:
    stripped = text.strip()
    if stripped.count("(cid:") > 20:
        return True     # font without a usable encoding: the "text" is not trustworthy
    if len(stripped) >= 300:
        return False
    page_area = float(page.width * page.height) or 1.0
    image_area = sum(float(im.get("width", 0)) * float(im.get("height", 0)) for im in page.images)
    if image_area / page_area > 0.5:
        return True
    if len(stripped) < 40:
        return bool(page.images) or len(page.curves) > 50   # scan, or text drawn as outlines
    return False


def _read_pdf(data: bytes, name: str) -> ReadDocument:
    import pdfplumber
    import pypdfium2

    doc = ReadDocument(name=name, fmt="PDF", kind=KIND_PDF, data=data)
    try:
        pdf = pdfplumber.open(io.BytesIO(data))
    except Exception as exc:
        low = str(exc).lower()
        hint = "it is password-protected" if ("password" in low or "encrypt" in low or not low) else str(exc)
        return _unreadable(name, "PDF", f"could not be opened ({hint}).")

    pages_text: List[str] = []
    vision_pages: List[int] = []
    with pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages[:_MAX_PDF_PAGES]):
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
            if _page_needs_vision(page, text):
                vision_pages.append(i)
                pages_text.append(f"[Page {i + 1}: scanned image, see attached image]")
            elif text.strip():
                pages_text.append(f"[Page {i + 1}]\n{text}")
    if total > _MAX_PDF_PAGES:
        doc.problems.append(
            f"'{name}' has {total} pages; only the first {_MAX_PDF_PAGES} were read."
        )
    doc.text = doc.prose = "\n\n".join(pages_text)

    if vision_pages:
        rendered = pypdfium2.PdfDocument(data)
        try:
            for i in vision_pages:
                page = rendered[i]
                width, height = page.get_size()
                scale = _fit_scale(width, height, upscale_to_limit=True)
                image = page.render(scale=scale).to_pil()
                doc.images.append(_encode_image(image, f"{name}, page {i + 1}"))
        finally:
            rendered.close()
    if not doc.text.strip() and not doc.images:
        doc.problems.append(f"'{name}' has no readable content.")
    return doc


# ═══════════════════════════════════════════════════════════════════════════════
# OFFICE DOCUMENTS
# ═══════════════════════════════════════════════════════════════════════════════

def _read_docx(data: bytes, name: str) -> ReadDocument:
    import docx
    from docx.oxml.ns import qn
    from docx.table import Table
    from docx.text.paragraph import Paragraph

    doc = ReadDocument(name=name, fmt="Word", kind=KIND_DOCUMENT)
    try:
        word = docx.Document(io.BytesIO(data))
    except Exception as exc:
        return _unreadable(name, "Word", f"could not be opened ({exc}).")

    ordered: List[str] = []
    prose: List[str] = []
    for child in word.element.body.iterchildren():
        if child.tag == qn("w:p"):
            text = Paragraph(child, word).text.strip()
            if text:
                ordered.append(text)
                prose.append(text)
        elif child.tag == qn("w:tbl"):
            title = f"Table {len(doc.tables) + 1}"
            frame = _docx_table_to_frame(Table(child, word))
            doc.tables[title] = frame
            ordered.append(f"=== {title} ===\n{_frame_to_text(frame)}")

    # Headers, footers and text boxes often carry the sample ID or report number.
    extra: List[str] = []
    for section in word.sections:
        for part in (section.header, section.footer):
            extra += [p.text.strip() for p in part.paragraphs if p.text.strip()]
    for box in word.element.body.iter(qn("w:txbxContent")):
        text = "".join(t.text or "" for t in box.iter(qn("w:t"))).strip()
        if text:
            extra.append(text)
    extra = list(dict.fromkeys(extra))
    if extra:
        ordered.append("[Headers, footers, text boxes]\n" + "\n".join(extra))
        prose += extra

    doc.text = "\n".join(ordered)
    doc.prose = "\n".join(prose)
    _add_embedded_images(doc, data, "word/media/")
    if not doc.text.strip() and not doc.images:
        doc.problems.append(f"'{name}' has no readable content.")
    return doc


def _docx_table_to_frame(table) -> pd.DataFrame:
    rows = []
    for row in table.rows:
        cells, seen = [], None
        for cell in row.cells:
            # A horizontally merged cell is returned once per grid column.
            cells.append("" if cell._tc is seen else cell.text.strip())
            seen = cell._tc
        rows.append(cells)
    width = max((len(r) for r in rows), default=0)
    return pd.DataFrame([r + [""] * (width - len(r)) for r in rows], dtype=str)


def _read_pptx(data: bytes, name: str) -> ReadDocument:
    from pptx import Presentation

    doc = ReadDocument(name=name, fmt="PowerPoint", kind=KIND_DOCUMENT)
    try:
        deck = Presentation(io.BytesIO(data))
    except Exception as exc:
        return _unreadable(name, "PowerPoint", f"could not be opened ({exc}).")

    ordered: List[str] = []
    prose: List[str] = []

    def visit(shape, slide_no: int) -> None:
        if shape.shape_type == 6:   # group
            for inner in shape.shapes:
                visit(inner, slide_no)
        elif getattr(shape, "has_table", False) and shape.has_table:
            title = f"Slide {slide_no} table {len(doc.tables) + 1}"
            frame = pd.DataFrame(
                [[cell.text.strip() for cell in row.cells] for row in shape.table.rows], dtype=str
            )
            doc.tables[title] = frame
            ordered.append(f"=== {title} ===\n{_frame_to_text(frame)}")
        elif getattr(shape, "has_text_frame", False) and shape.has_text_frame:
            text = shape.text_frame.text.strip()
            if text:
                ordered.append(text)
                prose.append(text)

    for number, slide in enumerate(deck.slides, 1):
        ordered.append(f"[Slide {number}]")
        for shape in slide.shapes:
            visit(shape, number)
        if slide.has_notes_slide:
            notes = slide.notes_slide.notes_text_frame.text.strip()
            if notes:
                ordered.append(f"[Slide {number} notes] {notes}")
                prose.append(notes)
    doc.text = "\n".join(ordered)
    doc.prose = "\n".join(prose)
    _add_embedded_images(doc, data, "ppt/media/")
    return doc


def _read_odt(data: bytes, name: str) -> ReadDocument:
    doc = ReadDocument(name=name, fmt="OpenDocument text", kind=KIND_DOCUMENT)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        root = ET.fromstring(archive.read("content.xml"))
    ns_table = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
    ns_text = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"
    in_tables = set()
    for table in root.iter(f"{ns_table}table"):
        rows = []
        for row in table.iter(f"{ns_table}table-row"):
            cells = []
            for cell in row.iter(f"{ns_table}table-cell"):
                cells.append(" ".join("".join(p.itertext()).strip() for p in cell.iter(f"{ns_text}p")))
                in_tables.update(cell.iter(f"{ns_text}p"))
            rows.append(cells)
        if rows:
            width = max(len(r) for r in rows)
            doc.tables[f"Table {len(doc.tables) + 1}"] = pd.DataFrame(
                [r + [""] * (width - len(r)) for r in rows], dtype=str
            )
    prose = [
        "".join(p.itertext()).strip()
        for tag in ("p", "h")
        for p in root.iter(f"{ns_text}{tag}")
        if p not in in_tables and "".join(p.itertext()).strip()
    ]
    doc.prose = "\n".join(prose)
    doc.text = "\n".join(filter(None, [doc.prose, _tables_to_text(doc.tables)]))
    return doc


def _add_embedded_images(doc: ReadDocument, data: bytes, media_prefix: str) -> None:
    """Pictures inside Office files are often pasted lab tables, so they count as data."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        return
    vector = 0
    with archive:
        for member in archive.namelist():
            if not member.startswith(media_prefix):
                continue
            if member.lower().endswith((".emf", ".wmf", ".svg")):
                vector += 1
                continue
            parts = _image_parts(archive.read(member), f"{doc.name}, picture {PurePosixPath(member).name}", embedded=True)
            doc.images.extend(parts)
    if vector:
        doc.problems.append(
            f"'{doc.name}' contains {vector} vector picture(s) (EMF/WMF/SVG) that were not read. "
            "If they hold data, export them as PNG or the document as PDF."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# IMAGES
# ═══════════════════════════════════════════════════════════════════════════════

def _open_image(data: bytes):
    from PIL import Image
    try:
        import pillow_heif
        pillow_heif.register_heif_opener()
    except ImportError:
        pass
    return Image.open(io.BytesIO(data))


def _pil_can_open(data: bytes) -> bool:
    try:
        _open_image(data).verify()
        return True
    except Exception:
        return False


def _fit_scale(width: float, height: float, upscale_to_limit: bool = False) -> float:
    scale = min(MAX_LONG_EDGE_PX / max(width, height), (MAX_PIXELS / (width * height)) ** 0.5)
    return scale if upscale_to_limit else min(scale, 1.0)


def _encode_image(image, label: str) -> ImagePart:
    """Lossless when small, because JPEG artefacts can change a small digit."""
    from PIL import Image

    image = image.convert("RGB")
    scale = _fit_scale(*image.size)
    if scale < 1.0:
        image = image.resize(
            (max(1, int(image.width * scale)), max(1, int(image.height * scale))),
            Image.LANCZOS,
        )
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    if buffer.tell() <= _PNG_PREFERRED_BYTES:
        return ImagePart(label, buffer.getvalue(), "image/png")
    for quality in (95, 90, 85, 75):
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG", quality=quality)
        if buffer.tell() <= MAX_IMAGE_BYTES:
            break
    return ImagePart(label, buffer.getvalue(), "image/jpeg")


def _image_parts(data: bytes, label: str, embedded: bool = False) -> List[ImagePart]:
    from PIL import ImageOps, ImageSequence

    source = _open_image(data)
    parts: List[ImagePart] = []
    frames = list(ImageSequence.Iterator(source))
    if source.format == "GIF":
        frames = frames[:1]
    for number, frame in enumerate(frames, 1):
        image = ImageOps.exif_transpose(frame.copy())
        long_edge, short_edge = max(image.size), min(image.size)
        if embedded and (long_edge < _MIN_EMBEDDED_LONG_EDGE or short_edge < _MIN_EMBEDDED_SHORT_EDGE):
            continue
        page_label = f"{label}, page {number}" if len(frames) > 1 else label
        parts.extend(_tile(image, page_label))
    return parts


def _tile(image, label: str) -> List[ImagePart]:
    """A long screenshot shrunk to fit would lose its digits, so cut it into overlapping pieces."""
    width, height = image.size
    long_edge, short_edge = max(width, height), min(width, height)
    if long_edge / max(short_edge, 1) <= 2 or _fit_scale(width, height) >= 1.0:
        return [_encode_image(image, label)]
    window = int(min(MAX_LONG_EDGE_PX, short_edge * 1.4, MAX_PIXELS / short_edge))
    step = int(window * 0.9)
    tiles, start = [], 0
    while True:
        end = min(start + window, long_edge)
        box = (0, start, width, end) if height >= width else (start, 0, end, height)
        tiles.append(image.crop(box))
        if end >= long_edge:
            break
        start += step
    return [
        _encode_image(tile, f"{label}, section {i} of {len(tiles)} (sections overlap)")
        for i, tile in enumerate(tiles, 1)
    ]


def _read_image(data: bytes, name: str) -> ReadDocument:
    doc = ReadDocument(name=name, fmt="Image", kind=KIND_IMAGE)
    try:
        doc.images = _image_parts(data, name)
        width, height = _open_image(data).size
    except Exception as exc:
        return _unreadable(name, "Image", f"could not be opened ({exc}).")
    if max(width, height) < _LOW_RES_LONG_EDGE:
        doc.problems.append(
            f"'{name}' is low resolution ({width}×{height}), so its digits may be misread. "
            "Ask the customer for the original file if you can."
        )
    return doc


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT, WEB PAGES, EMAIL, ARCHIVES
# ═══════════════════════════════════════════════════════════════════════════════

def _read_text(data: bytes, name: str, fmt: str) -> ReadDocument:
    text = _decode_text(data)
    if _looks_like_html(text):
        return _read_html(data, name)
    doc = ReadDocument(name=name, fmt=fmt or "Text", kind=KIND_DOCUMENT, text=text, prose=text)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    delim = _detect_delimiter(lines[:60], "\t;|")
    if delim and sum(delim in ln for ln in lines) >= 3:
        doc.tables["Table 1"] = _delimited_to_frame(text, delim)
    return doc


def _read_rtf(data: bytes, name: str) -> ReadDocument:
    from striprtf.striprtf import rtf_to_text

    text = rtf_to_text(_decode_text(data), errors="ignore")
    doc = ReadDocument(name=name, fmt="RTF", kind=KIND_DOCUMENT, text=text, prose=text)
    lines = [ln for ln in text.splitlines() if ln.count("|") >= 1]
    if len(lines) >= 2:
        doc.tables["Table 1"] = _delimited_to_frame("\n".join(lines), "|")
    return doc


def _looks_like_html(text: str) -> bool:
    head = text[:2000].lower()
    return "<html" in head or "<table" in head or "<!doctype html" in head


def _html_tables(html_text: str) -> Dict[str, pd.DataFrame]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_text, "lxml")
    tables: Dict[str, pd.DataFrame] = {}
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            row: List[str] = []
            for cell in tr.find_all(["td", "th"]):
                row.append(cell.get_text(" ", strip=True))
                row.extend([""] * (int(cell.get("colspan", 1) or 1) - 1))
            if row:
                rows.append(row)
        if rows:
            width = max(len(r) for r in rows)
            tables[f"Table {len(tables) + 1}"] = pd.DataFrame(
                [r + [""] * (width - len(r)) for r in rows], dtype=str
            )
    return tables


def _html_to_prose(html_text: str) -> str:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_text, "lxml")
    for tag in soup(["script", "style", "table", "head"]):
        tag.decompose()
    return "\n".join(ln.strip() for ln in soup.get_text("\n").splitlines() if ln.strip())


def _read_html(data: bytes, name: str) -> ReadDocument:
    text = _decode_text(data)
    tables = _html_tables(text)
    prose = _html_to_prose(text)
    return ReadDocument(
        name=name, fmt="Web page", kind=KIND_DOCUMENT, tables=tables, prose=prose,
        text="\n".join(filter(None, [prose, _tables_to_text(tables)])),
    )


def _email_document(name: str, fmt: str, headers: List[str], body: str, body_html: str = "") -> ReadDocument:
    tables = _html_tables(body_html) if body_html else {}
    prose = body.strip() or (_html_to_prose(body_html) if body_html else "")
    header_block = "\n".join(h for h in headers if h.split(":", 1)[-1].strip())
    return ReadDocument(
        name=name, fmt=fmt, kind=KIND_DOCUMENT, tables=tables, is_correspondence=True,
        prose=f"{header_block}\n\n{prose}".strip(),
        text="\n".join(filter(None, [header_block, prose, _tables_to_text(tables)])),
    )


def _read_attachment(data: bytes, name: str, parent: str, depth: int) -> List[ReadDocument]:
    """Images in a mail are usually signatures, so apply the embedded-picture size filter."""
    if depth >= _MAX_NESTING:
        return [_unreadable(name, "Attachment", f"nested too deeply inside '{parent}'.")]
    label = f"{parent} / {name}"
    ext = PurePosixPath(name.lower()).suffix.lstrip(".")
    if ext in _IMAGE_EXT:
        try:
            parts = _image_parts(data, label, embedded=True)
        except Exception as exc:
            return [_unreadable(label, "Image", f"could not be opened ({exc}).")]
        return [ReadDocument(name=label, fmt="Image", kind=KIND_IMAGE, images=parts)] if parts else []
    docs = read_upload(data, name, depth + 1)
    for doc in docs:
        doc.name = f"{parent} / {doc.name}"
    return docs


def _read_eml(data: bytes, name: str, depth: int) -> List[ReadDocument]:
    message = email.message_from_bytes(data, policy=email.policy.default)
    headers = [f"{h}: {message.get(h, '')}" for h in ("From", "To", "Date", "Subject")]
    plain = message.get_body(preferencelist=("plain",))
    html_part = message.get_body(preferencelist=("html",))
    docs = [_email_document(
        name, "Email", headers,
        plain.get_content() if plain else "",
        html_part.get_content() if html_part else "",
    )]
    for part in message.walk():
        if part.is_multipart() or part in (plain, html_part):
            continue
        content_type = part.get_content_type()
        filename = part.get_filename()
        if content_type == "message/rfc822":
            inner = part.get_payload()
            payload = inner[0].as_bytes() if isinstance(inner, list) else inner.as_bytes()
            docs += _read_attachment(payload, filename or "attached.eml", name, depth)
        elif filename or content_type.startswith("image/"):
            payload = part.get_payload(decode=True)
            if payload:
                ext = content_type.split("/")[-1]
                docs += _read_attachment(payload, filename or f"inline.{ext}", name, depth)
    return docs


def _read_msg(data: bytes, name: str, depth: int) -> List[ReadDocument]:
    import extract_msg

    message = extract_msg.openMsg(data)
    try:
        html_body = message.htmlBody
        if isinstance(html_body, bytes):
            html_body = _decode_text(html_body)
        headers = [
            f"From: {message.sender or ''}", f"To: {message.to or ''}",
            f"Date: {message.date or ''}", f"Subject: {message.subject or ''}",
        ]
        docs = [_email_document(name, "Email", headers, message.body or "", html_body or "")]
        for attachment in message.attachments:
            payload = attachment.data
            filename = attachment.longFilename or attachment.shortFilename or "attachment"
            if isinstance(payload, (bytes, bytearray)):
                docs += _read_attachment(bytes(payload), filename, name, depth)
            else:
                docs[0].problems.append(
                    f"'{name}' contains a nested email ('{filename}') that was not read. "
                    "Upload it separately."
                )
        return docs
    finally:
        message.close()


def _read_zip(data: bytes, name: str, depth: int) -> List[ReadDocument]:
    if depth >= _MAX_NESTING:
        return [_unreadable(name, "ZIP", "archive nested too deeply.")]
    docs: List[ReadDocument] = []
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        members = [
            m for m in archive.infolist()
            if not m.is_dir() and "__MACOSX" not in m.filename
            and not PurePosixPath(m.filename).name.startswith((".", "~$"))
        ]
        if len(members) > _MAX_ARCHIVE_MEMBERS:
            return [_unreadable(name, "ZIP", f"it holds {len(members)} files (limit {_MAX_ARCHIVE_MEMBERS}).")]
        if sum(m.file_size for m in members) > _MAX_ARCHIVE_BYTES:
            return [_unreadable(name, "ZIP", "it unpacks to more than 300 MB.")]
        for member in members:
            inner = PurePosixPath(member.filename).name
            if member.flag_bits & 0x1:
                docs.append(_unreadable(f"{name} / {inner}", "ZIP", "it is password-protected."))
                continue
            for doc in read_upload(archive.read(member), inner, depth + 1):
                doc.name = f"{name} / {doc.name}"
                docs.append(doc)
    if not docs:
        return [_unreadable(name, "ZIP", "the archive holds no files.")]
    return docs
