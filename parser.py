"""
parser.py — PFAS Evaluation Engine
Data ingestion layer: Excel PFAS tables, PDF lab reports, pasted text.

Returns a unified ParsedData object that the engine can consume directly.

Claros R&D Team | Framework Architecture by Zack Liu
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils import (
    PFAS_SPECIES_DB,
    PFAS_ALIASES,
    convert_to_mg_L,
    detect_unit_from_text,
    normalize_pfas_name,
    parse_numeric_value,
)

# ═══════════════════════════════════════════════════════════════════════════════
# PARSED DATA CONTAINER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ParsedData:
    """Unified container for all parsed input data."""

    # Core PFAS concentration data: {sample_name: {analyte_canonical: conc_mg_L}}
    pfas_samples: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # Analytes that were recognized as PFAS but had only ND / non-detect values.
    # {sample_name: [canonical_name, ...]}
    nd_species: Dict[str, List[str]] = field(default_factory=dict)

    # Original unit detected in the data source
    detected_unit: str = "ng/L"

    # Water matrix parameters extracted from text (mg/L or native units as noted)
    matrix_params: Dict[str, float] = field(default_factory=dict)

    # Keywords detected (species names found in text without numeric concentration)
    keyword_species: List[str] = field(default_factory=list)

    # Treatment goals / context extracted from free text
    treatment_goals_text: str = ""
    customer_notes_text: str = ""

    # Diagnostics
    logs: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # Source flags
    has_excel: bool = False
    has_pdf: bool = False
    has_text: bool = False

    def merge(self, other: "ParsedData") -> None:
        """Merge another ParsedData into self (samples are combined)."""
        for sample, data in other.pfas_samples.items():
            if sample in self.pfas_samples:
                self.pfas_samples[sample].update(data)
            else:
                self.pfas_samples[sample] = dict(data)
        for sample, nd_list in other.nd_species.items():
            if sample not in self.nd_species:
                self.nd_species[sample] = []
            for sp in nd_list:
                if sp not in self.nd_species[sample]:
                    self.nd_species[sample].append(sp)
        self.matrix_params.update(other.matrix_params)
        self.keyword_species += [k for k in other.keyword_species if k not in self.keyword_species]
        self.logs += other.logs
        self.warnings += other.warnings
        self.errors += other.errors
        if other.has_excel:
            self.has_excel = True
        if other.has_pdf:
            self.has_pdf = True
        if other.has_text:
            self.has_text = True

    @property
    def has_pfas_data(self) -> bool:
        return bool(self.pfas_samples) and any(self.pfas_samples.values())


# ═══════════════════════════════════════════════════════════════════════════════
# KNOWN PFAS KEY SET (for fast lookup)
# ═══════════════════════════════════════════════════════════════════════════════

_ALL_PFAS_KEYS_UPPER = {k.upper() for k in PFAS_SPECIES_DB} | set(PFAS_ALIASES.keys())
_ALL_PFAS_KEYS_SORTED_BY_LEN = sorted(_ALL_PFAS_KEYS_UPPER, key=len, reverse=True)

# Regex pattern matching any known PFAS abbreviation (word-boundary aware)
_PFAS_ABBREV_PATTERN = re.compile(
    r"\b(" + "|".join(
        re.escape(k) for k in sorted(PFAS_SPECIES_DB.keys(), key=len, reverse=True)
    ) + r")\b",
    flags=re.IGNORECASE,
)


def _looks_like_pfas(text: str) -> bool:
    """Quick check: does this string look like a PFAS analyte name?"""
    upper = text.strip().upper()
    if not upper or upper in ("NAN", "NONE", ""):
        return False
    # Exact key/alias match
    if upper in _ALL_PFAS_KEYS_UPPER:
        return True
    # Regex-based abbreviation match (e.g. "PFOA (ng/L)", "PFOS result")
    if bool(_PFAS_ABBREV_PATTERN.search(text)):
        return True
    # Substring match for full PFAS chemical names not covered by exact alias
    # (e.g. "Perfluoropentane sulfonic acid", "Fluorotelomer carboxylate")
    _CHEM_INDICATORS = (
        "PERFLUORO", "POLYFLUORO", "FLUOROTELOMER",
        "HFPO", "FTOH", "PFESA", "TRIFLUOROACET",
    )
    return any(ind in upper for ind in _CHEM_INDICATORS)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_excel(file_bytes: bytes, filename: str) -> ParsedData:
    """
    Parse an Excel (.xlsx/.xls) or CSV file containing PFAS concentration data.

    Expected layout (flexible detection):
      - Column 0:  PFAS analyte names
      - Column 1+: Sample concentrations (one column per sample)
      - Metadata / header rows at top are automatically skipped
      - Unit is detected from the column header row or top rows
      - Tries ALL sheets; uses the sheet with the most PFAS data

    Returns a ParsedData instance.
    """
    result = ParsedData()
    result.has_excel = True
    logs = result.logs

    # ── Load all sheets ───────────────────────────────────────────────────────
    if filename.lower().endswith(".csv"):
        try:
            df_raw = pd.read_csv(io.BytesIO(file_bytes), header=None, dtype=str)
            sheets = {"Sheet1": df_raw}
        except Exception as e:
            result.errors.append(f"[Excel] Failed to open CSV: {e}")
            return result
    else:
        try:
            sheets = pd.read_excel(
                io.BytesIO(file_bytes), header=None, dtype=str, sheet_name=None
            )
        except Exception as e:
            result.errors.append(f"[Excel] Failed to open file: {e}")
            return result

    logs.append(f"[Excel] '{filename}' — sheets: {list(sheets.keys())}")

    # ── Try each sheet; keep the one with the most PFAS data ─────────────────
    best_result: Optional[ParsedData] = None
    best_score = -1

    for sheet_name, df_raw in sheets.items():
        if df_raw is None or df_raw.empty or len(df_raw.columns) < 2:
            logs.append(f"[Excel] Sheet '{sheet_name}': skipped (empty or <2 columns)")
            continue

        logs.append(
            f"[Excel] Sheet '{sheet_name}': {len(df_raw)} rows × {len(df_raw.columns)} columns"
        )
        sheet_result = _parse_excel_sheet(df_raw, sheet_name, filename, logs)

        # Score: detected concentrations + ND species
        score = sum(len(v) for v in sheet_result.pfas_samples.values()) + sum(
            len(v) for v in sheet_result.nd_species.values()
        )
        if score > best_score:
            best_score = score
            best_result = sheet_result

    if best_result is None:
        result.warnings.append("[Excel] All sheets were empty or had fewer than 2 columns.")
        return result

    # Merge best sheet result into main result
    result.pfas_samples = best_result.pfas_samples
    result.nd_species = best_result.nd_species
    result.detected_unit = best_result.detected_unit
    result.warnings += best_result.warnings
    return result


def _parse_excel_sheet(
    df_raw: "pd.DataFrame",
    sheet_name: str,
    filename: str,
    logs: List[str],
) -> ParsedData:
    """Parse a single Excel sheet DataFrame. Returns a ParsedData."""
    result = ParsedData()

    # ── Step 1: Locate the PFAS data region (scan ALL rows) ──────────────────
    data_start_row = None
    header_row_idx = None

    # Log column A contents for first 30 rows to aid debugging
    col_a_preview = []
    for ri in range(min(30, len(df_raw))):
        col_a_preview.append(f"  row{ri+1}: {str(df_raw.iloc[ri, 0]).strip()!r}")
    logs.append(f"[Excel] Sheet '{sheet_name}' — Col A preview:\n" + "\n".join(col_a_preview))

    for ri in range(len(df_raw)):
        cell_val = str(df_raw.iloc[ri, 0]).strip()
        norm = normalize_pfas_name(cell_val)
        if norm in PFAS_SPECIES_DB or _looks_like_pfas(cell_val):
            data_start_row = ri
            header_row_idx = ri - 1 if ri > 0 else None
            logs.append(
                f"[Excel] Sheet '{sheet_name}': PFAS data region row {ri + 1}+ | "
                f"header row: {header_row_idx + 1 if header_row_idx is not None else 'N/A'} | "
                f"first analyte: {cell_val!r} → {norm!r}"
            )
            break

    if data_start_row is None:
        # Fallback: treat row 0 as header, row 1+ as data
        header_row_idx = 0
        data_start_row = 1
        result.warnings.append(
            f"[Excel] Sheet '{sheet_name}': Could not auto-detect PFAS data region. "
            "No recognized PFAS analyte names found in column A. "
            "Assuming row 1 = header, data from row 2. "
            "Check the Col A preview above in the debug log."
        )

    # ── Step 2: Detect concentration unit ────────────────────────────────────
    unit = "ng/L"
    rows_to_scan_for_unit = list(range(min(8, len(df_raw))))
    if header_row_idx is not None and header_row_idx >= 0:
        if header_row_idx not in rows_to_scan_for_unit:
            rows_to_scan_for_unit.append(header_row_idx)
    # Also scan the first data row
    if data_start_row not in rows_to_scan_for_unit:
        rows_to_scan_for_unit.append(data_start_row)

    for ri in rows_to_scan_for_unit:
        if ri >= len(df_raw):
            continue
        row_text = " ".join(str(v) for v in df_raw.iloc[ri] if str(v) not in ("nan", "None"))
        detected = detect_unit_from_text(row_text)
        if detected:
            unit = detected
            logs.append(f"[Excel] Sheet '{sheet_name}': unit '{unit}' detected from row {ri + 1}")
            break

    result.detected_unit = unit

    # ── Step 3: Extract sample names ────────────────────────────────────────
    n_cols = len(df_raw.columns)

    if header_row_idx is not None and header_row_idx >= 0:
        header_cells = [str(df_raw.iloc[header_row_idx, c]).strip() for c in range(1, n_cols)]
        sample_names = []
        for i, h in enumerate(header_cells):
            if h and h.lower() not in ("nan", "none", ""):
                # Strip unit suffixes from header cells: "Sample A (ng/L)" → "Sample A"
                clean = re.sub(r"\s*[\(\[]\s*(?:ng|µg|ug|mg)/[lL].*?[\)\]]", "", h).strip()
                clean = re.sub(r"\s*[\(\[]\s*(?:ppt|ppb|ppm).*?[\)\]]", "", clean).strip()
                sample_names.append(clean if clean else f"Sample_{i + 1}")
            else:
                sample_names.append(f"Sample_{i + 1}")
    else:
        sample_names = [f"Sample_{i + 1}" for i in range(n_cols - 1)]

    logs.append(f"[Excel] Sheet '{sheet_name}': sample columns {sample_names}")

    # ── Step 4: Parse concentration rows ────────────────────────────────────
    pfas_data: Dict[str, Dict[str, float]] = {s: {} for s in sample_names}
    nd_data: Dict[str, List[str]] = {s: [] for s in sample_names}
    parsed_rows = 0
    nd_rows = 0

    # Skip patterns for metadata rows (only relevant in fallback mode)
    _META_SKIP = re.compile(
        r"^(issue|report|date|client|lab|sample\s*id|collected|received|analysed|"
        r"analyzed|method|version|project|address|phone|email|test|certified|"
        r"accredited|unit|parameter|cas\s*(number|no|#)?|analysis|result|"
        r"detection|quantitation|limit|mdl|rl|mrl|comment|note|page)\b",
        re.IGNORECASE,
    )

    for ri in range(data_start_row, len(df_raw)):
        row = df_raw.iloc[ri]
        analyte_raw = str(row.iloc[0]).strip()

        # Skip empty rows
        if not analyte_raw or analyte_raw.lower() in ("nan", "none", ""):
            continue

        # Skip known total/sum rows
        if analyte_raw.lower() in (
            "total", "sum pfas", "total pfas", "pfas sum",
            "sum", "total pfas (calculated)", "σ pfas",
        ):
            continue

        # In fallback mode, skip rows that are clearly metadata (not PFAS analytes)
        if data_start_row == 1 and _META_SKIP.match(analyte_raw):
            logs.append(f"[Excel] Row {ri + 1}: skipping metadata row: {analyte_raw!r}")
            continue

        analyte = normalize_pfas_name(analyte_raw)

        row_had_numeric = False
        row_had_nd = False
        for ci, sample_name in enumerate(sample_names):
            raw_val = row.iloc[ci + 1] if (ci + 1) < len(row) else None
            val = parse_numeric_value(raw_val)
            if val is not None:
                val_mg_L = convert_to_mg_L(val, unit)
                if val_mg_L is not None:
                    pfas_data[sample_name][analyte] = val_mg_L
                    row_had_numeric = True
                else:
                    result.warnings.append(
                        f"[Excel] Row {ri + 1}: unit '{unit}' conversion failed for '{val}'"
                    )
            else:
                # Check if this is a ND / non-detect marker (not just empty)
                raw_str = str(raw_val).strip().lower() if raw_val is not None else ""
                if raw_str and raw_str not in ("nan", "none", ""):
                    row_had_nd = True

        if row_had_numeric:
            parsed_rows += 1
        elif row_had_nd:
            # Analyte was analyzed but all results were non-detect
            nd_rows += 1
            for sample_name in sample_names:
                if analyte not in nd_data[sample_name]:
                    nd_data[sample_name].append(analyte)

    # Remove empty samples
    pfas_data = {k: v for k, v in pfas_data.items() if v}
    nd_data = {k: v for k, v in nd_data.items() if v}
    result.pfas_samples = pfas_data
    result.nd_species = nd_data

    logs.append(
        f"[Excel] Sheet '{sheet_name}': {parsed_rows} detected rows | "
        f"{nd_rows} non-detect rows | "
        f"{len(pfas_data)} sample(s) | "
        f"{sum(len(v) for v in pfas_data.values())} data points"
    )

    if not pfas_data and not nd_data:
        result.warnings.append(
            f"[Excel] Sheet '{sheet_name}': No PFAS data extracted. "
            "If analyte names use an unsupported format, check the Col A debug preview above."
        )

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PDF PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_pdf(file_bytes: bytes, filename: str) -> ParsedData:
    """
    Parse a PDF lab report.

    Strategy:
      1. Try structured table extraction via pdfplumber.extract_tables()
      2. Fall back to line-by-line regex parsing of extracted text
    """
    result = ParsedData()
    result.has_pdf = True
    logs = result.logs

    try:
        import pdfplumber
    except ImportError:
        result.errors.append(
            "[PDF] pdfplumber is not installed. Run: pip install pdfplumber"
        )
        return result

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            logs.append(f"[PDF] '{filename}' → {len(pdf.pages)} page(s)")

            all_text_lines: List[str] = []
            all_tables: List[List[List[Optional[str]]]] = []

            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
                all_text_lines += page_text.splitlines()

                try:
                    tables = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines_strict",
                            "horizontal_strategy": "lines_strict",
                        }
                    )
                    if not tables:
                        tables = page.extract_tables()
                except Exception:
                    tables = []

                if tables:
                    logs.append(f"[PDF] Page {page_num + 1}: {len(tables)} table(s) found")
                    all_tables.extend(tables)

            # Detect unit from all text
            unit = "ng/L"
            full_text = " ".join(all_text_lines)
            detected = detect_unit_from_text(full_text)
            if detected:
                unit = detected
                logs.append(f"[PDF] Unit '{unit}' detected in document text")
            result.detected_unit = unit

            # Try table parsing first
            if all_tables:
                table_result = _parse_pdf_tables(all_tables, unit, logs)
                if table_result.has_pfas_data:
                    result.pfas_samples = table_result.pfas_samples
                    logs.append(f"[PDF] Table extraction successful → {len(result.pfas_samples)} sample(s)")
                    return result
                else:
                    logs.append("[PDF] Table extraction yielded no PFAS data — falling back to text parsing")

            # Fall back to text parsing
            text_result = _parse_pdf_text(all_text_lines, unit, logs)
            result.pfas_samples = text_result.pfas_samples
            if result.has_pfas_data:
                logs.append(f"[PDF] Text parsing → {len(result.pfas_samples)} sample(s)")
            else:
                result.warnings.append(
                    "[PDF] Could not extract structured PFAS data. "
                    "The PDF may be scanned or have a non-standard layout."
                )

    except Exception as e:
        result.errors.append(f"[PDF] Parse error: {e}")

    return result


def _parse_pdf_tables(
    tables: List[List[List[Optional[str]]]],
    unit: str,
    logs: List[str],
) -> ParsedData:
    """Attempt to parse PFAS data from pdfplumber table structures."""
    result = ParsedData()

    for ti, table in enumerate(tables):
        if not table or len(table) < 2:
            continue

        # Check if this table contains PFAS analyte names
        has_pfas = False
        for row in table[:10]:
            if row and _looks_like_pfas(str(row[0] or "")):
                has_pfas = True
                break

        if not has_pfas:
            continue

        logs.append(f"[PDF] Processing table {ti + 1} ({len(table)} rows)")

        # Extract header (sample names) from first row
        header = table[0]
        sample_names = []
        for ci, cell in enumerate(header[1:], start=1):
            name = str(cell or "").strip()
            if name and name.lower() not in ("nan", "none", ""):
                sample_names.append(name)
            else:
                sample_names.append(f"Sample_{ci}")

        if not sample_names:
            continue

        pfas_data: Dict[str, Dict[str, float]] = {s: {} for s in sample_names}

        for row in table[1:]:
            if not row or not row[0]:
                continue
            analyte_raw = str(row[0]).strip()
            if not analyte_raw or analyte_raw.lower() in ("nan", "none", "total", "sum"):
                continue

            analyte = normalize_pfas_name(analyte_raw)

            for ci, sample_name in enumerate(sample_names):
                if ci + 1 >= len(row):
                    break
                val = parse_numeric_value(row[ci + 1])
                if val is not None:
                    val_mg_L = convert_to_mg_L(val, unit)
                    if val_mg_L is not None:
                        pfas_data[sample_name][analyte] = val_mg_L

        pfas_data = {k: v for k, v in pfas_data.items() if v}
        if pfas_data:
            result.pfas_samples.update(pfas_data)

    return result


def _parse_pdf_text(lines: List[str], unit: str, logs: List[str]) -> ParsedData:
    """
    Fallback: extract PFAS data from raw text lines.
    Looks for lines containing a PFAS name followed by a number.
    Builds a single pseudo-sample named 'PDF_Sample'.
    """
    result = ParsedData()
    sample_data: Dict[str, float] = {}

    # Pattern: PFAS name (possibly with spaces/colons) followed by numeric value
    value_pattern = re.compile(
        r"(?P<conc>[0-9]+(?:[.,][0-9]+)?(?:[eE][+-]?[0-9]+)?)"
    )

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if line contains a PFAS name
        pfas_match = _PFAS_ABBREV_PATTERN.search(line)
        if not pfas_match:
            continue

        analyte_raw = pfas_match.group(0)
        analyte = normalize_pfas_name(analyte_raw)

        # Extract the first numeric value from the rest of the line
        rest = line[pfas_match.end():]
        num_match = value_pattern.search(rest)
        if num_match:
            raw_val = num_match.group("conc")
            val = parse_numeric_value(raw_val)
            if val is not None:
                val_mg_L = convert_to_mg_L(val, unit)
                if val_mg_L is not None and analyte not in sample_data:
                    sample_data[analyte] = val_mg_L

    if sample_data:
        result.pfas_samples["PDF_Sample"] = sample_data
        logs.append(f"[PDF] Text extraction: {len(sample_data)} analytes in pseudo-sample 'PDF_Sample'")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT / EMAIL PARSER
# ═══════════════════════════════════════════════════════════════════════════════

# Matrix parameter extraction patterns — updated to align with spec M3 required inputs
_MATRIX_PATTERNS: Dict[str, List[Tuple[str, str]]] = {
    # ── Required per spec ────────────────────────────────────────────────────
    "COD": [
        (r"COD\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"chemical\s+oxygen\s+demand\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "TOC": [
        (r"(?:TOC|total\s+organic\s+carbon)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "nitrate": [
        (r"(?:NO3|nitrate(?![-\s]*N))\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"nitrate[-\s]*N\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L-N"),  # as-N form
    ],
    "NO2": [
        (r"(?:NO2|nitrite(?![-\s]*N))\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"nitrite[-\s]*N\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L-N"),  # as-N form
    ],
    # ── Recommended per spec ─────────────────────────────────────────────────
    "UV254": [
        (r"UV254\s*[=:~≈]\s*([0-9.]+)\s*(?:cm-1|/cm|abs)?", "cm-1"),
        (r"UV\s*@?\s*254\s*nm\s*[=:~≈]\s*([0-9.]+)", "cm-1"),
    ],
    "UVT254": [
        (r"UVT(?:254)?\s*[=:~≈]\s*([0-9.]+)\s*%?", "%"),
        (r"UV\s+transmittance\s*[=:~≈]\s*([0-9.]+)\s*%?", "%"),
    ],
    "sample_color": [
        (r"(?:sample\s+)?colou?r\s*[=:~≈]\s*([A-Za-z]+)", "text"),
    ],
    # ── Conditional per spec ─────────────────────────────────────────────────
    "chloride": [
        (r"(?:Cl-?|chloride)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "fluoride": [
        (r"(?:F-?|fluoride)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "hardness": [
        (r"hardness\s*[=:~≈]\s*([0-9,]+)\s*(mg/[lL].*?CaCO3|mg/[lL]|ppm)", "mg/L"),
        (r"total\s+hardness\s*[=:~≈]\s*([0-9,]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    # ── Supplementary (adsorption/membrane context) ──────────────────────────
    "DOC": [
        (r"(?:DOC|dissolved\s+organic\s+carbon)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "TDS": [
        (r"TDS\s*[=:~≈]\s*([0-9,]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"total\s+dissolved\s+solids\s*[=:~≈]\s*([0-9,]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "sulfate": [
        (r"(?:SO4|sulfate|sulphate)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "pH": [
        (r"pH\s*[=:~≈]\s*([0-9.]+)", "dimensionless"),
    ],
    "turbidity": [
        (r"turbidity\s*[=:~≈]\s*([0-9.]+)\s*(?:NTU|ntu)?", "NTU"),
    ],
    "TSS": [
        (r"TSS\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"total\s+suspended\s+solids\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "temperature": [
        (r"temp(?:erature)?\s*[=:~≈]\s*([0-9.]+)\s*[°]?[Cc]", "°C"),
    ],
    "flow_rate": [
        (r"flow\s+rate?\s*[=:~≈]\s*([0-9,.]+)\s*(MGD|gpd|gpm|m3/h|m3/d|L/d|L/h)", "raw"),
    ],
}


def parse_text(email_text: str, goals_text: str = "") -> ParsedData:
    """
    Parse pasted email / customer notes and treatment goals text.

    Extracts:
    - PFAS concentrations mentioned inline (e.g. "PFOA: 250 ng/L")
    - PFAS keyword mentions (species names without quantification)
    - Water matrix parameters
    - Treatment goal context (stored as raw text)

    Returns a ParsedData instance.
    """
    result = ParsedData()
    result.has_text = bool(email_text.strip() or goals_text.strip())
    result.treatment_goals_text = goals_text.strip()
    result.customer_notes_text = email_text.strip()

    combined = f"{email_text}\n{goals_text}"
    logs = result.logs

    if not combined.strip():
        return result

    logs.append(f"[Text] Parsing {len(combined)} characters of input text")

    # ── Detect unit from text ────────────────────────────────────────────────
    unit = detect_unit_from_text(combined) or "ng/L"
    result.detected_unit = unit

    # ── Extract inline PFAS concentrations ──────────────────────────────────
    # Pattern: <PFAS_NAME> <optional separator> <number> <optional unit>
    inline_pattern = re.compile(
        r"(?P<pfas>" + "|".join(
            re.escape(k) for k in sorted(PFAS_SPECIES_DB.keys(), key=len, reverse=True)
        ) + r")"
        r"[\s:=,\-–]+?"
        r"(?P<val>[0-9]+(?:[.,][0-9]+)?(?:[eE][+-]?[0-9]+)?)"
        r"\s*(?P<unit>ng/[lL]|µg/[lL]|ug/[lL]|mg/[lL]|ppm|ppb|ppt)?",
        flags=re.IGNORECASE,
    )

    inline_data: Dict[str, float] = {}
    for m in inline_pattern.finditer(combined):
        analyte = normalize_pfas_name(m.group("pfas"))
        raw_val = m.group("val").replace(",", "")
        val_unit = m.group("unit") or unit
        try:
            val = float(raw_val)
        except ValueError:
            continue
        val_mg_L = convert_to_mg_L(val, val_unit)
        if val_mg_L is not None and analyte not in inline_data:
            inline_data[analyte] = val_mg_L

    if inline_data:
        result.pfas_samples["Text_Input"] = inline_data
        logs.append(f"[Text] Extracted {len(inline_data)} inline PFAS concentration(s)")

    # ── Extract keyword-only PFAS mentions (no concentration found) ──────────
    kw_found: List[str] = []
    for m in _PFAS_ABBREV_PATTERN.finditer(combined):
        name = normalize_pfas_name(m.group(0))
        if name not in inline_data and name not in kw_found:
            kw_found.append(name)
    result.keyword_species = kw_found
    if kw_found:
        logs.append(f"[Text] Keyword PFAS mentions (no concentration): {kw_found}")

    # ── Extract matrix parameters ─────────────────────────────────────────────
    matrix: Dict[str, float] = {}
    for param, patterns in _MATRIX_PATTERNS.items():
        for pat, pat_unit in patterns:
            m = re.search(pat, combined, re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    matrix[param] = val
                    logs.append(f"[Text] Matrix param: {param} = {val} {pat_unit}")
                    break
                except (ValueError, IndexError):
                    pass

    result.matrix_params = matrix

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED PARSE ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def parse_all(
    excel_bytes: Optional[bytes],
    excel_filename: Optional[str],
    pdf_bytes: Optional[bytes],
    pdf_filename: Optional[str],
    email_text: str,
    goals_text: str,
) -> ParsedData:
    """
    Parse all available input sources and return a merged ParsedData.

    Priority for conflicting concentration values: Excel > PDF > Text.
    """
    combined = ParsedData()
    combined.logs.append("=== Parser started ===")

    # Parse Excel
    if excel_bytes and excel_filename:
        excel_result = parse_excel(excel_bytes, excel_filename)
        combined.merge(excel_result)
        combined.detected_unit = excel_result.detected_unit

    # Parse PDF
    if pdf_bytes and pdf_filename:
        pdf_result = parse_pdf(pdf_bytes, pdf_filename)
        # Don't overwrite Excel samples — add as separate samples
        for sample, data in pdf_result.pfas_samples.items():
            if sample not in combined.pfas_samples:
                combined.pfas_samples[sample] = data
        combined.matrix_params.update(
            {k: v for k, v in pdf_result.matrix_params.items() if k not in combined.matrix_params}
        )
        combined.logs += pdf_result.logs
        combined.warnings += pdf_result.warnings
        combined.errors += pdf_result.errors
        if pdf_result.has_pdf:
            combined.has_pdf = True

    # Parse text
    text_result = parse_text(email_text, goals_text)
    combined.treatment_goals_text = text_result.treatment_goals_text
    combined.customer_notes_text = text_result.customer_notes_text
    combined.keyword_species += [
        k for k in text_result.keyword_species if k not in combined.keyword_species
    ]
    combined.matrix_params.update(
        {k: v for k, v in text_result.matrix_params.items() if k not in combined.matrix_params}
    )
    if text_result.has_text:
        combined.has_text = True
        # Only add text PFAS data if no structured data from files
        if not combined.has_pfas_data and text_result.has_pfas_data:
            combined.pfas_samples.update(text_result.pfas_samples)
    combined.logs += text_result.logs
    combined.warnings += text_result.warnings

    combined.logs.append(
        f"=== Parse complete: {len(combined.pfas_samples)} sample(s), "
        f"{len(combined.matrix_params)} matrix param(s), "
        f"{len(combined.keyword_species)} keyword(s) ==="
    )

    return combined
