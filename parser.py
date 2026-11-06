"""
parser.py — PFAS Evaluation Engine
Data ingestion layer: Excel PFAS tables, PDF lab reports, pasted text.

Returns a unified ParsedData object that the engine can consume directly.

Claros Product Development Team | Framework Architecture by Zack Liu
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from file_readers import KIND_DOCUMENT, KIND_PDF, KIND_SPREADSHEET, KIND_UNREADABLE, ReadDocument, load_sheets
from utils import (
    PFAS_SPECIES_DB,
    PFAS_ALIASES,
    _ND_TOKENS,
    convert_flow_to_gpm,
    convert_to_mg_L,
    detect_unit_from_text,
    normalize_pfas_name,
    parse_numeric_value,
)

# ═══════════════════════════════════════════════════════════════════════════════
# PARSED DATA CONTAINER
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SampleMetadata:
    """
    Semantic metadata about a sample column, populated by the LLM pre-parser.
    Carries information that the rule-based parser cannot derive from structure alone.
    """
    # True when this column represents a statistical roll-up (Average, Maximum …)
    # rather than an independent physical measurement.
    is_statistical_summary: bool = False
    # "average" | "maximum" | "minimum" | "median" | "typical" | ""
    summary_type: str = ""


@dataclass
class ParsedData:
    """Unified container for all parsed input data."""

    # Core PFAS concentration data: {sample_name: {analyte_canonical: conc_mg_L}}
    pfas_samples: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # Analytes that were recognized as PFAS but had only ND / non-detect values.
    # {sample_name: [canonical_name, ...]}
    nd_species: Dict[str, List[str]] = field(default_factory=dict)

    # Bulk organofluorine measurements captured from PFAS sheet (excluded from
    # species composition but used for theoretical TOF comparison).
    # {sample_name: {"AOF": value_mg_L_as_F, "TOF": value_mg_L_as_F}}
    aof_tof_data: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # Original unit detected in the data source
    detected_unit: str = "ng/L"

    # Water matrix parameters that are not tied to one sample, e.g. from the
    # customer's notes (mg/L or native units as noted). They apply to every sample.
    matrix_params: Dict[str, float] = field(default_factory=dict)

    # Water matrix measured on a specific sample: {sample: {param: value}}.
    # Where the source gives a range, the value is the maximum (screening basis).
    sample_matrix: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Min / avg / max as reported, for PFAS species and matrix parameters that
    # have more than one of them: {sample: {name: {"min"|"avg"|"max": value}}}
    sample_ranges: Dict[str, Dict[str, Dict[str, float]]] = field(default_factory=dict)
    # Flow and operating pattern of a sample's stream. Keys, all optional:
    # min_gpm, avg_gpm, max_gpm, unit, min, avg, max (as reported),
    # hours_min, hours_avg, hours_max (per day), daily_volume, daily_volume_unit
    sample_flow: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Average PFAS values of a sample whose pfas_samples entry holds its maxima
    sample_avg: Dict[str, Dict[str, float]] = field(default_factory=dict)
    # Species of such a sample that have an average but no maximum
    sample_max_missing: Dict[str, List[str]] = field(default_factory=dict)
    # What a sample's values are when they are not single measurements:
    # "average" | "maximum" | "minimum" | "median"
    sample_basis: Dict[str, str] = field(default_factory=dict)
    # File each sample was read from
    sample_source: Dict[str, str] = field(default_factory=dict)
    # Label spanning the value columns of the table a sample came from
    # (parser-internal, e.g. "Influent Water" above Min / Avg / Max)
    stream_hint: str = ""

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
    has_other: bool = False

    # Which files were read and how, as {"file", "format", "read_as", "note"}
    source_files: List[Dict[str, str]] = field(default_factory=list)
    # What the user must check by eye: {"level": "critical"|"warning", "message"}
    data_checks: List[Dict[str, str]] = field(default_factory=list)

    # ── LLM pre-parser outputs (populated only when LLM mode is active) ──────
    # Per-sample semantic metadata (statistical summary flags, etc.)
    sample_metadata: Dict[str, SampleMetadata] = field(default_factory=dict)
    # Notes and warnings produced by the LLM during extraction
    llm_parse_notes: List[str] = field(default_factory=list)
    # Raw JSON string returned by the LLM (for debug tab)
    llm_raw_response: str = ""
    # Project context extracted by LLM (customer, site, country, flow rate)
    llm_project_context: Dict[str, Any] = field(default_factory=dict)

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
        for key, value in other.matrix_params.items():
            self.matrix_params.setdefault(key, value)
        for mine, theirs in (
            (self.sample_matrix, other.sample_matrix),
            (self.sample_ranges, other.sample_ranges),
            (self.sample_flow, other.sample_flow),
            (self.sample_avg, other.sample_avg),
        ):
            for sample, values in theirs.items():
                target = mine.setdefault(sample, {})
                for key, value in values.items():
                    target.setdefault(key, value)
        for sample, species in other.sample_max_missing.items():
            target_list = self.sample_max_missing.setdefault(sample, [])
            target_list += [sp for sp in species if sp not in target_list]
        for sample, basis in other.sample_basis.items():
            self.sample_basis.setdefault(sample, basis)
        for sample, source in other.sample_source.items():
            self.sample_source.setdefault(sample, source)
        for sample, bulk in other.aof_tof_data.items():
            if sample not in self.aof_tof_data:
                self.aof_tof_data[sample] = {}
            self.aof_tof_data[sample].update(bulk)
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
        if other.has_other:
            self.has_other = True
        self.source_files += other.source_files
        self.data_checks += other.data_checks
        self.sample_metadata.update(other.sample_metadata)
        self.llm_parse_notes += other.llm_parse_notes
        for key, value in other.llm_project_context.items():
            self.llm_project_context.setdefault(key, value)
        if other.llm_raw_response:
            self.llm_raw_response = other.llm_raw_response

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
    try:
        sheets = load_sheets(file_bytes, filename)
    except Exception as e:
        result.errors.append(f"[Excel] Failed to open file: {e}")
        return result

    logs.append(f"[Excel] '{filename}', sheets: {list(sheets.keys())}")

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
    result.aof_tof_data = best_result.aof_tof_data

    result.stream_hint = best_result.stream_hint

    # ── Water matrix and flow from all sheets ────────────────────────────────
    _attach_water_data(result, sheets, filename)
    if result.matrix_params or result.sample_matrix or result.sample_flow:
        if not result.has_pfas_data and not result.nd_species:
            # A water-quality-only workbook is expected when PFAS results
            # arrive in a separate file — not a parse failure.
            result.warnings = [w for w in result.warnings if "PFAS data" not in w]
            logs.append(f"[Excel] '{filename}': no PFAS results; read as water-quality data")

    return result


def _detect_transposed_layout(df: "pd.DataFrame") -> bool:
    """
    Returns True when the sheet uses a *transposed* layout:
      • Row 0, columns 1+  →  PFAS analyte abbreviations (e.g. PFOS, PFOA, PFBS …)
      • Column A, rows 1+  →  sample descriptors (e.g. "Average concentration since 2020")

    This is the mirror-image of the normal layout where analyte names appear in
    column A and sample identifiers appear across the header row.

    A layout is flagged as transposed when ≥ 2 cells in row 0, columns 1+, pass
    the _looks_like_pfas() test.  The check is only reached when no PFAS name
    has been found in column A, so false positives are rare.
    """
    if len(df) < 2 or len(df.columns) < 2:
        return False

    pfas_hits = 0
    for ci in range(1, min(len(df.columns), 30)):
        cell = str(df.iloc[0, ci]).strip()
        if cell.lower() in ("nan", "none", ""):
            continue
        if _looks_like_pfas(cell):
            pfas_hits += 1
            if pfas_hits >= 2:
                return True
    return False


def _parse_transposed_excel_sheet(
    df_raw: "pd.DataFrame",
    sheet_name: str,
    filename: str,
    logs: List[str],
) -> ParsedData:
    """
    Parse a *transposed* Excel sheet where:
      • Row 0 contains PFAS analyte names in columns 1+
      • Column A contains sample names / statistical descriptors in rows 1+
      • Data cells are at the intersection (row > 0, col > 0)

    Automatically sets SampleMetadata.is_statistical_summary for rows whose
    name includes "Average", "Maximum", "Minimum", etc.
    """
    result = ParsedData()
    logs.append(
        f"[Excel] Sheet '{sheet_name}': TRANSPOSED LAYOUT detected: "
        "PFAS species in row 0, samples in column A."
    )

    # ── Step 1: Detect unit from top rows ────────────────────────────────────
    unit = "ng/L"
    for ri in range(min(5, len(df_raw))):
        row_text = " ".join(
            str(v) for v in df_raw.iloc[ri] if str(v) not in ("nan", "None")
        )
        detected = detect_unit_from_text(row_text)
        if detected:
            unit = detected
            logs.append(
                f"[Excel] Transposed '{sheet_name}': unit '{unit}' detected from row {ri + 1}"
            )
            break
    result.detected_unit = unit

    # ── Step 2: Map column indices → canonical PFAS analyte names ───────────
    n_cols = len(df_raw.columns)
    analyte_map: Dict[int, str] = {}  # col_index → canonical name
    aof_cols: Dict[int, str] = {}     # col_index → "AOF" | "TOF"

    for ci in range(1, n_cols):
        raw = str(df_raw.iloc[0, ci]).strip()
        if raw.lower() in ("nan", "none", ""):
            continue
        raw_lower = raw.lower()
        if raw_lower in (
            "aof", "total aof", "adsorbable organic fluorine",
        ):
            aof_cols[ci] = "AOF"
        elif raw_lower in (
            "tof", "total organic fluorine", "total fluorine",
            "total oxidizable fluorine",
        ):
            aof_cols[ci] = "TOF"
        elif _looks_like_pfas(raw):
            analyte_map[ci] = normalize_pfas_name(raw)

    logs.append(
        f"[Excel] Transposed '{sheet_name}': "
        f"{len(analyte_map)} PFAS analyte columns, "
        f"{len(aof_cols)} AOF/TOF columns"
    )

    if not analyte_map and not aof_cols:
        result.warnings.append(
            f"[Excel] Sheet '{sheet_name}': Transposed layout suspected but "
            "no PFAS analyte names found in row 0."
        )
        return result

    # ── Step 3: Parse rows 1+ — each row is one sample ──────────────────────
    _STAT_MARKERS = [
        ("average", "average"), ("avg", "average"), ("mean", "average"),
        ("maximum", "maximum"), ("max conc", "maximum"),
        ("minimum", "minimum"), ("min conc", "minimum"),
        ("median", "median"),
        ("typical", "typical"), ("representative", "typical"),
    ]

    pfas_data: Dict[str, Dict[str, float]] = {}
    nd_data: Dict[str, List[str]] = {}

    for ri in range(1, len(df_raw)):
        sample_raw = str(df_raw.iloc[ri, 0]).strip()
        if not sample_raw or sample_raw.lower() in ("nan", "none", ""):
            continue

        # Strip unit suffixes from the sample name label
        sample_name = re.sub(
            r"\s*[\(\[]\s*(?:ng|µg|ug|mg)/[lL].*?[\)\]]", "", sample_raw
        ).strip()
        sample_name = re.sub(
            r"\s*[\(\[]\s*(?:ppt|ppb|ppm).*?[\)\]]", "", sample_name
        ).strip()
        if not sample_name:
            sample_name = f"Sample_{ri}"

        # Detect statistical summary type
        is_stat = False
        stat_type = ""
        _sl = sample_name.lower()
        for marker, stype in _STAT_MARKERS:
            if marker in _sl:
                is_stat = True
                stat_type = stype
                break

        if is_stat:
            result.sample_metadata[sample_name] = SampleMetadata(
                is_statistical_summary=True, summary_type=stat_type
            )
            logs.append(
                f"[Excel] Transposed row {ri + 1}: '{sample_name}' → "
                f"statistical summary ({stat_type})"
            )

        pfas_data[sample_name] = {}
        nd_data[sample_name] = []

        # PFAS analyte cells
        for ci, analyte in analyte_map.items():
            if ci >= len(df_raw.columns):
                continue
            raw_val = df_raw.iloc[ri, ci]
            val = parse_numeric_value(raw_val)
            if val is not None:
                val_mg_L = convert_to_mg_L(val, unit)
                if val_mg_L is not None:
                    pfas_data[sample_name][analyte] = val_mg_L
            else:
                raw_str = (
                    str(raw_val).strip().lower() if raw_val is not None else ""
                )
                if raw_str and raw_str not in ("nan", "none", ""):
                    if analyte not in nd_data[sample_name]:
                        nd_data[sample_name].append(analyte)

        # AOF / TOF cells
        for ci, bulk_key in aof_cols.items():
            if ci >= len(df_raw.columns):
                continue
            raw_val = df_raw.iloc[ri, ci]
            val = parse_numeric_value(raw_val)
            if val is not None:
                val_mg_L = convert_to_mg_L(val, unit)
                if val_mg_L is not None:
                    if sample_name not in result.aof_tof_data:
                        result.aof_tof_data[sample_name] = {}
                    result.aof_tof_data[sample_name][bulk_key] = val_mg_L

    # Remove empty samples
    pfas_data = {k: v for k, v in pfas_data.items() if v}
    nd_data = {k: v for k, v in nd_data.items() if v}
    result.pfas_samples = pfas_data
    result.nd_species = nd_data

    logs.append(
        f"[Excel] Transposed '{sheet_name}': {len(pfas_data)} sample(s) | "
        f"{sum(len(v) for v in pfas_data.values())} data points"
    )

    if not pfas_data and not nd_data:
        result.warnings.append(
            f"[Excel] Sheet '{sheet_name}': Transposed layout detected but "
            "no PFAS data could be extracted. "
            "Verify that row 0 column headers are recognised PFAS abbreviations."
        )

    return result


def _is_nd_marker(raw: Any) -> bool:
    """True for a non-detect entry such as '<2', 'ND' or 'not detected'."""
    text = str(raw).strip().lower() if raw is not None else ""
    if not text or text in ("nan", "none"):
        return False
    if text[0] in "<≤" or text in _ND_TOKENS:
        return True
    return bool(re.match(r"n\.?d\.?\b|non[- ]?detect|not detected|below\b|bdl\b", text))


def _group_label(df: "pd.DataFrame", row: int) -> str:
    """The single label spanning a table's value columns, e.g. 'Influent Water'."""
    labels = {
        str(df.iloc[row, c]).strip() for c in range(1, len(df.columns))
    } - {"", "nan", "None"}
    if len(labels) != 1:
        return ""
    label = labels.pop()
    if parse_numeric_value(label) is not None or ":" in label or len(label) > 60:
        return ""
    return label


_NOT_A_SAMPLE = re.compile(
    r"^\s*(?:rl|mdl|mrl|dl|lod|loq|pql|sdl|reporting\s+limit|detection\s+limit|"
    r"quantitation\s+limit|limit|cas|cas\s*(?:no|number|#|rn)\.?|method|units?|uom|"
    r"qual(?:ifier)?|flags?|q|dilution|df|spike|recovery|%\s*rec)\s*$",
    re.IGNORECASE,
)


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
    logs.append(f"[Excel] Sheet '{sheet_name}', col A preview:\n" + "\n".join(col_a_preview))

    for ri in range(len(df_raw)):
        cell_val = str(df_raw.iloc[ri, 0]).strip()
        norm = normalize_pfas_name(cell_val)
        if norm in PFAS_SPECIES_DB or _looks_like_pfas(cell_val):
            data_start_row = ri
            header_row_idx = ri - 1 if ri > 0 else None
            # Chemistry or unit rows can sit between the sample names and the first
            # PFAS row; their values must not be taken for sample names.
            while header_row_idx:
                above = str(df_raw.iloc[header_row_idx, 0]).strip()
                if above.lower() in ("unit", "units", "uom") or _row_kind(above) in (
                    "matrix", "flow", "hours", "volume"
                ):
                    header_row_idx -= 1
                else:
                    break
            logs.append(
                f"[Excel] Sheet '{sheet_name}': PFAS data region row {ri + 1}+ | "
                f"header row: {header_row_idx + 1 if header_row_idx is not None else 'N/A'} | "
                f"first analyte: {cell_val!r} → {norm!r}"
            )
            break

    if data_start_row is None:
        # Before falling back to generic layout, check whether the sheet is in
        # *transposed* orientation (PFAS species as column headers in row 0,
        # sample descriptors in column A).  This handles lab-report exports such
        # as "Average/Maximum concentration since 2020" in column A with PFAS
        # abbreviations spread across the header row.
        if _detect_transposed_layout(df_raw):
            logs.append(
                f"[Excel] Sheet '{sheet_name}': delegating to transposed parser"
            )
            return _parse_transposed_excel_sheet(df_raw, sheet_name, filename, logs)

        # No PFAS names anywhere but recognisable water-quality parameters:
        # this is a general-chemistry sheet. Its values are picked up by the
        # matrix extractor; reading it as PFAS would invent fake species.
        def _wq_hits(cells) -> int:
            return sum(1 for c in cells if _keyword_match(str(c).strip().lower()))

        wq_in_col_a = _wq_hits(df_raw.iloc[:, 0])
        wq_in_header = max(
            (_wq_hits(df_raw.iloc[ri]) for ri in range(min(40, len(df_raw)))), default=0
        )
        if wq_in_col_a >= 2 or wq_in_header >= 2:
            logs.append(
                f"[Excel] Sheet '{sheet_name}': no PFAS analytes, water-quality parameters "
                "found; treated as a water-quality sheet"
            )
            return result

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
        # A chemistry row ("COD = 300 mg/L") says nothing about the PFAS unit, and
        # taking its mg/L for ng/L results would be a million-fold error.
        first_cell = str(df_raw.iloc[ri, 0]).strip()
        if _keyword_match(first_cell.lower()) and not _looks_like_pfas(first_cell):
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
    if header_row_idx:
        result.stream_hint = _group_label(df_raw, header_row_idx - 1)

    unit_col: Optional[int] = None
    if header_row_idx is not None and header_row_idx >= 0:
        for ci in range(1, n_cols):
            header = str(df_raw.iloc[header_row_idx, ci]).strip().lower()
            if header in ("unit", "units", "uom", "unité", "unités", "einheit", "unidad"):
                unit_col = ci
                break

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

    # Forward-fill column A to handle merged cells (merged analyte name cells
    # appear as value in the first row of the merge, NaN in the rest).
    df_raw = _forward_fill_col_a(df_raw)

    for ri in range(data_start_row, len(df_raw)):
        row = df_raw.iloc[ri]
        analyte_raw = str(row.iloc[0]).strip()

        # Skip empty rows
        if not analyte_raw or analyte_raw.lower() in ("nan", "none", ""):
            continue

        # Skip known total/sum rows and bulk sum parameters (not individual species)
        _analyte_lower = analyte_raw.lower().strip()

        # ── Capture AOF / TOF before skipping (used for theoretical TOF comparison) ──
        # These are bulk organofluorine measures — NOT individual PFAS species.
        # We exclude them from composition to avoid distortion, but store the
        # numeric values so the engine can compare theoretical TOF vs measured AOF/TOF.
        _AOF_MARKERS = frozenset({
            "aof", "total aof", "aof (ng/l)", "aof (µg/l)",
            "adsorbable organic fluorine",
        })
        _TOF_MARKERS = frozenset({
            "tof", "total organic fluorine", "total fluorine",
            "total oxidizable fluorine",
        })
        if _analyte_lower in _AOF_MARKERS or _analyte_lower in _TOF_MARKERS:
            bulk_key = "AOF" if _analyte_lower in _AOF_MARKERS else "TOF"
            for ci, sample_name in enumerate(sample_names):
                raw_val = row.iloc[ci + 1] if (ci + 1) < len(row) else None
                val = parse_numeric_value(raw_val)
                if val is not None:
                    val_mg_L = convert_to_mg_L(val, unit)
                    if val_mg_L is not None:
                        if sample_name not in result.aof_tof_data:
                            result.aof_tof_data[sample_name] = {}
                        result.aof_tof_data[sample_name][bulk_key] = val_mg_L
            logs.append(
                f"[Excel] Row {ri + 1}: captured {bulk_key} ({analyte_raw!r}) for TOF analysis"
            )
            continue  # Skip from PFAS composition

        if _analyte_lower in (
            "total", "sum pfas", "total pfas", "pfas sum",
            "sum", "total pfas (calculated)", "σ pfas",
            # AOF/AOX are bulk sum parameters, NOT individual PFAS species.
            # Including them distorts composition percentages and suppresses
            # FTOH/short-chain fractions. Always exclude them.
            "aof", "aox",
            "total aof", "total aox",
            "aof (ng/l)", "aox (ng/l)", "aof (µg/l)", "aox (µg/l)",
            "adsorbable organic fluorine",
            "adsorbable organic halides", "adsorbable organic halogens",
            "extractable organic fluorine", "eof",
            "top assay", "top",
            "sum parameter", "sum parameters",
            # TOF / Total Organic Fluorine — bulk measure, not individual species
            "tof", "total organic fluorine", "total fluorine",
            "total oxidizable fluorine",
        ):
            logs.append(f"[Excel] Row {ri + 1}: skipping sum/bulk parameter: {analyte_raw!r}")
            continue

        # In fallback mode, skip rows that are clearly metadata (not PFAS analytes)
        if data_start_row == 1 and _META_SKIP.match(analyte_raw):
            logs.append(f"[Excel] Row {ri + 1}: skipping metadata row: {analyte_raw!r}")
            continue

        analyte = normalize_pfas_name(analyte_raw)

        row_unit = unit
        if unit_col is not None and unit_col < len(row):
            row_unit = detect_unit_from_text(str(row.iloc[unit_col])) or unit

        if (
            analyte not in PFAS_SPECIES_DB
            and not _looks_like_pfas(analyte_raw)
            and _keyword_match(_analyte_lower)
        ):
            logs.append(
                f"[Excel] Row {ri + 1}: {analyte_raw!r} is a water-quality parameter; "
                "left to matrix extraction"
            )
            continue

        row_had_numeric = False
        row_had_nd = False
        for ci, sample_name in enumerate(sample_names):
            raw_val = row.iloc[ci + 1] if (ci + 1) < len(row) else None
            val = parse_numeric_value(raw_val)
            if val is not None:
                val_mg_L = convert_to_mg_L(val, row_unit)
                if val_mg_L is not None:
                    pfas_data[sample_name][analyte] = val_mg_L
                    row_had_numeric = True
                else:
                    result.warnings.append(
                        f"[Excel] Row {ri + 1}: unit '{unit}' conversion failed for '{val}'"
                    )
            elif _is_nd_marker(raw_val):
                # Non-detect is recorded for this sample only. A blank cell is
                # missing data, and a units cell is not a result at all.
                row_had_nd = True
                if analyte not in nd_data[sample_name]:
                    nd_data[sample_name].append(analyte)

        if row_had_numeric:
            parsed_rows += 1
        elif row_had_nd:
            nd_rows += 1

    # Remove empty samples, and columns that describe the result instead of being one
    pfas_data = {k: v for k, v in pfas_data.items() if v and not _NOT_A_SAMPLE.match(k)}
    nd_data = {k: v for k, v in nd_data.items() if v and not _NOT_A_SAMPLE.match(k)}
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
# MATRIX PARAMETER EXTRACTION FROM EXCEL
# ═══════════════════════════════════════════════════════════════════════════════

# Keywords for matching water-quality parameter names found in Excel cells.
#
# Supports two table orientations:
#   ROW layout    – parameter name in column A, one row per parameter
#   COLUMN layout – parameter names in a header row, one column per parameter
#
# Keys = canonical param names used by the engine.
# Values = lowercase match strings.
#
# Matching rules (see _keyword_match):
#   • Exact match always tried first.
#   • Starts-with match allowed for keywords with len > 2 (prevents "fe" matching "feed").
#   • Pre-comma prefix tried: "Iron, dissolved" → try prefix "iron".
#   • Parenthetical qualifier stripped: "Iron (total)" → try "iron".
#
# Languages covered: English, French, German, Dutch, Spanish, Italian, Portuguese.
_MATRIX_EXCEL_KEYWORDS: Dict[str, List[str]] = {
    # ── Oxygen demand ────────────────────────────────────────────────────────
    "COD": [
        "cod", "chemical oxygen demand",
        "dco", "demande chimique en oxygène", "demande chimique en oxygene",
        "csb", "chemischer sauerstoffbedarf",   # German
        "czv", "chemisch zuurstofverbruik",      # Dutch
        "dqo", "demanda quimica de oxigeno",     # Spanish/Portuguese
        "richiesta chimica ossigeno",             # Italian
    ],
    "BOD": [
        "bod", "bod5", "bod 5", "bod7",
        "biological oxygen demand", "biochemical oxygen demand",
        "dbo", "dbo5",                           # French/Spanish
        "bsb", "bsb5", "bsb 5", "bsb7",         # German (biochemischer sauerstoffbedarf)
        "bzv", "bzv5",                           # Dutch
    ],
    "TOC": [
        "toc", "total organic carbon",
        "carbone organique total", "cot",        # French
        "gesamter organischer kohlenstoff",      # German
        "totaal organisch koolstof",             # Dutch
        "carbono organico total",               # Spanish/Portuguese
    ],
    "DOC": [
        "doc", "dissolved organic carbon",
        "carbone organique dissous",             # French
        "gelöster organischer kohlenstoff",      # German
    ],
    # ── Nitrogen species ─────────────────────────────────────────────────────
    "TKN": [
        "tkn",
        "total kjeldahl nitrogen", "kjeldahl nitrogen", "nitrogen kjeldahl",
        "ntk", "azote kjeldahl", "azote total kjeldahl",  # French
        "kjeldahl-stickstoff", "stickstoff kjeldahl",     # German
        "kjeldahl-stikstof",                              # Dutch
        "nitrogeno kjeldahl",                             # Spanish
    ],
    "ammonia": [
        "nh3", "nh4",
        "ammonia", "ammonium", "ammoniacal nitrogen",
        "nh3-n", "nh4-n", "nh4+",
        "ammoniac", "azote ammoniacal", "azote nh4", "azote ammonium",  # French
        "ammonium-stickstoff", "ammoniak",                               # German
        "ammoniumstikstof",                                              # Dutch
        "nitrógeno amoniacal", "amoniaco",                               # Spanish
    ],
    "nitrate": [
        "no3", "nitrate", "nitrates", "no3-n",
        "azote nitrate", "azote no3",            # French
        "nitrat", "nitrat-stickstoff",           # German
        "nitraat",                               # Dutch
        "nitrato",                               # Spanish/Italian/Portuguese
    ],
    "NO2": [
        "no2", "nitrite", "nitrites", "no2-n",
        "nitrit",                                # German
        "nitriet",                               # Dutch
        "nitrito",                               # Spanish/Italian
    ],
    "TN": [
        "tn", "total nitrogen", "total n",
        "azote total", "azote global",           # French
        "gesamtstickstoff",                      # German
        "totaal stikstof",                       # Dutch
        "nitrogeno total",                       # Spanish
    ],
    "TP": [
        "tp", "total phosphorus",
        "phosphore total",                       # French
        "gesamtphosphor",                        # German
        "totaal fosfor",                         # Dutch
        "fosforo total",                         # Spanish/Italian
    ],
    "phosphate": ["po4", "phosphate", "phosphates", "phosphat"],
    # ── Carbon / organics ────────────────────────────────────────────────────
    "sulfate": [
        "so4", "sulfate", "sulphate", "sulfates", "sulphates",
        "sulfat",                                # German
        "sulfaat",                               # Dutch
        "sulfato",                               # Spanish/Portuguese
    ],
    "chloride": [
        "chloride", "chlorides",
        "chlorure", "chlorures",                 # French
        "chlorid",                               # German
        "chloride",                              # Dutch (same)
        "cloruro",                               # Spanish/Italian
    ],
    "fluoride": [
        "fluoride", "fluorides",
        "fluorure", "fluorures",                 # French
        "fluorid",                               # German
        "fluoride",                              # Dutch
        "fluoruro",                              # Spanish
    ],
    # ── Physical / general ───────────────────────────────────────────────────
    "pH": ["ph"],
    "conductivity": [
        "conductivity", "electrical conductivity", "specific conductance",
        "conductance",
        "conductivite", "conductivité", "conductance spécifique",  # French
        "dissolved salts", "sels dissous",       # French shorthand
        "leitfähigkeit", "elektrische leitfähigkeit",              # German
        "geleidbaarheid",                        # Dutch
        "conductividad",                         # Spanish
        "conducibilità",                         # Italian
    ],
    "turbidity": [
        "turbidity",
        "turbidite", "turbidité",                # French
        "trübung",                               # German
        "troebelheid",                           # Dutch
        "turbidez",                              # Spanish/Portuguese
        "torbidità",                             # Italian
    ],
    "hardness": [
        "hardness", "total hardness",
        "durete", "dureté", "dureté totale",     # French
        "th",                                    # French acronym
        "härte", "gesamthärte",                  # German
        "hardheid",                              # Dutch
        "dureza",                                # Spanish
        "durezza",                               # Italian
    ],
    "TDS": [
        "tds", "total dissolved solids", "dissolved solids",
        "matières dissoutes", "résidu sec",      # French
        "gelöste feststoffe",                    # German
        "opgeloste vaste stoffen",               # Dutch
        "solidos disueltos totales",             # Spanish
    ],
    "TSS": [
        "tss", "total suspended solids", "suspended solids", "solids",
        "matieres en suspension", "matières en suspension", "mes",
        "matières en suspension totales",        # French
        "schwebstoffe",                          # German
        "zwevende stoffen",                      # Dutch
        "solidos suspendidos totales",           # Spanish
    ],
    "sodium": ["sodium", "natrium", "sodio"],
    "perchlorate": ["perchlorate", "clo4"],
    "ORP": ["orp", "redox potential", "oxidation reduction potential"],
    "oil_grease": ["oil & grease", "oil and grease", "o&g", "fog"],
    "alkalinity": [
        "alkalinity", "total alkalinity",
        "alcalinite", "alcalinité", "tac",       # French (titre alcalimétrique)
        "alkalinität",                           # German
        "alkaliniteit",                          # Dutch
        "alcalinidad",                           # Spanish
    ],
    "UV254": [
        "uv254", "uv-254", "uv 254", "uv@254", "uv abs 254",
        "uv-absorption 254", "uv-abs",
    ],
    "temperature": [
        "temperature", "temp",
        "température",                           # French
        "temperatur",                            # German/Dutch
        "temperatura",                           # Spanish/Italian/Portuguese
    ],
    # ── Metals ────────────────────────────────────────────────────────────────
    # Strategy: match full names AND bare symbol (exact only, len=2 → no starts-with).
    "iron": [
        "iron", "total iron", "iron total", "iron dissolved",
        "fe",                                    # symbol — EXACT MATCH ONLY (len=2)
        "fer", "fer total", "total fer",         # French
        "eisen", "gesamteisen",                  # German
        "ijzer",                                 # Dutch
        "hierro",                                # Spanish
        "ferro",                                 # Italian/Portuguese
    ],
    "manganese": [
        "manganese", "total manganese", "manganese total",
        "mn",                                    # symbol — EXACT MATCH ONLY (len=2)
        "manganèse", "manganèse total",          # French
        "mangan",                                # German/Dutch
        "manganeso",                             # Spanish
        "manganese",                             # Italian (same)
    ],
    "copper": [
        "copper", "total copper", "copper total",
        "cu",                                    # symbol — EXACT MATCH ONLY (len=2)
        "cuivre", "cuivre total",                # French
        "kupfer",                                # German
        "koper",                                 # Dutch
        "cobre",                                 # Spanish/Portuguese
        "rame",                                  # Italian
    ],
    "zinc": [
        "zinc", "total zinc", "zinc total",
        "zn",                                    # symbol — EXACT MATCH ONLY (len=2)
        "zinco",                                 # Italian/Portuguese
        "zinc",                                  # Spanish/Dutch (same)
        "zink",                                  # German/Dutch
    ],
    "aluminum": [
        "aluminum", "aluminium", "total aluminum", "total aluminium",
        "aluminium total", "aluminum total",
        "al",                                    # symbol — EXACT MATCH ONLY (len=2)
        "aluminio",                              # Spanish
        "alluminio",                             # Italian
    ],
    "nickel": [
        "nickel", "total nickel", "nickel total",
        "ni",                                    # symbol — EXACT MATCH ONLY (len=2)
        "nichel",                                # Italian
        "nikel",                                 # Dutch
        "niquel",                                # Spanish/Portuguese
    ],
    "chromium": [
        "chromium", "total chromium", "chromium total", "chromium vi", "chromium iii",
        "cr",                                    # symbol — EXACT MATCH ONLY (len=2)
        "chrome", "chrome total", "total chrome", "chrome vi",  # French
        "chrom", "gesamtchrom",                  # German
        "chroom",                                # Dutch
        "cromo",                                 # Spanish/Italian
    ],
    "lead": [
        "lead", "total lead", "lead total",
        "pb",                                    # symbol — EXACT MATCH ONLY (len=2)
        "plomb", "plomb total",                  # French
        "blei",                                  # German
        "lood",                                  # Dutch
        "plomo",                                 # Spanish
        "piombo",                                # Italian
        "chumbo",                                # Portuguese
    ],
    "arsenic": [
        "arsenic", "total arsenic", "arsenic total",
        "as",                                    # symbol — EXACT MATCH ONLY (len=2)
        "arsen",                                 # German
        "arsenico",                              # Spanish/Italian
        "arsênio",                               # Portuguese
    ],
    "mercury": [
        "mercury", "total mercury", "mercury total",
        "hg",                                    # symbol — EXACT MATCH ONLY (len=2)
        "mercure",                               # French
        "quecksilber",                           # German
        "kwik",                                  # Dutch
        "mercurio",                              # Spanish/Italian
        "mercúrio",                              # Portuguese
    ],
    "cadmium": [
        "cadmium", "total cadmium", "cadmium total",
        "cd",                                    # symbol — EXACT MATCH ONLY (len=2)
        "cadmio",                                # Spanish/Italian
    ],
    "barium": [
        "barium", "total barium",
        "ba",                                    # symbol — EXACT MATCH ONLY (len=2)
        "baryum",                                # French
        "bario",                                 # Spanish/Italian
    ],
    "calcium": [
        "calcium", "total calcium",
        "ca",                                    # symbol — EXACT MATCH ONLY (len=2)
        "calcio",                                # Spanish/Italian/Portuguese
        "kalk",                                  # Dutch informal
    ],
    "magnesium": [
        "magnesium", "total magnesium",
        "mg",                                    # symbol — EXACT MATCH ONLY (len=2)
        "magnésium",                             # French
        "magnesio",                              # Spanish/Italian
    ],
    "boron": [
        "boron", "total boron",
        "bore",                                  # French
        "bor",                                   # German/Dutch/Spanish
        "boro",                                  # Italian/Portuguese
    ],
    "selenium": [
        "selenium", "total selenium",
        "sélénium",                              # French
        "selen",                                 # German/Dutch
        "selenio",                               # Spanish/Italian
    ],
    "silver": [
        "silver", "total silver",
        "ag",                                    # symbol — EXACT MATCH ONLY (len=2)
        "argent",                                # French
        "silber",                                # German
        "zilver",                                # Dutch
        "plata",                                 # Spanish
        "argento",                               # Italian
    ],
}

# Parameters that stay in their native unit (not converted to mg/L)
_MATRIX_NATIVE_UNIT_PARAMS: frozenset = frozenset({
    "pH", "temperature", "conductivity", "turbidity", "UV254", "UVT254", "flow_rate", "ORP",
})

# Strings that mark a cell as containing a sum parameter, not a real analyte
# (same set used for PFAS, shared here for matrix skip logic)
_SUM_PARAM_LOWER = frozenset({
    "aof", "aox", "eof", "top", "top assay",
    "sum parameter", "sum parameters",
})


def _keyword_match(cell_lower: str) -> Optional[str]:
    """
    Return the canonical parameter name if cell_lower matches any keyword.

    Matching tries (in order):
      1. Exact match on raw lowercased cell
      2. Exact match after stripping all parenthetical qualifiers
         "iron (total)"  → "iron",  "fe (ii)" → "fe"
      3. Exact match on the part before the first comma
         "iron, dissolved" → "iron",  "cr, total" → "cr"
      4. Starts-with match — only for keywords with len > 2
         (prevents "fe" matching "feed", "al" matching "alkalinity")
         "iron total" starts with "iron" ✓,  "no3-n" starts with "no3" ✓
    """
    raw = cell_lower.strip()
    if not raw:
        return None

    # Build candidate forms
    no_paren = re.sub(r"\s*[\(\[<][^\)\]>]*[\)\]>]", "", raw).strip().rstrip(",;: ")
    before_comma = no_paren.split(",")[0].strip()
    candidates = {raw, no_paren, before_comma}

    for param, keywords in _MATRIX_EXCEL_KEYWORDS.items():
        for kw in keywords:
            # Exact match on any candidate form
            if kw in candidates:
                return param
            # Starts-with (only safe for longer keywords)
            if len(kw) > 2:
                for cand in candidates:
                    if cand.startswith(kw) and (
                        len(cand) == len(kw) or not cand[len(kw)].isalpha()
                    ):
                        return param
    return None


def _parse_unit_from_cell(cell_val: str, default: str = "mg/L") -> str:
    """Extract unit from a parenthetical suffix like 'COD (mg/L)' or 'Fe (µg/L)'."""
    m = re.search(
        r"[\(\[<]\s*"
        r"((?:ng|µg|μg|ug|mg|g)/[lL]|ppm|ppb|ppt"
        r"|NTU|ntu|[µuμ]S/cm|mS/cm|°C|%)\s*"
        r"[\)\]>]",
        cell_val, re.IGNORECASE,
    )
    return m.group(1).strip() if m else default


_AS_N_FACTOR = {"nitrate": 4.43, "NO2": 3.29, "ammonia": 1.21}
_AS_N_NAME = re.compile(
    r"(?:no[23]|nh[34]\+?|nitrate|nitrite|ammonia|ammonium)\s*[-_]?\s*(?:\(?\s*as\s+)?n(?:itrogen)?\b"
    r"|ammoniacal|azote|stickstoff|stikstof|nitr[oó]geno",
    re.IGNORECASE,
)
_AS_N_UNIT = re.compile(r"\bas\s+n\b|mg\s*-?\s*n\s*/\s*l|mg\s*/\s*l\s*-?\s*n\b", re.IGNORECASE)


def _store_param(
    params: Dict[str, float],
    param: str,
    val: float,
    unit: str,
    label: str,
    logs: List[str],
    raw_name: str = "",
) -> None:
    """Convert value to canonical unit and store in params dict."""
    if param in _MATRIX_NATIVE_UNIT_PARAMS or not unit:
        params[param] = val
        logs.append(f"[Excel Matrix] {label}: {param} = {val} (native)")
        return
    as_n = param in _AS_N_FACTOR and bool(
        _AS_N_NAME.search(raw_name) or _AS_N_UNIT.search(unit)
    )
    if as_n:
        unit = _AS_N_UNIT.sub("", unit).strip() or "mg/L"
    mg_l = convert_to_mg_L(val, unit)
    if mg_l is None:
        # "mg/L as SO4-2", "µg/L (ppb)": take the magnitude unit it contains
        base_unit = detect_unit_from_text(unit)
        mg_l = convert_to_mg_L(val, base_unit) if base_unit else None
    if mg_l is None:
        mg_l = val  # unknown unit — assume already mg/L
    if as_n:
        # Engine thresholds and reagent stoichiometry assume as-ion mg/L
        mg_l *= _AS_N_FACTOR[param]
        logs.append(
            f"[Excel Matrix] {label}: {param} = {mg_l:.2f} mg/L as ion "
            f"(raw {val} {unit} as N, ×{_AS_N_FACTOR[param]})"
        )
    else:
        logs.append(f"[Excel Matrix] {label}: {param} = {mg_l} mg/L  (raw {val} {unit})")
    params[param] = mg_l


def _extract_matrix_from_excel(
    sheets: Dict[str, "pd.DataFrame"],
    logs: List[str],
) -> Dict[str, float]:
    """
    Scan every sheet for water matrix parameters using both layout detectors.
    First sheet to yield a given parameter wins.
    """
    combined: Dict[str, float] = {}
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue
        for k, v in _extract_matrix_params_from_sheet(df, sheet_name, logs).items():
            if k not in combined:
                combined[k] = v
    return combined


def _extract_matrix_params_from_sheet(
    df: "pd.DataFrame",
    sheet_name: str,
    logs: List[str],
) -> Dict[str, float]:
    """
    Try both layout strategies on a single sheet and merge results.
    """
    params: Dict[str, float] = {}
    if df.empty or len(df.columns) < 2:
        return params

    # Row layout: parameter name in column A, value(s) in columns B+
    for k, v in _extract_row_layout_matrix(df, sheet_name, logs).items():
        params[k] = v

    # Column layout: parameter names in a header row, values in rows below
    for k, v in _extract_column_layout_matrix(df, sheet_name, logs).items():
        if k not in params:
            params[k] = v

    return params


def _forward_fill_col_a(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Return a copy of df with column A NaN values forward-filled.
    Handles merged cells: pandas reads merged cells as value in first row,
    NaN in subsequent rows of the merged range.
    """
    df = df.copy()
    last_val = None
    for ri in range(len(df)):
        cell = str(df.iloc[ri, 0]).strip()
        if cell and cell.lower() not in ("nan", "none", ""):
            last_val = cell
        elif last_val is not None:
            df.iloc[ri, 0] = last_val
    return df


def _extract_row_layout_matrix(
    df: "pd.DataFrame",
    sheet_name: str,
    logs: List[str],
) -> Dict[str, float]:
    """
    Row layout: column A contains parameter names; values are in columns B+.
    Handles merged cells in column A via forward-fill.
    Detects optional "Unit" column and "Average"/"Mean" preferred value column.
    """
    params: Dict[str, float] = {}
    df = _forward_fill_col_a(df)

    _UNIT_HDR = {
        "unit", "units", "unité", "unites", "unités", "unite", "uom",
        "einheit",          # German
        "eenheid",          # Dutch
        "unidad",           # Spanish
        "unità",            # Italian
    }
    _VALUE_HDR = {
        "average", "avg", "mean", "typical", "value", "valeur",
        "moyenne", "résultat", "resultat", "concentration",
        "mittelwert", "durchschnitt",   # German
        "gemiddelde",                   # Dutch
        "promedio", "media",            # Spanish/Italian
    }
    unit_col_idx: Optional[int] = None
    value_col_idx: Optional[int] = None

    # Search first 10 rows for header columns
    for ri in range(min(10, len(df))):
        for ci in range(len(df.columns)):
            cell = str(df.iloc[ri, ci]).strip().lower()
            if cell in _UNIT_HDR and unit_col_idx is None:
                unit_col_idx = ci
            if cell in _VALUE_HDR and value_col_idx is None and ci > 0:
                value_col_idx = ci

    for ri in range(len(df)):
        cell_val = str(df.iloc[ri, 0]).strip()
        if not cell_val or cell_val.lower() in ("nan", "none", ""):
            continue
        if cell_val.lower().strip() in _SUM_PARAM_LOWER:
            continue

        matched = _keyword_match(cell_val.lower())
        if not matched or matched in params:
            continue

        unit = "" if matched in _MATRIX_NATIVE_UNIT_PARAMS else "mg/L"
        unit = _parse_unit_from_cell(cell_val, unit)
        if unit_col_idx is not None and unit_col_idx < len(df.columns):
            uc = str(df.iloc[ri, unit_col_idx]).strip()
            if uc and uc.lower() not in ("nan", "none", ""):
                unit = uc

        found_val: Optional[float] = None
        if value_col_idx is not None and value_col_idx < len(df.columns):
            found_val = parse_numeric_value(df.iloc[ri, value_col_idx])
        if found_val is None:
            for ci in range(1, len(df.columns)):
                if ci == unit_col_idx:
                    continue
                v = parse_numeric_value(df.iloc[ri, ci])
                if v is not None:
                    found_val = v
                    break

        if found_val is None:
            continue
        _store_param(params, matched, found_val, unit,
                     f"'{sheet_name}' r{ri + 1} (row-layout)", logs, raw_name=cell_val)

    if params:
        logs.append(
            f"[Excel Matrix] '{sheet_name}' row-layout: {len(params)} found: "
            f"{list(params.keys())}"
        )
    return params


def _extract_column_layout_matrix(
    df: "pd.DataFrame",
    sheet_name: str,
    logs: List[str],
) -> Dict[str, float]:
    """
    Column layout: parameter names span a header row (columns B+);
    column A holds row-type labels ('Unit', 'Average', 'Maximum', etc.).

    Searches up to row 40 for the header, to handle reports with long
    preamble text before the data table starts.

    Typical structure:
      Row k:   [label]   | COD  | BOD5 | TKN  | Copper | …
      Row k+1: Unit       | mg/L | mg/L | mg/L | mg/L   | …
      Row k+2: Average… | 100  | 11   | 8.5  | 0.02   | …
    """
    params: Dict[str, float] = {}
    if df.empty or len(df.columns) < 3:
        return params

    # ── Find the header row (most keyword hits across columns) ─────────────────
    # Search up to row 40 — many reports have a long preamble
    SEARCH_DEPTH = min(40, len(df))
    best_row = -1
    best_count = 0
    best_col_map: Dict[int, str] = {}

    for ri in range(SEARCH_DEPTH):
        col_map: Dict[int, str] = {}
        for ci in range(len(df.columns)):   # include column A (some sheets skip it)
            cell = str(df.iloc[ri, ci]).strip()
            if not cell or cell.lower() in ("nan", "none", ""):
                continue
            if cell.lower().strip() in _SUM_PARAM_LOWER:
                continue
            matched = _keyword_match(cell.lower())
            if matched and ci not in col_map:
                col_map[ci] = matched
        if len(col_map) > best_count:
            best_count = len(col_map)
            best_row = ri
            best_col_map = col_map

    if best_count < 2:
        return params

    logs.append(
        f"[Excel Matrix] '{sheet_name}': column-layout header at row {best_row + 1} "
        f"({best_count} params detected)"
    )

    # ── Find unit row and value row after the header ───────────────────────────
    _UNIT_A = {
        "unit", "units", "unité", "unites", "unités", "unite",
        "einheit", "eenheid", "unidad", "unità",
    }
    _VALUE_A_STARTS = (
        "average", "avg", "mean", "typical", "value", "valeur",
        "moyenne", "concentration",
        "mittelwert", "durchschnitt", "gemiddelde", "promedio", "media",
    )

    unit_row: Optional[int] = None
    value_row: Optional[int] = None

    # Search up to 20 rows after the header
    AFTER_DEPTH = min(best_row + 20, len(df))
    for ri in range(best_row + 1, AFTER_DEPTH):
        col_a = str(df.iloc[ri, 0]).strip().lower()
        col_a_clean = re.sub(r"\s*[\(\[<][^\)\]>]*[\)\]>]", "", col_a).strip()
        if col_a_clean in _UNIT_A and unit_row is None:
            unit_row = ri
        elif any(col_a_clean.startswith(s) for s in _VALUE_A_STARTS) and value_row is None:
            value_row = ri

    # Fallback: first row after header that has numeric values in matched columns
    if value_row is None:
        for ri in range(best_row + 1, AFTER_DEPTH):
            if ri == unit_row:
                continue
            if any(
                ci < len(df.columns) and parse_numeric_value(df.iloc[ri, ci]) is not None
                for ci in best_col_map
            ):
                value_row = ri
                break

    if value_row is None:
        logs.append(f"[Excel Matrix] '{sheet_name}': column layout: no value row found")
        return params

    logs.append(
        f"[Excel Matrix] '{sheet_name}': unit row="
        f"{unit_row + 1 if unit_row is not None else 'N/A'}, "
        f"value row={value_row + 1}"
    )

    # ── Extract each detected column ──────────────────────────────────────────
    for ci, canonical in best_col_map.items():
        if canonical in params or ci >= len(df.columns):
            continue

        # Unit: from the unit row, else from the header cell, else default
        unit = "" if canonical in _MATRIX_NATIVE_UNIT_PARAMS else "mg/L"
        header_cell = str(df.iloc[best_row, ci]).strip()
        unit = _parse_unit_from_cell(header_cell, unit)
        if unit_row is not None and ci < len(df.columns):
            uc = str(df.iloc[unit_row, ci]).strip()
            if uc and uc.lower() not in ("nan", "none", ""):
                unit = uc

        found_val = parse_numeric_value(df.iloc[value_row, ci])
        if found_val is None:
            continue
        _store_param(params, canonical, found_val, unit,
                     f"'{sheet_name}' col {ci + 1} (col-layout)", logs, raw_name=header_cell)

    if params:
        logs.append(
            f"[Excel Matrix] '{sheet_name}' column-layout: {len(params)} found: "
            f"{list(params.keys())}"
        )
    return params


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

            result.matrix_params = _extract_matrix_from_pdf(all_tables, all_text_lines, logs)
            if result.matrix_params:
                logs.append(
                    f"[PDF] Water-quality params: {list(result.matrix_params.keys())}"
                )

            # Try table parsing first
            if all_tables:
                table_result = _parse_pdf_tables(all_tables, unit, logs)
                if table_result.has_pfas_data:
                    result.pfas_samples = table_result.pfas_samples
                    logs.append(f"[PDF] Table extraction successful → {len(result.pfas_samples)} sample(s)")
                    return result
                else:
                    logs.append("[PDF] Table extraction yielded no PFAS data; falling back to text parsing")

            # Fall back to text parsing
            text_result = _parse_pdf_text(all_text_lines, unit, logs)
            result.pfas_samples = text_result.pfas_samples
            if result.has_pfas_data:
                logs.append(f"[PDF] Text parsing → {len(result.pfas_samples)} sample(s)")
            elif result.matrix_params:
                logs.append(
                    f"[PDF] '{filename}': no PFAS results; read as water-quality data"
                )
            else:
                result.warnings.append(
                    "[PDF] Could not extract structured PFAS data. "
                    "The PDF may be scanned or have a non-standard layout."
                )

    except Exception as e:
        result.errors.append(f"[PDF] Parse error: {e}")

    return result


_PDF_UNIT_TOKEN = re.compile(
    r"(?:ng|µg|μg|ug|mg|g)(?:\s*-?\s*n)?/[lL](?:-?n)?|ppm|ppb|ppt|ntu|[µuμ]s/cm|ms/cm"
    r"|°c|%|s\.?u\.?|cm-1|1/cm|/cm|abs",
    re.IGNORECASE,
)
_PDF_PLAIN_NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?|\.\d+")


def _extract_matrix_from_pdf(
    tables: List[List[List[Optional[str]]]],
    lines: List[str],
    logs: List[str],
) -> Dict[str, float]:
    """
    Water-quality parameters from a PDF lab report. Tables reuse the Excel
    matrix detectors; text lines are read as "<parameter> <value> <unit>",
    e.g. "Chloride 120 mg/L 1.0". First occurrence of a parameter wins.
    """
    params: Dict[str, float] = {}

    for ti, table in enumerate(tables):
        df = pd.DataFrame([["" if c is None else str(c) for c in row] for row in table])
        for k, v in _extract_matrix_params_from_sheet(df, f"PDF table {ti + 1}", logs).items():
            params.setdefault(k, v)

    for line in lines:
        tokens = line.split()
        # The value is the first token that starts like a number; anything
        # before it is the parameter name (and possibly a unit).
        idx = next(
            (i for i, t in enumerate(tokens) if i > 0 and re.match(r"[<>≤≥.\d]", t)),
            None,
        )
        if idx is None or not _PDF_PLAIN_NUMBER.fullmatch(tokens[idx]):
            continue  # no value, or a non-detect like "<0.5"
        name_tokens = tokens[:idx]
        unit = ""
        if len(name_tokens) > 1 and _PDF_UNIT_TOKEN.fullmatch(name_tokens[-1]):
            unit = name_tokens.pop()
        elif idx + 1 < len(tokens) and _PDF_UNIT_TOKEN.fullmatch(tokens[idx + 1]):
            unit = tokens[idx + 1]
            if [t.lower() for t in tokens[idx + 2:idx + 4]] == ["as", "n"]:
                unit += " as N"
        name = " ".join(name_tokens).rstrip(":=")
        param = _keyword_match(name.lower())
        if not param or param in params:
            continue
        if normalize_pfas_name(name) in PFAS_SPECIES_DB or _looks_like_pfas(name):
            continue
        # A unit guards against prose such as "Copper Ridge Road 123"
        if not unit and param != "pH":
            continue
        val = parse_numeric_value(tokens[idx].replace(",", ""))
        if val is None:
            continue
        if param in _MATRIX_NATIVE_UNIT_PARAMS:
            unit = ""
        _store_param(params, param, val, unit, "PDF text", logs, raw_name=name)

    for k, v in parse_text("\n".join(lines)).matrix_params.items():
        params.setdefault(k, v)

    return params


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
    # ── Nitrogen species ─────────────────────────────────────────────────────
    "ammonia": [
        (r"(?:NH3|NH4\+?|ammonia|ammonium)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"ammonia[-\s]*N\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L-N"),
    ],
    "TKN": [
        (r"TKN\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"total\s+kjeldahl\s+(?:nitrogen|N)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    # ── Metals (precipitation screening at pH 12) ─────────────────────────────
    "iron": [
        (r"(?:total\s+)?(?:iron|Fe(?:\s*(?:2|3)\+?)?)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "manganese": [
        (r"(?:manganese|Mn)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "copper": [
        (r"(?:copper|Cu)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "zinc": [
        (r"(?:zinc|Zn)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "aluminum": [
        (r"(?:aluminum|aluminium|Al)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "nickel": [
        (r"(?:nickel|Ni)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "chromium": [
        (r"(?:total\s+)?(?:chromium|Cr(?:\s*(?:VI|III))?)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
    ],
    "lead": [
        (r"(?:lead|Pb)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
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
}

_FLOW_TEXT = re.compile(
    r"(?:flow(?:\s*rate)?|throughput|capacity|d[ée]bit|durchfluss)\b[^0-9\n]{0,25}?"
    r"([0-9][0-9,.]*)\s*"
    r"(MGD|gpm|gpd|gph|gal/min|gal/day|m3/h|m³/h|m3/hr|m3/d|m³/d|m3/day|L/min|L/s|L/h|lpm)\b",
    re.IGNORECASE,
)


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
                    # Normalize as-N nitrogen values to species form (engine thresholds
                    # and reagent stoichiometry assume as-ion / as-molecule mg/L)
                    if pat_unit == "mg/L-N":
                        factor = {"nitrate": 4.43, "NO2": 3.29, "ammonia": 1.21}.get(param, 1.0)
                        val = val * factor
                        logs.append(
                            f"[Text] Matrix param: {param} = {val:.2f} mg/L as ion "
                            f"(converted from as-N ×{factor})"
                        )
                    else:
                        logs.append(f"[Text] Matrix param: {param} = {val} {pat_unit}")
                    matrix[param] = val
                    break
                except (ValueError, IndexError):
                    pass

    result.matrix_params = matrix

    flow = _FLOW_TEXT.search(combined)
    if flow:
        value = parse_numeric_value(flow.group(1).rstrip(".,"))
        gpm = convert_flow_to_gpm(value, flow.group(2)) if value is not None else None
        if gpm is not None:
            result.llm_project_context["throughput_gpm"] = round(gpm, 2)
            result.llm_project_context["flow_rate_display"] = f"{value:g} {flow.group(2)}"
            logs.append(f"[Text] Flow: {value:g} {flow.group(2)} = {gpm:.1f} GPM")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED PARSE ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

_PER_SAMPLE_FIELDS = (
    "pfas_samples", "nd_species", "aof_tof_data", "sample_metadata", "sample_matrix",
    "sample_ranges", "sample_flow", "sample_avg", "sample_max_missing", "sample_basis",
    "sample_source",
)


def _rename_sample(data: ParsedData, old: str, new: str) -> None:
    for name in _PER_SAMPLE_FIELDS:
        per_sample = getattr(data, name)
        if old in per_sample:
            per_sample[new] = per_sample.pop(old)


def _drop_sample(data: ParsedData, sample: str) -> None:
    for name in _PER_SAMPLE_FIELDS:
        getattr(data, name).pop(sample, None)


def _table_has_pfas(df: "pd.DataFrame") -> bool:
    """Word or web tables also hold addresses and methods; only real results count."""
    if df is None or df.empty or len(df.columns) < 2:
        return False
    for cell in df.iloc[:, 0]:
        text = str(cell).strip()
        if normalize_pfas_name(text) in PFAS_SPECIES_DB or _looks_like_pfas(text):
            return True
    return _detect_transposed_layout(df)


def parse_table_document(doc: ReadDocument) -> ParsedData:
    """PFAS results, water matrix and flow from the tables of a document."""
    result = ParsedData()
    logs = result.logs
    for title, df in doc.tables.items():
        if not _table_has_pfas(df):
            continue
        table = _parse_excel_sheet(df, title, doc.name, logs)
        result.warnings += table.warnings
        result.stream_hint = result.stream_hint or table.stream_hint
        if table.has_pfas_data and not result.has_pfas_data:
            result.detected_unit = table.detected_unit
        # One results table split across pages comes through as several tables.
        for sample, data in table.pfas_samples.items():
            result.pfas_samples.setdefault(sample, {}).update(data)
        for sample, species in table.nd_species.items():
            mine = result.nd_species.setdefault(sample, [])
            mine += [sp for sp in species if sp not in mine]
        for sample, bulk in table.aof_tof_data.items():
            result.aof_tof_data.setdefault(sample, {}).update(bulk)
    _attach_water_data(result, doc.tables, doc.name)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# WATER MATRIX AND FLOW PER SAMPLE
# ═══════════════════════════════════════════════════════════════════════════════

_STAT_COLUMN = re.compile(
    r"^\s*(min|minimum|avg|average|mean|typical|max|maximum|median)\.?\s*$", re.IGNORECASE
)
_GENERIC_VALUE_COLUMN = re.compile(
    r"^\s*(results?|values?|concentrations?|conc\.?|readings?|data|amount|levels?|"
    r"measured|measurement|sample_\d+)\s*$",
    re.IGNORECASE,
)
_UNIT_HEADERS = frozenset({
    "unit", "units", "uom", "unité", "unités", "unite", "unites", "einheit", "eenheid",
    "unidad", "unità",
})
_FLOW_LABEL = re.compile(
    r"\bflow\s*rate\b|^(?:design|operational|operating|average|peak|max(?:imum)?)?\s*flow\b"
    r"|\bthroughput\b|\bd[ée]bit\b|\bdurchfluss\b|\bcaudal\b",
    re.IGNORECASE,
)
_HOURS_LABEL = re.compile(
    r"operational\s+schedule|operating\s+(?:schedule|hours|time)|hours?\s*(?:per|/)\s*day",
    re.IGNORECASE,
)
_VOLUME_LABEL = re.compile(r"daily\s+volume|volume\s+per\s+day", re.IGNORECASE)
_FLOW_UNIT_IN_TEXT = re.compile(
    r"mgd|gpm|gpd|gph|gal/min|gal/day|m3/h|m³/h|m3/hr|m3/d|m³/d|m3/day|l/min|l/s|l/h|lpm",
    re.IGNORECASE,
)


def _stat_kind(column: str) -> str:
    name = column.strip().lower()
    if name.startswith("min"):
        return "min"
    if name.startswith("max"):
        return "max"
    return "avg"


@dataclass
class _ValueTable:
    """Water chemistry, flow and operation of one table, by value column."""
    matrix: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    flow: Dict[str, Tuple[float, str]] = field(default_factory=dict)      # column -> (value, unit)
    hours: Dict[str, float] = field(default_factory=dict)
    volume: Dict[str, Tuple[float, str]] = field(default_factory=dict)
    stream_hint: str = ""
    unreadable_flow: List[str] = field(default_factory=list)


def _row_kind(label: str) -> Optional[str]:
    if not label:
        return None
    if normalize_pfas_name(label) in PFAS_SPECIES_DB or _looks_like_pfas(label):
        return "pfas"
    low = label.lower()
    if _FLOW_LABEL.search(label) and "overflow" not in low:
        return "flow"
    if _HOURS_LABEL.search(label):
        return "hours"
    if _VOLUME_LABEL.search(label):
        return "volume"
    if low in _SUM_PARAM_LOWER:
        return None
    return "matrix" if _keyword_match(low) else None


def _read_value_table(df: "pd.DataFrame", title: str, logs: List[str]) -> Optional[_ValueTable]:
    """
    Read a table with parameter names in column A and named value columns
    (sample names, or Min / Avg / Max). Returns None when the table has no
    header row to name its columns; the caller then uses the layout detectors.
    """
    if df is None or df.empty or len(df.columns) < 2:
        return None
    n_rows, n_cols = df.shape

    def cell(r: int, c: int) -> str:
        text = str(df.iloc[r, c]).strip()
        return "" if text.lower() in ("nan", "none") else text

    kinds = [_row_kind(cell(r, 0)) for r in range(n_rows)]
    first = next((r for r, kind in enumerate(kinds) if kind), None)
    if not first:
        return None

    header = None
    for r in range(first - 1, -1, -1):
        if cell(r, 0).lower() in _UNIT_HEADERS:
            continue
        if any(c and parse_numeric_value(c) is None for c in (cell(r, c) for c in range(1, n_cols))):
            header = r
            break
    if header is None:
        return None

    unit_col = next((c for c in range(1, n_cols) if cell(header, c).lower() in _UNIT_HEADERS), None)
    value_cols: Dict[int, str] = {}
    for c in range(1, n_cols):
        name = cell(header, c)
        if c == unit_col or not name or _NOT_A_SAMPLE.match(name):
            continue
        name = re.sub(r"\s*[\(\[]\s*(?:ng|µg|ug|mg)/[lL].*?[\)\]]", "", name).strip()
        name = re.sub(r"\s*[\(\[]\s*(?:ppt|ppb|ppm).*?[\)\]]", "", name).strip()
        if name:
            value_cols[c] = name
    if not value_cols:
        return None

    table = _ValueTable(stream_hint=_group_label(df, header - 1) if header > 0 else "")
    for r in range(first, n_rows):
        kind = kinds[r]
        if kind in (None, "pfas"):
            continue
        label = cell(r, 0)
        unit_text = cell(r, unit_col) if unit_col is not None else ""
        for c, column in value_cols.items():
            value = parse_numeric_value(cell(r, c))
            if value is None:
                continue
            where = f"'{title}' r{r + 1} [{column}]"
            if kind == "matrix":
                param = _keyword_match(label.lower())
                unit = "" if param in _MATRIX_NATIVE_UNIT_PARAMS else (
                    unit_text or _parse_unit_from_cell(label, "mg/L")
                )
                stored: Dict[str, float] = {}
                _store_param(stored, param, value, unit, where, logs, raw_name=label)
                table.matrix.setdefault(column, {}).setdefault(param, stored[param])
            elif kind == "flow":
                found = _FLOW_UNIT_IN_TEXT.search(unit_text) or _FLOW_UNIT_IN_TEXT.search(label)
                if found:
                    table.flow.setdefault(column, (value, found.group()))
                else:
                    table.unreadable_flow.append(f"{label}: {value:g} {unit_text}".strip())
            elif kind == "hours":
                table.hours.setdefault(column, value)
            elif kind == "volume":
                table.volume.setdefault(column, (value, unit_text or "gallons"))
    return table


def describe_flow(flow: Dict[str, Any]) -> str:
    """'avg 25, max 50 gpm' from a sample_flow entry."""
    unit = flow.get("unit", "gpm")
    parts = [f"{key} {flow[key]:g}" for key in ("min", "avg", "max") if flow.get(key) is not None]
    if not parts:
        return ""
    if len(parts) == 1:
        return f"{parts[0].split(' ', 1)[1]} {unit}"
    return f"{', '.join(parts)} {unit}"


def design_flow_gpm(flow: Optional[Dict[str, Any]]) -> Optional[float]:
    """The flow a stream is sized on: its maximum, else average, else minimum."""
    if not flow:
        return None
    for key in ("max_gpm", "avg_gpm", "min_gpm"):
        if flow.get(key) is not None:
            return float(flow[key])
    return None


def _flow_entry(
    flow: Dict[str, Tuple[float, str]],
    hours: Dict[str, float],
    volume: Dict[str, Tuple[float, str]],
) -> Dict[str, Any]:
    """Build a sample_flow entry from {"min"|"avg"|"max": ...} readings."""
    entry: Dict[str, Any] = {}
    for kind, (value, unit) in flow.items():
        gpm = convert_flow_to_gpm(value, unit)
        if gpm is None:
            continue
        entry[kind] = value
        entry[f"{kind}_gpm"] = round(gpm, 3)
        entry.setdefault("unit", unit)
    for kind, value in hours.items():
        entry[f"hours_{kind}"] = value
    for value, unit in list(volume.values())[:1]:
        entry["daily_volume"] = value
        entry["daily_volume_unit"] = unit
    return entry


def _attach_water_data(result: ParsedData, tables: Dict[str, "pd.DataFrame"], filename: str) -> None:
    """
    Read water chemistry and flow from a file's tables and tie them to its
    samples. Columns named Min / Avg / Max are statistics of one stream, not
    three samples.
    """
    logs = result.logs
    general: Dict[str, Any] = {}
    matrix: Dict[str, Dict[str, Any]] = {}
    flow: Dict[str, Tuple[float, str]] = {}
    hours: Dict[str, float] = {}
    volume: Dict[str, Tuple[float, str]] = {}
    hints = [result.stream_hint]

    for title, df in tables.items():
        table = _read_value_table(df, str(title), logs)
        if table is None:
            if df is not None and not df.empty and len(df.columns) >= 2:
                for key, value in _extract_matrix_params_from_sheet(df, str(title), logs).items():
                    general.setdefault(key, value)
            continue
        hints.append(table.stream_hint)
        for column, params in table.matrix.items():
            target = matrix.setdefault(column, {})
            for key, value in params.items():
                target.setdefault(key, value)
        for column, reading in table.flow.items():
            flow.setdefault(column, reading)
        for column, value in table.hours.items():
            hours.setdefault(column, value)
        for column, reading in table.volume.items():
            volume.setdefault(column, reading)
        for item in table.unreadable_flow:
            result.data_checks.append({
                "level": "warning",
                "message": f"'{filename}': flow value without a recognised unit ({item}); not used.",
            })

    pfas_names = set(result.pfas_samples) | set(result.nd_species) | set(result.aof_tof_data)
    columns = pfas_names | set(matrix) | set(flow) | set(hours) | set(volume)

    if columns and all(_STAT_COLUMN.match(c) for c in columns):
        stream = next((h for h in hints if h), "") or PurePosixPath(filename.split(" / ")[-1]).stem
        _fold_statistics(result, stream, matrix, flow, hours, volume, filename)
    else:
        for column in sorted(set(matrix) | set(flow) | set(hours) | set(volume)):
            entry = _flow_entry(
                {"avg": flow[column]} if column in flow else {},
                {"avg": hours[column]} if column in hours else {},
                {"avg": volume[column]} if column in volume else {},
            )
            named = column in pfas_names or not (
                _GENERIC_VALUE_COLUMN.match(column) or _STAT_COLUMN.match(column)
            )
            if named:
                if column in matrix:
                    result.sample_matrix[column] = matrix[column]
                if entry:
                    result.sample_flow[column] = entry
            else:
                for key, value in matrix.get(column, {}).items():
                    general.setdefault(key, value)
                gpm = design_flow_gpm(entry)
                if gpm is not None:
                    result.llm_project_context.setdefault("throughput_gpm", round(gpm, 2))
                    result.llm_project_context.setdefault("flow_rate_display", describe_flow(entry))

    for key, value in general.items():
        result.matrix_params.setdefault(key, value)
    if general or matrix:
        logs.append(
            f"[Matrix] '{filename}': {len(general)} parameter(s) not tied to a sample, "
            f"{sum(len(m) for m in result.sample_matrix.values())} tied to samples"
        )


def _fold_statistics(
    result: ParsedData,
    stream: str,
    matrix: Dict[str, Dict[str, Any]],
    flow: Dict[str, Tuple[float, str]],
    hours: Dict[str, float],
    volume: Dict[str, Tuple[float, str]],
    filename: str,
) -> None:
    """Turn Min / Avg / Max columns into one sample named after its stream."""
    def by_kind(per_column: Dict[str, Any]) -> Dict[str, Any]:
        folded: Dict[str, Any] = {}
        for column, value in per_column.items():
            folded.setdefault(_stat_kind(column), value)
        return folded

    def merged(per_column: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        folded: Dict[str, Dict[str, Any]] = {"min": {}, "avg": {}, "max": {}}
        for column, values in per_column.items():
            for key, value in values.items():
                folded[_stat_kind(column)].setdefault(key, value)
        return folded

    def ranges(stats: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        names = set(stats["min"]) | set(stats["avg"]) | set(stats["max"])
        out = {
            name: {kind: stats[kind][name] for kind in ("min", "avg", "max") if name in stats[kind]}
            for name in names
        }
        return {name: r for name, r in out.items() if len(r) > 1}

    pfas = merged(result.pfas_samples)
    worst = {**pfas["min"], **pfas["avg"], **pfas["max"]}     # maximum wins, then average
    non_detects = sorted({sp for species in result.nd_species.values() for sp in species} - set(worst))
    bulk = merged(result.aof_tof_data)
    bulk_worst = {**bulk["min"], **bulk["avg"], **bulk["max"]}

    result.pfas_samples = {stream: worst} if worst else {}
    result.nd_species = {stream: non_detects} if non_detects else {}
    result.aof_tof_data = {stream: bulk_worst} if bulk_worst else {}

    if pfas["max"] and pfas["avg"]:
        result.sample_avg[stream] = dict(pfas["avg"])
        missing = sorted(set(pfas["avg"]) - set(pfas["max"]))
        if missing:
            result.sample_max_missing[stream] = missing
    elif pfas["max"]:
        result.sample_basis[stream] = "maximum"
    elif pfas["avg"]:
        result.sample_basis[stream] = "average"
    elif pfas["min"]:
        result.sample_basis[stream] = "minimum"
    only_min = sorted(set(pfas["min"]) - set(pfas["avg"]) - set(pfas["max"]))
    if only_min and (pfas["avg"] or pfas["max"]):
        result.data_checks.append({
            "level": "warning",
            "message": (
                f"'{filename}': only a minimum is reported for {', '.join(only_min)}; "
                "the minimum is used, so these are understated."
            ),
        })

    chem = merged(matrix)
    screening = {**chem["min"], **chem["avg"], **chem["max"]}
    if screening:
        result.sample_matrix[stream] = screening
    all_ranges = {**ranges(pfas), **ranges(chem)}
    if all_ranges:
        result.sample_ranges[stream] = all_ranges
    entry = _flow_entry(by_kind(flow), by_kind(hours), by_kind(volume))
    if entry:
        result.sample_flow[stream] = entry
    result.logs.append(
        f"[Stream] '{filename}': Min/Avg/Max columns read as one sample '{stream}' "
        f"({len(worst)} PFAS, {len(screening)} matrix, flow: {describe_flow(entry) or 'none'})"
    )


def sample_matrices(parsed: ParsedData) -> Dict[str, Dict[str, Any]]:
    """Water matrix of each sample: its own values over those not tied to a sample."""
    return {name: {**parsed.matrix_params, **own} for name, own in parsed.sample_matrix.items()}


def screening_matrix(parsed: ParsedData) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    The matrix the overall screening runs on: for each parameter the highest
    value across samples. Also returns which sample supplied each value.
    """
    worst: Dict[str, Any] = dict(parsed.matrix_params)
    origin: Dict[str, str] = {}
    for sample, params in parsed.sample_matrix.items():
        for key, value in params.items():
            current = worst.get(key)
            try:
                higher = current is None or float(value) > float(current)
            except (TypeError, ValueError):
                higher = current is None
            if higher:
                worst[key] = value
                origin[key] = sample
    return worst, origin


def describe_source(doc: ReadDocument, images_read: bool = False) -> Dict[str, str]:
    """One row of the report's source-file table."""
    if doc.kind == KIND_UNREADABLE:
        read_as = "Not read"
    else:
        parts = []
        if doc.kind == KIND_SPREADSHEET or doc.tables:
            parts.append("Tables")
        if doc.prose.strip():
            parts.append("Text")
        if doc.images:
            parts.append(
                f"{len(doc.images)} image(s), read twice" if images_read
                else f"{len(doc.images)} image(s) NOT read"
            )
        read_as = ", ".join(parts) or "No content"
    return {"file": doc.name, "format": doc.fmt, "read_as": read_as, "note": " ".join(doc.notes)}


def _flag_if_empty(result: ParsedData, doc: ReadDocument) -> None:
    if doc.is_context:
        return      # an email or spec with no results table is normal
    if not (
        result.pfas_samples or result.nd_species or result.matrix_params
        or result.sample_matrix or result.sample_flow or result.llm_project_context
    ):
        result.data_checks.append({
            "level": "warning",
            "message": (
                f"No PFAS or water-quality values were found in '{doc.name}'. "
                "If it does hold results, turn on Enhanced document reading or check its layout."
            ),
        })


def parse_all(
    documents: List[ReadDocument],
    email_text: str,
    goals_text: str,
) -> ParsedData:
    """
    Parse all available input sources and return a merged ParsedData.

    Priority for conflicting concentration values:
    spreadsheet and document tables > PDF > free text.
    """
    combined = ParsedData()
    combined.logs.append("=== Parser started ===")

    for doc in documents:
        combined.source_files.append(describe_source(doc))
        combined.data_checks += [{"level": "critical", "message": m} for m in doc.problems]
        if doc.images:
            combined.data_checks.append({
                "level": "critical",
                "message": (
                    f"'{doc.name}': {len(doc.images)} image(s) or scanned page(s) were NOT read. "
                    "Standard parsing cannot read images; turn on Enhanced document reading."
                ),
            })

    owners: Dict[str, ReadDocument] = {}   # sample -> document that supplied its PFAS data
    shared_names: set = set()              # names used by several files, now file-specific

    def stem(doc: ReadDocument) -> str:
        return PurePosixPath(doc.name.split(" / ")[-1]).stem

    def absorb(result: ParsedData, doc: ReadDocument) -> None:
        for name in sorted(set(result.pfas_samples) | set(result.nd_species)):
            owner = owners.get(name)
            if owner is not None and owner is not doc and (
                doc.kind == KIND_PDF and owner.kind == KIND_SPREADSHEET
            ):
                # The same report as a spreadsheet and as a PDF: keep the spreadsheet.
                _drop_sample(result, name)
                combined.data_checks.append({
                    "level": "warning",
                    "message": (
                        f"Sample '{name}' is in both '{owner.name}' and '{doc.name}'. They are "
                        "treated as the same report in two formats; the spreadsheet values are used."
                    ),
                })
                continue
            if name in shared_names or (owner is not None and owner is not doc):
                new = f"{name} ({stem(doc)})"
                while new in owners:
                    new += " (2)"
                if owner is not None:
                    first = f"{name} ({stem(owner)})"
                    _rename_sample(combined, name, first)
                    owners[first] = owners.pop(name)
                    shared_names.add(name)
                    message = (
                        f"Sample '{name}' appears in more than one file. Kept as separate "
                        f"samples '{first}' and '{new}'."
                    )
                else:
                    message = f"Sample '{name}' appears in more than one file. Kept as '{new}'."
                _rename_sample(result, name, new)
                combined.data_checks.append({"level": "warning", "message": message})
                combined.warnings.append(f"[Parser] {message}")
                name = new
            owners[name] = doc
            result.sample_source.setdefault(name, doc.name)

        for key, value in result.matrix_params.items():
            current = combined.matrix_params.get(key)
            try:
                differs = current is not None and abs(float(current) - float(value)) > 0.01 * max(
                    abs(float(current)), abs(float(value))
                )
            except (TypeError, ValueError):
                differs = False
            if differs:
                higher = max(float(current), float(value))
                combined.matrix_params[key] = higher
                combined.data_checks.append({
                    "level": "warning",
                    "message": (
                        f"{key} is reported as {float(current):g} and as {float(value):g} in "
                        f"files that do not tie it to a sample ('{doc.name}' is one). "
                        f"The higher value, {higher:g}, is used."
                    ),
                })
        if result.has_pfas_data and not combined.has_pfas_data:
            combined.detected_unit = result.detected_unit
        _flag_if_empty(result, doc)
        combined.merge(result)

    # Spreadsheets first, so a PDF copy of the same report can be recognised
    for doc in sorted(documents, key=lambda d: d.kind != KIND_SPREADSHEET):
        if doc.kind == KIND_SPREADSHEET:
            absorb(parse_excel(doc.data, doc.name), doc)
        elif doc.kind == KIND_DOCUMENT and doc.tables:
            result = parse_table_document(doc)
            result.has_other = True
            absorb(result, doc)
        elif doc.kind == KIND_PDF:
            result = parse_table_document(doc) if doc.tables else ParsedData()
            if not (result.pfas_samples or result.nd_species or result.sample_matrix
                    or result.sample_flow or result.matrix_params):
                # No ruled tables with results: read the page text instead.
                result = parse_pdf(doc.data, doc.name)
            result.has_pdf = True
            absorb(result, doc)

    # Prose of lab documents supplies values only. It is kept out of the notes
    # text because engine keyword rules (such as AN-1 on "drinking water") would
    # otherwise fire on a lab report's method descriptions.
    prose_docs = [d for d in documents if d.kind == KIND_DOCUMENT and d.prose.strip()]
    if prose_docs:
        combined.has_other = True
        prose_result = parse_text("\n".join(d.prose for d in prose_docs), "")
        combined.matrix_params.update(
            {k: v for k, v in prose_result.matrix_params.items() if k not in combined.matrix_params}
        )
        combined.keyword_species += [
            k for k in prose_result.keyword_species if k not in combined.keyword_species
        ]
        if not combined.has_pfas_data and prose_result.has_pfas_data:
            combined.pfas_samples.update(prose_result.pfas_samples)
        combined.logs += prose_result.logs

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

    for key, value in text_result.llm_project_context.items():
        combined.llm_project_context.setdefault(key, value)

    combined.logs.append(
        f"=== Parse complete: {len(combined.pfas_samples)} sample(s), "
        f"{len(screening_matrix(combined)[0])} matrix param(s), "
        f"{len(combined.keyword_species)} keyword(s) ==="
    )

    return combined
