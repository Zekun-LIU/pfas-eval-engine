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

    # ── Extract matrix parameters from all sheets ─────────────────────────────
    matrix_params = _extract_matrix_from_excel(sheets, logs)
    if matrix_params:
        result.matrix_params.update(matrix_params)
        logs.append(f"[Excel] Matrix params auto-extracted: {list(matrix_params.keys())}")

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
        ):
            logs.append(f"[Excel] Row {ri + 1}: skipping sum/bulk parameter: {analyte_raw!r}")
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
    "nitrite": [
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
    "pH", "temperature", "conductivity", "turbidity", "UV254", "UVT254", "flow_rate",
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


def _store_param(
    params: Dict[str, float],
    param: str,
    val: float,
    unit: str,
    label: str,
    logs: List[str],
) -> None:
    """Convert value to canonical unit and store in params dict."""
    if param in _MATRIX_NATIVE_UNIT_PARAMS or not unit:
        params[param] = val
        logs.append(f"[Excel Matrix] {label}: {param} = {val} (native)")
    else:
        mg_l = convert_to_mg_L(val, unit)
        if mg_l is None:
            mg_l = val  # unknown unit — assume already mg/L
        params[param] = mg_l
        logs.append(f"[Excel Matrix] {label}: {param} = {mg_l} mg/L  (raw {val} {unit})")


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
                     f"'{sheet_name}' r{ri + 1} (row-layout)", logs)

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
        logs.append(f"[Excel Matrix] '{sheet_name}': column-layout — no value row found")
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
                     f"'{sheet_name}' col {ci + 1} (col-layout)", logs)

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
    # ── Nitrogen species ─────────────────────────────────────────────────────
    "ammonia": [
        (r"(?:NH3|NH4\+?|ammonia|ammonium)\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
        (r"ammonia[-\s]*N\s*[=:~≈]\s*([0-9.]+)\s*(mg/[lL]|ppm)", "mg/L"),
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
