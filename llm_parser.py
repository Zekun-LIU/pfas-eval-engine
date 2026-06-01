"""
llm_parser.py — Enhanced Data-Reading via LLM Pre-Parser

Uses Claude Haiku as a smart pre-parser to extract structured PFAS and
water-quality data from Excel sheets, PDFs, and customer emails, then maps
the output to the same ParsedData container that the rule-based parser
produces.  The expert evaluation engine (engine.py) is completely unchanged.

Architecture:
  raw file bytes
    → _excel_to_text() / _pdf_to_text()
    → Claude Haiku (structured JSON)
    → parse_from_llm_json()
    → ParsedData  →  evaluate()  →  EvaluationResult

Claros R&D Team | Framework Architecture by Zack Liu
"""
from __future__ import annotations

import io
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from utils import convert_to_mg_L, normalize_pfas_name
from parser import ParsedData, SampleMetadata


# ═══════════════════════════════════════════════════════════════════════════════
# LLM EXTRACTION PROMPT
# ═══════════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """\
You are a precise data-extraction assistant for a PFAS water treatment evaluation system.

Your task: extract ALL relevant data from the provided document and return it as a single \
JSON object following the schema at the end of this prompt.
Return ONLY valid JSON — no markdown fencing, no explanation, no code fences.

EXTRACTION RULES
1. Extract exact numeric values — never estimate or interpolate.
2. BLANK / EMPTY cell  →  set value: null, is_missing: true
   (This is categorically different from a non-detect result.)
3. <MDL / ND / <RL / <DL / non-detect / n.d. / "not detected"
   →  set value: null, is_nd: true
4. Statistical summaries: if a sample column header contains "Average", "Moyenne",
   "Maximum", "Max", "Minimum", "Min", "Median", "Typical", "Representatif" etc.
   → set is_statistical_summary: true and fill summary_type accordingly.
   These are NOT independent samples — mark them explicitly.
5. AOF (Adsorbable Organic Fluorine) and TOF (Total Organic Fluorine) are BULK
   parameters — do NOT include them in pfas_measurements. Put them in the aof/tof
   fields of the relevant sample.
6. Keep the unit string exactly as found in the document.
7. For water matrix: search ALL sheets/sections, including non-English text.
   Common translations: DCO=COD, DBO=BOD, DCO/DBO in French; CSB in German;
   Leitfähigkeit=conductivity; Conductivité=conductivity; pH is universal.
8. treatment_goals_extracted: any mention of target concentrations, regulatory
   discharge limits, treatment objectives, or project drivers found in the document.
9. parse_notes: if you are uncertain about any extraction, or had to make an
   assumption, add a clear explanation.
10. Throughput / flow rate: if the document mentions a treatment capacity, flow rate,
    water volume processed per unit time, or project scale in any unit
    (GPM, gal/min, L/min, m³/h, m³/day, m3/d, L/s, MGD …) extract it as
    flow_rate_value (number) and flow_rate_unit (string) in the project section.
    French: débit, capacité de traitement, volume traité.
    German: Durchfluss, Durchsatz, Volumenstrom.

JSON SCHEMA (return a JSON object that matches this structure exactly):
{
  "project": {
    "customer_name": "string or null",
    "site_name": "string or null",
    "country": "string or null",
    "flow_rate_value": "number or null",
    "flow_rate_unit": "string or null"
  },
  "samples": [
    {
      "name": "exact column header from document",
      "is_statistical_summary": false,
      "summary_type": "null | average | maximum | minimum | median | typical",
      "pfas_measurements": [
        {
          "name": "species name exactly as in document",
          "value": "number or null",
          "unit": "string (ng/L, µg/L, mg/L, ppt, ppb, ppm — as in document)",
          "is_nd": false,
          "is_missing": false
        }
      ],
      "aof": {"value": "number or null", "unit": "string or null"},
      "tof": {"value": "number or null", "unit": "string or null"}
    }
  ],
  "water_matrix": {
    "pH": "number or null",
    "COD":         {"value": "number or null", "unit": "string or null"},
    "TOC":         {"value": "number or null", "unit": "string or null"},
    "DOC":         {"value": "number or null", "unit": "string or null"},
    "BOD":         {"value": "number or null", "unit": "string or null"},
    "nitrate":     {"value": "number or null", "unit": "string or null"},
    "nitrite":     {"value": "number or null", "unit": "string or null"},
    "ammonia":     {"value": "number or null", "unit": "string or null"},
    "TKN":         {"value": "number or null", "unit": "string or null"},
    "TN":          {"value": "number or null", "unit": "string or null"},
    "TP":          {"value": "number or null", "unit": "string or null"},
    "conductivity":{"value": "number or null", "unit": "string or null"},
    "turbidity":   {"value": "number or null", "unit": "string or null"},
    "TSS":         {"value": "number or null", "unit": "string or null"},
    "TDS":         {"value": "number or null", "unit": "string or null"},
    "hardness":    {"value": "number or null", "unit": "string or null"},
    "alkalinity":  {"value": "number or null", "unit": "string or null"},
    "chloride":    {"value": "number or null", "unit": "string or null"},
    "fluoride":    {"value": "number or null", "unit": "string or null"},
    "sulfate":     {"value": "number or null", "unit": "string or null"},
    "temperature": {"value": "number or null", "unit": "string or null"},
    "UV254":       {"value": "number or null", "unit": "string or null"},
    "iron":        {"value": "number or null", "unit": "string or null"},
    "manganese":   {"value": "number or null", "unit": "string or null"},
    "copper":      {"value": "number or null", "unit": "string or null"},
    "zinc":        {"value": "number or null", "unit": "string or null"},
    "aluminum":    {"value": "number or null", "unit": "string or null"},
    "nickel":      {"value": "number or null", "unit": "string or null"},
    "chromium":    {"value": "number or null", "unit": "string or null"},
    "lead":        {"value": "number or null", "unit": "string or null"},
    "arsenic":     {"value": "number or null", "unit": "string or null"},
    "mercury":     {"value": "number or null", "unit": "string or null"},
    "cadmium":     {"value": "number or null", "unit": "string or null"}
  },
  "treatment_goals_extracted": "string or null",
  "parse_notes": ["list of strings"]
}
"""

# ── Flow-rate conversion table → GPM ─────────────────────────────────────────
_FLOW_TO_GPM: Dict[str, float] = {
    "gpm": 1.0, "gal/min": 1.0, "gallon/min": 1.0, "gallons/min": 1.0,
    "gpd": 1.0 / 1440,          # gallons/day
    "mgd": 694.444,              # million gallons/day
    "l/min": 0.264172, "lpm": 0.264172, "l/m": 0.264172,
    "l/h": 0.004403, "l/hr": 0.004403,
    "l/s": 15.8503,
    "m3/h": 4.40287, "m³/h": 4.40287, "m3/hr": 4.40287,
    "m3/day": 0.183448, "m³/day": 0.183448, "m3/d": 0.183448, "m³/d": 0.183448,
    "m3/s": 15850.3, "m³/s": 15850.3,
}

def _convert_flow_to_gpm(value: float, unit: str) -> Optional[float]:
    """Convert any flow-rate value to US GPM. Returns None if unit unknown."""
    key = unit.lower().strip().replace(" ", "").replace("·", "")
    factor = _FLOW_TO_GPM.get(key)
    return value * factor if factor is not None else None


# Parameters that use their own native unit (not converted to mg/L)
_NATIVE_UNIT_PARAMS = frozenset({
    "pH", "temperature", "conductivity", "turbidity", "UV254", "UVT254",
})

# LLM field name → canonical engine parameter name (where they differ)
_LLM_TO_CANONICAL = {
    "nitrite": "NO2",
}


# ═══════════════════════════════════════════════════════════════════════════════
# DOCUMENT → TEXT CONVERSION
# ═══════════════════════════════════════════════════════════════════════════════

def _score_sheet_pfas_relevance(df: "pd.DataFrame") -> int:
    """
    Return 0-100 relevance score for a sheet based on PFAS-related content.
    Higher score → prioritise this sheet in the document text budget.
    """
    if df is None or df.empty:
        return 0
    # Sample up to 300 cells to keep scoring fast
    flat = df.astype(str).values.flatten()[:300]
    text = " ".join(flat).lower()
    pfas_markers = [
        "pfos", "pfoa", "pfba", "pfca", "pfsa", "pfhxa", "pfna", "pfda",
        "perfluoro", "fluorotelomer", "determinand", "ng/l", "µg/l", "ug/l",
        "mg/l", "total fluorine", "organic fluorine", "tof", "aof", "ftsa",
    ]
    return min(sum(10 for m in pfas_markers if m in text), 100)


def _excel_to_text(file_bytes: bytes, filename: str) -> str:
    """
    Convert Excel sheets to pipe-separated text for LLM parsing.

    Key improvements over v1:
    - Scores each sheet for PFAS relevance and orders them highest-first
    - Skips clearly irrelevant sheets (score == 0) to save context budget
    - Raises the character budget from 7 000 → 28 000 so large CofA files
      with 50+ analytes × 8+ samples fit without truncation
    - Blank cells appear as "(blank)" so the LLM can distinguish blank from ND
    """
    try:
        import pandas as pd
    except ImportError:
        return "[ERROR: pandas not installed]"

    try:
        if filename.lower().endswith(".csv"):
            sheets = {"Sheet1": pd.read_csv(io.BytesIO(file_bytes), header=None, dtype=str)}
        else:
            sheets = pd.read_excel(
                io.BytesIO(file_bytes), header=None, dtype=str, sheet_name=None
            )
    except Exception as e:
        return f"[ERROR reading Excel '{filename}': {e}]"

    # Score and sort: PFAS-relevant sheets first, discard zero-score sheets
    scored: List[Tuple[int, str, "pd.DataFrame"]] = []
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue
        score = _score_sheet_pfas_relevance(df)
        scored.append((score, sheet_name, df))

    scored.sort(key=lambda x: -x[0])  # highest score first

    parts: List[str] = []
    for score, sheet_name, df in scored:
        if score == 0:
            continue  # skip metadata-only / empty sheets
        parts.append(f"\n=== Sheet: {sheet_name} (relevance: {score}) ===")
        for ri in range(len(df)):  # include ALL rows — no row cap
            row_cells = []
            for ci in range(len(df.columns)):
                raw = str(df.iloc[ri, ci]).strip()
                if raw.lower() in ("nan", "none", ""):
                    row_cells.append("(blank)")
                else:
                    row_cells.append(raw)
            if all(c == "(blank)" for c in row_cells):
                continue  # skip entirely blank rows
            parts.append(" | ".join(row_cells))

    text = "\n".join(parts)
    if len(text) > 28_000:
        text = text[:28_000] + "\n... [TRUNCATED — document continues]"
    return text


def _pdf_to_text(file_bytes: bytes, filename: str) -> str:
    """
    Extract text from a digital (text-selectable) PDF.
    Truncates to 7 000 characters.
    """
    try:
        import pdfplumber
    except ImportError:
        return "[ERROR: pdfplumber not installed]"

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            pages: List[str] = []
            for i, page in enumerate(pdf.pages[:20]):
                t = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
                if t.strip():
                    pages.append(f"[Page {i + 1}]\n{t}")
            full = "\n\n".join(pages)
            if len(full) > 7000:
                full = full[:7000] + "\n... [TRUNCATED]"
            return full
    except Exception as e:
        return f"[ERROR reading PDF '{filename}': {e}]"


# ═══════════════════════════════════════════════════════════════════════════════
# LLM CALL
# ═══════════════════════════════════════════════════════════════════════════════

def _call_claude_parser(
    document_text: str,
    goals_text: str,
    api_key: str,
) -> Tuple[str, dict]:
    """
    Send document text to Claude Sonnet for structured PFAS data extraction.
    Returns (raw_response_text, parsed_dict).
    Raises on API error or JSON parse failure.

    Uses claude-sonnet-4-5 (upgraded from Haiku) with max_tokens=8192 to handle
    large CofA files with 50+ analytes across 8+ samples without JSON truncation.
    """
    try:
        import anthropic
    except ImportError as e:
        raise RuntimeError("anthropic package not installed. Run: pip install anthropic") from e

    user_prompt = f"DOCUMENT CONTENT:\n{document_text}"
    if goals_text.strip():
        user_prompt += f"\n\nUSER-PROVIDED TREATMENT GOALS / CUSTOMER CONTEXT:\n{goals_text}"

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=8192,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown fences if the model added them despite instructions
    clean = re.sub(r"^```(?:json)?\s*", "", raw)
    clean = re.sub(r"\s*```$", "", clean).strip()

    # Extract the outermost JSON object
    json_match = re.search(r"\{[\s\S]*\}", clean)
    if not json_match:
        raise ValueError(
            f"Claude Sonnet did not return a JSON object. First 500 chars of response:\n{raw[:500]}"
        )

    data = json.loads(json_match.group())
    return raw, data


# Keep legacy name as alias so any external callers are unaffected
_call_claude_haiku = _call_claude_parser


# ═══════════════════════════════════════════════════════════════════════════════
# JSON → ParsedData MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

def parse_from_llm_json(data: dict, goals_text: str = "") -> ParsedData:
    """
    Map the structured JSON returned by Claude Haiku to a ParsedData object.
    The resulting ParsedData is consumed by the unchanged expert engine.
    """
    result = ParsedData()
    result.llm_raw_response = json.dumps(data, ensure_ascii=False, indent=2)

    # ── Project context ───────────────────────────────────────────────────────
    proj = data.get("project") or {}
    result.llm_project_context = {k: v for k, v in proj.items() if v is not None}

    # ── Treatment goals: merge user input with any goals extracted by LLM ─────
    goals_llm = (data.get("treatment_goals_extracted") or "").strip()
    combined_goals = "\n".join(filter(None, [goals_text.strip(), goals_llm]))
    result.treatment_goals_text = combined_goals

    # ── Parse notes ───────────────────────────────────────────────────────────
    result.llm_parse_notes = list(data.get("parse_notes") or [])

    # ── Samples ───────────────────────────────────────────────────────────────
    for sample_raw in data.get("samples") or []:
        sample_name = (sample_raw.get("name") or "Unknown Sample").strip()
        is_summary = bool(sample_raw.get("is_statistical_summary", False))
        summary_type = (sample_raw.get("summary_type") or "").lower()

        result.sample_metadata[sample_name] = SampleMetadata(
            is_statistical_summary=is_summary,
            summary_type=summary_type,
        )

        pfas_dict: Dict[str, float] = {}
        nd_list: List[str] = []

        for meas in sample_raw.get("pfas_measurements") or []:
            species_raw = (meas.get("name") or "").strip()
            if not species_raw:
                continue

            canonical = normalize_pfas_name(species_raw)
            is_nd = bool(meas.get("is_nd", False))
            is_missing = bool(meas.get("is_missing", False))
            value = meas.get("value")
            unit = (meas.get("unit") or "ng/L").strip()

            if is_missing:
                # do_not_assume_zero: blank cell → excluded entirely
                result.llm_parse_notes.append(
                    f"Sample '{sample_name}', '{species_raw}': blank cell — excluded (do_not_assume_zero)"
                )
            elif is_nd:
                nd_list.append(canonical)
            elif value is not None:
                try:
                    val_f = float(value)
                    val_mg = convert_to_mg_L(val_f, unit)
                    if val_mg is not None:
                        pfas_dict[canonical] = val_mg
                    else:
                        result.warnings.append(
                            f"[LLM] Unit conversion failed: {species_raw} = {value} {unit!r}"
                        )
                except (ValueError, TypeError):
                    result.warnings.append(
                        f"[LLM] Non-numeric value for {species_raw!r}: {value!r}"
                    )

        if pfas_dict:
            result.pfas_samples[sample_name] = pfas_dict
        if nd_list:
            result.nd_species[sample_name] = nd_list

        # AOF / TOF bulk values
        bulk: Dict[str, float] = {}
        for key, field_name in [("AOF", "aof"), ("TOF", "tof")]:
            bulk_data = sample_raw.get(field_name) or {}
            v = bulk_data.get("value")
            u = (bulk_data.get("unit") or "ng/L").strip()
            if v is not None:
                try:
                    v_mg = convert_to_mg_L(float(v), u)
                    if v_mg is not None:
                        bulk[key] = v_mg
                except (ValueError, TypeError):
                    pass
        if bulk:
            result.aof_tof_data[sample_name] = bulk

    # ── Water matrix ──────────────────────────────────────────────────────────
    matrix_raw = data.get("water_matrix") or {}

    for llm_key, val_data in matrix_raw.items():
        if val_data is None:
            continue

        canonical_key = _LLM_TO_CANONICAL.get(llm_key, llm_key)

        # pH is stored as a bare number in the schema
        if llm_key == "pH":
            if isinstance(val_data, (int, float)):
                result.matrix_params["pH"] = float(val_data)
            continue

        if not isinstance(val_data, dict):
            continue

        v = val_data.get("value")
        u = (val_data.get("unit") or "mg/L").strip()
        if v is None:
            continue

        try:
            v_float = float(v)
        except (ValueError, TypeError):
            continue

        if canonical_key in _NATIVE_UNIT_PARAMS:
            result.matrix_params[canonical_key] = v_float
        else:
            v_mg = convert_to_mg_L(v_float, u)
            if v_mg is not None:
                result.matrix_params[canonical_key] = v_mg
            else:
                # Unknown unit — store as-is and log
                result.matrix_params[canonical_key] = v_float
                result.warnings.append(
                    f"[LLM] Unknown unit '{u}' for {llm_key} — stored raw value {v_float}"
                )

    # ── Flow rate / throughput: convert to GPM and store in project context ──────
    flow_val  = proj.get("flow_rate_value")
    flow_unit = (proj.get("flow_rate_unit") or "").strip()
    if flow_val is not None:
        try:
            fv = float(flow_val)
            gpm = _convert_flow_to_gpm(fv, flow_unit)
            if gpm is not None:
                result.llm_project_context["throughput_gpm"] = round(gpm, 2)
                result.llm_project_context["flow_rate_display"] = f"{fv:g} {flow_unit}"
                result.llm_parse_notes.append(
                    f"Throughput extracted: {fv:g} {flow_unit} → {gpm:.1f} GPM"
                )
            else:
                # Unit unknown — store raw for display only
                result.llm_project_context["flow_rate_display"] = f"{fv:g} {flow_unit} (unit not converted)"
                result.llm_parse_notes.append(
                    f"Throughput extracted: {fv:g} {flow_unit} — unit not recognised, GPM conversion skipped"
                )
        except (ValueError, TypeError):
            pass

    result.logs.append(
        f"[LLM] parse_from_llm_json: {len(result.pfas_samples)} sample(s), "
        f"{len(result.matrix_params)} matrix param(s), "
        f"{len(result.llm_parse_notes)} parse note(s)"
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def parse_with_llm(
    excel_bytes: Optional[bytes],
    excel_filename: Optional[str],
    pdf_bytes: Optional[bytes],
    pdf_filename: Optional[str],
    goals_text: str,
    api_key: str,
) -> ParsedData:
    """
    Main LLM parsing entry point.

    1. Converts uploaded files to human-readable text
    2. Sends to Claude Haiku for structured extraction
    3. Maps JSON output to ParsedData

    On any failure falls back to the rule-based parse_all() so the app
    never crashes due to LLM unavailability.
    """
    # Import here to avoid circular imports at module load time
    from parser import parse_all  # noqa: F811

    has_excel = bool(excel_bytes and excel_filename)
    has_pdf = bool(pdf_bytes and pdf_filename)

    document_text = ""
    if has_excel:
        document_text += f"\n[EXCEL FILE: {excel_filename}]\n"
        document_text += _excel_to_text(excel_bytes, excel_filename)
    if has_pdf:
        document_text += f"\n[PDF FILE: {pdf_filename}]\n"
        document_text += _pdf_to_text(pdf_bytes, pdf_filename)

    if not document_text.strip() and not goals_text.strip():
        return ParsedData()

    try:
        raw_response, parsed_json = _call_claude_parser(document_text, goals_text, api_key)
        result = parse_from_llm_json(parsed_json, goals_text)
        result.has_excel = has_excel
        result.has_pdf = has_pdf
        result.has_text = bool(goals_text.strip())
        result.llm_raw_response = raw_response
        result.logs.insert(0, "[LLM] ✨ Enhanced Data-Reading active — Claude Sonnet pre-parser")
        return result

    except Exception as exc:
        # Graceful fallback: rule-based parser as safety net
        fallback = parse_all(
            excel_bytes=excel_bytes,
            excel_filename=excel_filename,
            pdf_bytes=pdf_bytes,
            pdf_filename=pdf_filename,
            email_text="",
            goals_text=goals_text,
        )
        fallback.warnings.append(
            f"[LLM] Enhanced Data-Reading failed ({exc!s}) — fell back to rule-based parser."
        )
        fallback.llm_raw_response = f"ERROR:\n{exc}"
        fallback.logs.insert(0, f"[LLM] FALLBACK — reason: {exc}")
        return fallback
