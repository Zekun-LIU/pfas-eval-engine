"""
app.py — PFAS Material Evaluation Engine
Streamlit web application entry point.

Claros Product Development Team | Framework Architecture by Zack Liu
"""

from __future__ import annotations

import sys
import os

# Ensure local modules are importable regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import html

import streamlit as st

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION  (must be first Streamlit call)
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="PFAS Material Evaluation Engine",
    page_icon="⚗",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Late imports (after page config)
from engine import (
    EvaluationResult, Module1Result, Module2Result, Module3Result,
    SampleResult, TOFAnalysisResult, evaluate,
    M3_COD_MAX_MG_L, M3_TOC_MAX_MG_L,
    M3_NITRATE_MANAGEABLE, M3_NITRATE_HIGH,
    M3_CHLORIDE_CORROSION, M3_FLUORIDE_TOF,
    M3_HARDNESS_PRECIP, M3_AMMONIA_HIGH,
    M3_TKN_HIGH, M3_METAL_FLAG_PPM,
    TOF_COVERAGE_THRESHOLD,
)
from file_readers import ACCEPTED_FORMATS_LABEL, read_upload, split_context_documents
from parser import (
    ParsedData, describe_flow, design_flow_gpm, parse_all, sample_matrices, screening_matrix,
)
from utils import (
    CATEGORY_LABELS,
    format_conc_auto,
    format_pct,
)
from module4 import (
    generate_test_plans, build_plan_text, TestPlan,
    SHORT_TIMEPOINTS, LONG_TIMEPOINTS, LONG_DENSE_TIMEPOINTS,
)


# ═══════════════════════════════════════════════════════════════════════════════
# API KEY HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _get_api_key() -> str | None:
    """Return Anthropic API key from Streamlit secrets, or None if not configured."""
    try:
        return st.secrets.get("ANTHROPIC_API_KEY") or None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# LLM EMAIL GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _generate_llm_email(result: EvaluationResult, project_context: dict, api_key: str) -> str:
    """
    Use Claude to write a professional business email from evaluation results.
    Falls back to the template draft if the API call fails.
    """
    # Build a compact structured summary for the LLM
    lines: list[str] = [f"OVERALL STATUS: {result.overall_status}"]
    if project_context.get("customer_name"):
        lines.append(f"CUSTOMER: {project_context['customer_name']}")
    if project_context.get("site_name"):
        lines.append(f"SITE: {project_context['site_name']}")
    if project_context.get("country"):
        lines.append(f"COUNTRY: {project_context['country']}")
    if project_context.get("throughput_gpm"):
        lines.append(f"THROUGHPUT: {project_context['throughput_gpm']:.0f} GPM"
                     + (" ⚠️ LARGE-SCALE (>100 GPM)" if float(project_context['throughput_gpm']) > 100 else ""))
    if project_context.get("flow_rate_display"):
        lines.append(f"  ({project_context['flow_rate_display']} as reported)")

    # Worst-case sample PFAS profile
    worst = None
    if result.samples:
        worst = max(
            result.samples,
            key=lambda sr: (
                {"CRITICAL": 2, "CONDITIONAL": 1, "PROCEED": 0}.get(sr.sample_status, 0),
                sr.module1.total_conc_mg_L,
            ),
        )
    if worst and worst.module1.total_conc_mg_L > 0:
        m1 = worst.module1
        lines.append(f"\nPFAS PROFILE (WORST CASE: {worst.sample_name}):")
        lines.append(f"  Total PFAS: {format_conc_auto(m1.total_conc_mg_L)}")
        for s in m1.species[:6]:
            if s.detected:
                lines.append(f"  {s.name}: {format_conc_auto(s.conc_mg_L)} ({s.percentage:.1f}%)")
        for cat, frac in sorted(m1.category_fractions.items(), key=lambda x: -x[1]):
            if frac > 0.01:
                lines.append(f"  Category {CATEGORY_LABELS.get(cat, cat)}: {format_pct(frac * 100)}")
        # TOF coverage
        if worst.tof_result is not None:
            tof = worst.tof_result
            lines.append(
                f"  TOF Coverage: {tof.coverage_ratio * 100:.1f}% "
                f"(theoretical {format_conc_auto(tof.theoretical_tof_mg_L)} vs "
                f"reported {tof.measured_type} {format_conc_auto(tof.measured_mg_L)})"
            )
            if tof.unknown_pfas_flag:
                lines.append("  ⚠️ LOW TOF COVERAGE: significant unknown/unidentified PFAS present")

        # Average case if available
        if worst.avg_module1 is not None:
            avg_m1 = worst.avg_module1
            lines.append(f"\nPFAS PROFILE (AVERAGE / STEADY-STATE):")
            lines.append(f"  Total PFAS: {format_conc_auto(avg_m1.total_conc_mg_L)}")
            for s in avg_m1.species[:4]:
                if s.detected:
                    lines.append(f"  {s.name}: {format_conc_auto(s.conc_mg_L)}")

    if result.comparison is not None:
        lines.append("\nSAMPLE COMPARISON AND BASIS OF DESIGN:")
        if result.comparison.legend:
            lines.append(f"  Sample labels: {result.comparison.legend}")
        for text in (result.comparison.headline, *result.comparison.findings,
                     *result.comparison.design_notes):
            lines.append(f"  - {text}")
    flows = {n: f for n, f in result.sample_flow.items() if describe_flow(f)}
    if flows:
        lines.append("\nFLOW BY SAMPLE:")
        for name, flow in flows.items():
            hours = f", {flow['hours_avg']:g} h/day" if flow.get("hours_avg") is not None else ""
            lines.append(f"  {name}: {describe_flow(flow)}{hours}")

    # Key flags — include all severities for the model to reason over
    lines.append("\nKEY FLAGS:")
    seen: set = set()
    for sr in result.samples:
        for f in sr.module2.flags:
            if f.severity in ("critical", "commercial", "technical", "pathway") and f.message not in seen:
                lines.append(f"  [{f.severity.upper()}] {f.message}")
                seen.add(f.message)
    for f in result.module3.flags:
        if f.severity in ("warning", "commercial") and f.message not in seen:
            lines.append(f"  [MATRIX/PROJ] {f.message}")
            seen.add(f.message)

    if getattr(result, "analytical_flag", None):
        lines.append("\nANALYTICAL CONSTRAINT:")
        lines.append(f"  [WARNING] {result.analytical_flag.message}")
        lines.append(f"  {result.analytical_flag.detail}")

    lines.append("\nTREATMENT IMPLICATIONS:")
    for item in result.treatment_summary[:5]:
        lines.append(f"  - {item}")

    if result.missing_info:
        lines.append("\nMISSING INFORMATION:")
        for item in result.missing_info[:5]:
            lines.append(f"  - {item}")

    summary = "\n".join(lines)

    system = (
        "You are a senior technical sales engineer at Claros Water Technologies writing an "
        "internal business email to your team about a PFAS treatment opportunity.\n\n"
        "Write a professional, clear email (280–380 words) grounded in the evaluation data.\n\n"
        "FORMAT:\n"
        "Subject: PFAS Treatment Feasibility: [customer/site descriptor + status]\n\n"
        "[Opening, 2 sentences: opportunity context, overall verdict]\n\n"
        "[PFAS Profile, 3 sentences: key species with actual concentrations, composition "
        "category, TOF coverage finding if relevant]\n\n"
        "[Key Technical Findings, 4-6 bullet points drawn from the evaluation flags]\n\n"
        "[Scale & Project Context, 1 short paragraph: throughput, any large-scale flag, "
        "site/country context]\n\n"
        "[Recommendation, 1 paragraph: PROCEED / PROCEED WITH CONDITIONS / DO NOT PROCEED "
        "with specific reasoning]\n\n"
        "[Next Steps, 3 concrete action items]\n\n"
        "Claros Product Development Team | PFAS Evaluation Engine\n\n"
        "RULES:\n"
        "- Use actual species names and concentrations from the data; never invent numbers.\n"
        "- If CRITICAL flag exists, lead with it in the opening.\n"
        "- If TOF coverage is low (<50%), name it as a key risk: unknown PFAS cannot be "
        "guaranteed to be treated.\n"
        "- If an ANALYTICAL CONSTRAINT is present (ultra-low / drinking-water target), state "
        "it clearly: analytical capability must be confirmed before testing, and the "
        "treatability timeline extends 2–3 weeks for contamination control.\n"
        "- If throughput >100 GPM, call out the large-scale flag explicitly.\n"
        "- If a SAMPLE COMPARISON is present, say whether the samples are uniform, give the "
        "max/min spread in total PFAS and the largest single differences, and state what that "
        "means for the basis of design. Report flow per sample when it is given per sample.\n"
        "- Do not use em dashes; use commas, colons, semicolons or full stops instead.\n"
        "- Return ONLY the email text: no markdown fencing, no preamble."
    )

    try:
        from llm_parser import call_claude
        return call_claude(
            api_key, system, f"EVALUATION RESULTS:\n{summary}", max_tokens=16_000, effort="medium"
        )
    except Exception as exc:
        return f"[AI email generation failed: {exc}]\n\n{result.email_draft}"

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CSS
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    :root {
        --ink: #1B1B1B;
        --muted: #5C5C5C;
        --rule: #CFCFCF;
        --rule-light: #E6E6E6;
        --tint: #F4F4F2;
        --accent: #1F4E79;
        --sev-critical: #A61B1B;
        --sev-warning: #8F5200;
        --sev-ok: #2E6B30;
        --sev-info: #1F4E79;
        --sev-commercial: #5B3A8C;
        --sev-technical: #8F5200;
        --sev-other: #555555;
        /* SF Pro on Apple devices; Inter, its closest open relative, everywhere else */
        --sans: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Inter", "Segoe UI",
                "Helvetica Neue", Arial, sans-serif;
        --mono: ui-monospace, "SF Mono", "Cascadia Mono", Consolas, monospace;
    }

    html, body, [class*="css"] {
        font-family: var(--sans);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        font-feature-settings: "cv11", "ss03";
        color: var(--ink);
        line-height: 1.5;
    }
    .block-container { padding-top: 3.4rem; padding-bottom: 2.5rem; max-width: 1180px; }
    section[data-testid="stSidebar"] { display: none; }
    .mono, code { font-family: var(--mono); font-size: 0.92em; }
    .stApp, .stApp p, .stApp li, .stApp label, .stApp input, .stApp textarea, .stApp button,
    .stApp [data-testid="stMarkdownContainer"], .stApp [data-testid="stCaptionContainer"] {
        font-family: var(--sans);
    }

    /* ── Document header ─────────────────────────────────────── */
    .rpt-header {
        display: flex; justify-content: space-between; align-items: flex-end;
        gap: 24px; flex-wrap: wrap;
        border-bottom: 2px solid var(--ink);
        padding: 4px 0 10px 0; margin-bottom: 18px;
    }
    .rpt-header .org { font-size: 0.82rem; font-weight: 600; color: var(--accent); }
    .rpt-header h1 {
        margin: 2px 0 0 0; padding: 0; font-size: 1.5rem; font-weight: 600;
        color: var(--ink); line-height: 1.2; letter-spacing: -0.02em;
        font-family: var(--sans);
    }
    .rpt-header .sub { font-size: 0.88rem; color: var(--muted); margin-top: 2px; }
    .rpt-header .meta { font-size: 0.76rem; color: var(--muted); text-align: right; line-height: 1.5; }

    /* ── Report headings ─────────────────────────────────────── */
    .rpt-h2 {
        font-size: 1.05rem; font-weight: 600; color: var(--ink); letter-spacing: -0.01em;
        margin: 26px 0 8px 0; padding-bottom: 4px;
        border-bottom: 1px solid var(--ink);
    }
    .rpt-h2 .n {
        font-weight: 500; font-variant-numeric: tabular-nums;
        color: var(--muted); margin-right: 12px;
    }
    .rpt-h3 { font-size: 0.9rem; font-weight: 600; color: var(--ink); margin: 18px 0 6px 0; }
    .field-label { font-size: 0.85rem; font-weight: 600; color: var(--ink); margin-bottom: 4px; }
    .field-sublabel { font-size: 0.78rem; color: var(--muted); margin: 4px 0 -4px 0; }
    .muted { color: var(--muted); font-size: 0.84rem; }

    /* ── Tables ──────────────────────────────────────────────── */
    table.rpt {
        border-collapse: collapse; width: 100%; margin: 4px 0 12px 0;
        font-size: 0.84rem; border: none;
    }
    table.rpt th, table.rpt td {
        border: none; border-bottom: 1px solid var(--rule-light);
        padding: 6px 14px 6px 0; text-align: left; vertical-align: top;
        background: transparent; color: var(--ink);
    }
    table.rpt th { font-weight: 600; border-bottom: 1px solid var(--ink); white-space: nowrap; }
    table.rpt tr:last-child td { border-bottom: 1px solid var(--rule); }
    table.rpt .num {
        text-align: right;
        font-variant-numeric: tabular-nums; white-space: nowrap;
    }
    table.rpt .code {
        font-family: var(--mono); font-size: 0.76rem; white-space: nowrap;
    }
    table.rpt th.num, table.rpt th.code { font-family: inherit; font-size: inherit; }
    table.rpt tr, table.rpt thead { border: none !important; background: transparent !important; }
    table.rpt .detail { color: var(--muted); font-size: 0.79rem; margin-top: 3px; line-height: 1.45; }
    table.kv th { border-bottom: none; color: var(--muted); font-weight: 500; font-size: 0.78rem; padding-bottom: 0; }
    table.kv td { border-bottom: 1px solid var(--rule); font-variant-numeric: tabular-nums; font-size: 0.95rem; font-weight: 500; }

    /* ── Severity labels: text + square marker, no pills ──────── */
    .sev { white-space: nowrap; font-weight: 600; font-size: 0.8rem; }
    .sev::before {
        content: ""; display: inline-block; width: 7px; height: 7px;
        margin-right: 7px; vertical-align: 1px; background: currentColor;
    }
    .sev-critical   { color: var(--sev-critical); }
    .sev-warning    { color: var(--sev-warning); }
    .sev-ok, .sev-pathway { color: var(--sev-ok); }
    .sev-info       { color: var(--sev-info); }
    .sev-commercial { color: var(--sev-commercial); }
    .sev-technical  { color: var(--sev-technical); }
    .sev-special_handling, .sev-none { color: var(--sev-other); }
    .status-line { font-size: 0.95rem; margin: 4px 0 8px 0; }
    .status-line .sev { font-size: 1rem; letter-spacing: 0.3px; }

    /* ── Notes (replaces coloured callout cards) ─────────────── */
    .note {
        border-left: 2px solid var(--rule); padding: 1px 0 1px 12px;
        margin: 8px 0; font-size: 0.85rem; line-height: 1.55; color: var(--ink);
    }
    .note-critical { border-left-color: var(--sev-critical); }
    .note-warning  { border-left-color: var(--sev-warning); }
    .note-info     { border-left-color: var(--sev-info); }
    .note-ok       { border-left-color: var(--sev-ok); }
    .note .detail  { color: var(--muted); font-size: 0.8rem; }

    ol.rpt-list, ul.rpt-list { margin: 4px 0 8px 0; padding-left: 22px; font-size: 0.88rem; }
    ol.rpt-list li, ul.rpt-list li { margin: 2px 0; }

    /* ── Email draft ─────────────────────────────────────────── */
    .email-box {
        background: #FFFFFF; border: 1px solid var(--rule); border-radius: 2px;
        padding: 20px 24px; font-size: 0.88rem; white-space: pre-wrap;
        line-height: 1.7; color: var(--ink) !important;
    }

    /* ── Inputs ──────────────────────────────────────────────── */
    [data-testid="stTextArea"] textarea::placeholder { color: #9A9A9A !important; opacity: 1 !important; }
    [data-testid="stFileUploaderDropzone"] { background: #FFFFFF; border: 1px dashed #BDBDBD; }

    /* ── Buttons ─────────────────────────────────────────────── */
    .st-key-run_btn button { width: 100%; }
    div[data-testid="stButton"] > button {
        min-width: 180px; background: var(--accent); color: #FFFFFF !important;
        font-weight: 600; font-size: 0.92rem; padding: 9px 12px;
        border: 1px solid var(--accent); border-radius: 2px; cursor: pointer;
    }
    div[data-testid="stButton"] > button:hover  { background: #173B5C; border-color: #173B5C; }
    div[data-testid="stButton"] > button:active { background: #112C45; }
    [data-testid="stDownloadButton"] > button {
        background: #FFFFFF !important; border: 1px solid #8A8A8A !important;
        color: var(--ink) !important; font-size: 0.85rem !important;
        font-weight: 500 !important; border-radius: 2px !important; cursor: pointer !important;
    }
    [data-testid="stDownloadButton"] > button:hover { background: var(--tint) !important; }

    /* ── Tabs ────────────────────────────────────────────────── */
    button[data-baseweb="tab"] {
        font-size: 0.9rem !important; font-weight: 500 !important;
        padding: 8px 2px !important; margin-right: 26px !important; color: var(--muted) !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] { font-weight: 600 !important; color: var(--ink) !important; }
    div[data-baseweb="tab-highlight"] { background-color: var(--ink) !important; height: 2px !important; }
    div[data-baseweb="tab-panel"] { padding-top: 10px !important; }

    hr { border-color: var(--rule) !important; }
    [data-testid="stExpander"] details { border-radius: 2px !important; }

    /* ── Run progress (thin bar instead of spinner ring) ─────── */
    .prog { margin: 10px 0 18px 0; font-size: 0.85rem; color: var(--muted); }
    .prog-bar { height: 3px; background: var(--rule-light); margin-top: 6px; }
    .prog-bar > div { height: 3px; background: var(--accent); }

    .empty-state { color: var(--muted); padding: 22px 0 30px 0; font-size: 0.88rem; }

    .rpt-footer {
        font-size: 0.74rem; color: var(--muted); margin-top: 40px; padding-top: 10px;
        border-top: 1px solid var(--rule);
    }

    [data-testid="stStatusWidget"] { display: none !important; }

    button:focus-visible, [data-baseweb="tab"]:focus-visible, textarea:focus-visible {
        outline: 2px solid var(--accent) !important; outline-offset: 2px !important;
    }
    @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after { animation: none !important; transition: none !important; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════════════
# RENDERING FUNCTIONS  (defined before any UI calls)
# ═══════════════════════════════════════════════════════════════════════════════

_SEV_LABEL = {
    "critical":         "Critical",
    "commercial":       "Commercial",
    "technical":        "Technical",
    "pathway":          "Pathway",
    "special_handling": "Special handling",
    "warning":          "Warning",
    "info":             "Info",
    "ok":               "OK",
}
_STATUS_SEV = {"PROCEED": "ok", "CONDITIONAL": "warning", "CRITICAL": "critical"}


def _sev_html(severity: str, label: str | None = None) -> str:
    text = label or _SEV_LABEL.get(severity, severity.replace("_", " ").capitalize())
    return f'<span class="sev sev-{severity}">{text}</span>'


def _heading(text: str, num: str | None = None) -> None:
    n = f'<span class="n">{num}</span>' if num else ""
    st.markdown(f'<div class="rpt-h2">{n}{text}</div>', unsafe_allow_html=True)


def _subheading(text: str) -> None:
    st.markdown(f'<div class="rpt-h3">{text}</div>', unsafe_allow_html=True)


def _note(body: str, severity: str = "", detail: str = "") -> None:
    cls = f" note-{severity}" if severity else ""
    detail_html = f'<div class="detail">{detail}</div>' if detail else ""
    st.markdown(f'<div class="note{cls}">{body}{detail_html}</div>', unsafe_allow_html=True)


def _html_table(
    rows: list[dict],
    num_cols: tuple = (),
    code_cols: tuple = (),
    html_cols: tuple = (),
    css: str = "rpt",
) -> None:
    """Static report table. Cells are escaped unless their column is in html_cols."""
    if not rows:
        return
    cols = list(rows[0].keys())

    def _cls(c: str) -> str:
        if c in num_cols:
            return ' class="num"'
        if c in code_cols:
            return ' class="code"'
        return ""

    head = "".join(f"<th{_cls(c)}>{html.escape(c)}</th>" for c in cols)
    body = "".join(
        "<tr>" + "".join(
            f"<td{_cls(c)}>"
            + (str(r.get(c, "")) if c in html_cols else html.escape(str(r.get(c, ""))))
            + "</td>"
            for c in cols
        ) + "</tr>"
        for r in rows
    )
    st.markdown(
        f'<table class="{css}"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>',
        unsafe_allow_html=True,
    )


def _kv_table(pairs: list[tuple[str, str]]) -> None:
    """One-row summary table: labels above values (replaces metric cards)."""
    _html_table([{k: v for k, v in pairs}], css="rpt kv")


def _flags_table(flags) -> None:
    """All findings render as rows of one table: rule, severity, finding (+ detail)."""
    flags = list(flags)
    if not flags:
        return
    rows = []
    for f in flags:
        detail = f'<div class="detail">{f.detail}</div>' if f.detail else ""
        rows.append({
            "Rule": html.escape(str(f.rule_id)),
            "Severity": _sev_html(f.severity),
            "Finding": f"{f.message}{detail}",
        })
    _html_table(rows, code_cols=("Rule",), html_cols=("Rule", "Severity", "Finding"))


def _render_flag(flag) -> None:
    _flags_table([flag])


def _param_traffic_light(key: str, value) -> str:
    """
    Return a traffic-light emoji for a Module 3 matrix parameter based on engine thresholds.
    🟢 = OK / within range
    🟡 = moderate concern / watch
    🔴 = exceeded / concern
    ⚪ = no threshold defined for this parameter
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "⚪"

    if key == "COD":
        return "🟢" if v <= M3_COD_MAX_MG_L else "🔴"
    if key in ("TOC", "DOC"):
        return "🟢" if v <= M3_TOC_MAX_MG_L else "🔴"
    if key in ("nitrate", "NO2"):
        if v < M3_NITRATE_MANAGEABLE:
            return "🟢"
        if v <= M3_NITRATE_HIGH:
            return "🟡"
        return "🔴"
    if key == "chloride":
        return "🟢" if v <= M3_CHLORIDE_CORROSION else "🔴"
    if key == "fluoride":
        return "🟢" if v <= M3_FLUORIDE_TOF else "🔴"
    if key == "hardness":
        return "🟢" if v <= M3_HARDNESS_PRECIP else "🟡"
    if key == "ammonia":
        return "🟢" if v <= M3_AMMONIA_HIGH else "🟡"
    if key == "TKN":
        return "🟢" if v <= M3_TKN_HIGH else "🟡"
    if key in ("iron", "manganese", "copper", "zinc", "aluminum",
               "nickel", "chromium", "lead"):
        return "🟢" if v <= M3_METAL_FLAG_PPM else "🟡"
    if key == "TP":
        # Total Phosphorus: < 1 mg/L OK, 1–10 mg/L Watch, > 10 mg/L Concern
        if v < 1.0:
            return "🟢"
        if v <= 10.0:
            return "🟡"
        return "🔴"
    # pH, turbidity, TSS, BOD, TN, conductivity, TDS, sulfate, temperature,
    # UV254, UVT254, phosphate, arsenic, cadmium, mercury, silver, …
    return "⚪"


def _render_tof_analysis(tof: "TOFAnalysisResult") -> None:
    """Render the TOF/AOF theoretical coverage analysis for a single sample."""
    _subheading("TOF coverage: theoretical vs reported organic fluorine")
    _kv_table([
        ("Theoretical TOF (as F)", format_conc_auto(tof.theoretical_tof_mg_L)),
        (f"Reported {tof.measured_type} (as F)", format_conc_auto(tof.measured_mg_L)),
        ("Coverage", f"{tof.coverage_ratio * 100:.1f}%"),
    ])
    if tof.species_contributions:
        _html_table(
            [
                {
                    "Species": name,
                    "Concentration": format_conc_auto(conc),
                    "F contribution (as F)": format_conc_auto(f_c),
                    "% of theoretical TOF": (
                        f"{f_c / tof.theoretical_tof_mg_L * 100:.1f}%"
                        if tof.theoretical_tof_mg_L > 0 else "n/a"
                    ),
                }
                for name, conc, f_c in tof.species_contributions
            ],
            num_cols=("Concentration", "F contribution (as F)", "% of theoretical TOF"),
        )
    _flags_table(tof.flags)
    if tof.unknown_pfas_flag:
        st.caption(
            f"Threshold: theoretical TOF must be at least {TOF_COVERAGE_THRESHOLD * 100:.0f}% "
            f"of reported {tof.measured_type} to clear the unknown-PFAS flag."
        )


def _category_rows(m1) -> list[dict]:
    return [
        {"Category": CATEGORY_LABELS.get(cat, cat), "Share of total": format_pct(frac * 100)}
        for cat, frac in sorted(m1.category_fractions.items(), key=lambda x: -x[1])
        if frac > 0.001
    ]


def _render_sample_section(
    sr: SampleResult,
    expanded: bool = True,
    nd_species: "List[str] | None" = None,
) -> None:
    """Render Module 1 + 2 results for a single sample."""
    from utils import get_pfas_info

    m1 = sr.module1
    m2 = sr.module2

    with st.expander(
        f"Sample: **{sr.sample_name}**  ·  {sr.sample_status.capitalize()}  ·  "
        f"Total PFAS {format_conc_auto(m1.total_conc_mg_L)}",
        expanded=expanded,
    ):
        # ── Module 1 ──────────────────────────────────────────────────────────
        _subheading("PFAS composition (Module 1)")

        if m1.species:
            _kv_table([
                ("Total PFAS (detected)", format_conc_auto(m1.total_conc_mg_L)),
                ("Primary set (top 5, ≥5%)", str(len(m1.primary_set))),
                ("Top-5 cumulative", format_pct(m1.top5_cumulative_pct)),
                ("Other fraction", format_pct(m1.other_fraction_pct)),
            ])
            _html_table(
                [
                    {
                        "Analyte": s.name,
                        "Full name": s.full_name,
                        "Category": CATEGORY_LABELS.get(s.category, s.category),
                        "Concentration": format_conc_auto(s.conc_mg_L),
                        "% of total": f"{s.percentage:.1f}",
                        "Primary set": "Yes" if s.in_primary_set else "",
                    }
                    for s in m1.species
                    if s.conc_mg_L > 0 or s.in_primary_set
                ],
                num_cols=("Concentration", "% of total"),
                code_cols=("Analyte",),
            )
            if m1.category_fractions:
                _html_table(_category_rows(m1), num_cols=("Share of total",))
        elif nd_species:
            _note(
                f"All {len(nd_species)} analyte(s) in this sample are below the detection "
                "limit (ND). No concentrations were quantified.",
                "warning",
            )
            _html_table(
                [
                    {
                        "Analyte": name,
                        "Full name": get_pfas_info(name).get("full_name", name),
                        "Category": CATEGORY_LABELS.get(
                            get_pfas_info(name).get("category", "unknown"), "Unclassified"
                        ),
                        "Result": "< MDL (ND)",
                    }
                    for name in nd_species
                ],
                code_cols=("Analyte",),
            )
            st.caption(
                "ND species are excluded from totals (do_not_assume_zero). "
                "No treatment flag can be raised without a quantified concentration."
            )
        else:
            _note("No concentration data for this sample.", "warning")

        # ── TOF / AOF coverage ────────────────────────────────────────────────
        if sr.tof_result is not None:
            tof = sr.tof_result
            cov_pct = tof.coverage_ratio * 100
            if tof.unknown_pfas_flag:
                sev, verdict = "critical", (
                    f"Only {cov_pct:.1f}% of reported {tof.measured_type} is accounted for by "
                    "identified species. A significant unknown PFAS fraction is present, so "
                    "treatment of the unidentified species cannot be guaranteed."
                )
            else:
                sev, verdict = "ok", (
                    f"Identified species account for {cov_pct:.1f}% of reported "
                    f"{tof.measured_type}: good coverage, low unknown-PFAS risk."
                )
            _note(
                f"<strong>TOF/AOF coverage.</strong> Theoretical TOF "
                f"{format_conc_auto(tof.theoretical_tof_mg_L)} as F; reported "
                f"{tof.measured_type} {format_conc_auto(tof.measured_mg_L)} as F; "
                f"coverage {cov_pct:.1f}%.",
                sev,
                detail=verdict,
            )

        _flags_table(m1.flags)

        # ── Average case (steady-state, for stat-summary datasets) ───────────
        if getattr(sr, "avg_module1", None) is not None:
            avg_m1 = sr.avg_module1
            with st.expander(
                f"Average / steady-state profile  ·  Total PFAS {format_conc_auto(avg_m1.total_conc_mg_L)}",
                expanded=False,
            ):
                st.caption(
                    "Steady-state operating concentrations (average). Equipment is sized on the "
                    "maximum (worst case) above; this shows the expected day-to-day loading."
                )
                if avg_m1.species:
                    _html_table(
                        [
                            {
                                "Analyte": s.name,
                                "Full name": s.full_name,
                                "Avg concentration": format_conc_auto(s.conc_mg_L),
                                "% of avg total": f"{s.percentage:.1f}",
                                "Category": CATEGORY_LABELS.get(s.category, s.category),
                            }
                            for s in avg_m1.species
                            if s.conc_mg_L > 0
                        ],
                        num_cols=("Avg concentration", "% of avg total"),
                        code_cols=("Analyte",),
                    )
                    if avg_m1.category_fractions:
                        _html_table(_category_rows(avg_m1), num_cols=("Share of total",))

        # ── Module 2 ──────────────────────────────────────────────────────────
        _subheading("Reactivity screening (Module 2)")
        if not m2.flags:
            st.markdown('<div class="muted">No reactivity flags triggered.</div>',
                        unsafe_allow_html=True)
        else:
            _flags_table(m2.flags)

        if m2.treatment_implications:
            _subheading("Treatment implications")
            st.markdown(
                '<ul class="rpt-list">'
                + "".join(f"<li>{item}</li>" for item in m2.treatment_implications)
                + "</ul>",
                unsafe_allow_html=True,
            )

        if m2.operating_scenarios:
            with st.expander("Operating scenario details"):
                for sc in m2.operating_scenarios:
                    st.markdown(f"- {sc}")


_MATRIX_STATUS = {"🟢": ("ok", "OK"), "🟡": ("warning", "Watch"), "🔴": ("critical", "Concern")}
_MATRIX_SPEC = {
    "COD": "Required", "TOC": "Required", "nitrate": "Required", "NO2": "Required",
    "nitrite": "Required", "UV254": "Recommended", "UVT254": "Recommended",
}


def _sample_labels(samples: list) -> tuple:
    """Column labels for samples: the names when short, else S1, S2 ... with a legend."""
    if len(samples) <= 4 and all(len(s) <= 22 for s in samples):
        return {s: s for s in samples}, ""
    labels = {s: f"S{i}" for i, s in enumerate(samples, 1)}
    legend = "; ".join(f"<strong>{labels[s]}</strong> = {html.escape(s)}" for s in samples)
    return labels, f'<div class="muted" style="margin:4px 0 6px 0;">{legend}</div>'


def _render_module3(
    m3: Module3Result, num: str | None = None, per_sample: dict | None = None
) -> None:
    """Render Module 3 water matrix screening results."""
    _MATRIX_UNITS = {
        "COD": "mg/L", "TOC": "mg/L", "DOC": "mg/L",
        "BOD": "mg/L",
        "nitrate": "mg/L", "NO2": "mg/L", "nitrite": "mg/L",
        "ammonia": "mg/L", "TKN": "mg/L", "TN": "mg/L",
        "TP": "mg/L", "phosphate": "mg/L",
        "UV254": "cm⁻¹", "UVT254": "%",
        "chloride": "mg/L", "fluoride": "mg/L",
        "hardness": "mg/L as CaCO₃", "TDS": "mg/L",
        "sulfate": "mg/L", "alkalinity": "mg/L as CaCO₃",
        "pH": "", "turbidity": "NTU",
        "TSS": "mg/L", "temperature": "°C", "flow_rate": "(raw)",
        "sample_color": "", "conductivity": "mS/cm",
        "iron": "mg/L", "manganese": "mg/L", "copper": "mg/L",
        "zinc": "mg/L", "aluminum": "mg/L", "nickel": "mg/L",
        "chromium": "mg/L", "lead": "mg/L",
        "arsenic": "mg/L", "cadmium": "mg/L", "mercury": "mg/L",
        "silver": "mg/L", "barium": "mg/L", "calcium": "mg/L",
        "magnesium": "mg/L", "boron": "mg/L", "selenium": "mg/L",
    }
    _MATRIX_NICE = {
        "COD":         "COD (chemical oxygen demand)",
        "TOC":         "TOC (total organic carbon)",
        "DOC":         "DOC (dissolved organic carbon)",
        "BOD":         "BOD₅ (biological oxygen demand)",
        "nitrate":     "Nitrate NO₃⁻",
        "NO2":         "Nitrite NO₂⁻",
        "nitrite":     "Nitrite NO₂⁻",
        "ammonia":     "Ammonia / ammonium (NH₃/NH₄⁺)",
        "TKN":         "TKN (total Kjeldahl nitrogen)",
        "TN":          "Total nitrogen",
        "TP":          "Total phosphorus",
        "phosphate":   "Phosphate (PO₄³⁻)",
        "UV254":       "UV₂₅₄ absorbance",
        "UVT254":      "UV₂₅₄ transmittance",
        "chloride":    "Chloride Cl⁻",
        "fluoride":    "Fluoride F⁻",
        "hardness":    "Total hardness",
        "alkalinity":  "Total alkalinity",
        "TDS":         "TDS (total dissolved solids)",
        "sulfate":     "Sulfate SO₄²⁻",
        "pH":          "pH",
        "turbidity":   "Turbidity",
        "TSS":         "TSS (total suspended solids)",
        "temperature": "Temperature",
        "conductivity":"Conductivity",
        "flow_rate":   "Flow rate",
        "sample_color":"Sample color",
        "iron":        "Iron (Fe)",
        "manganese":   "Manganese (Mn)",
        "copper":      "Copper (Cu)",
        "zinc":        "Zinc (Zn)",
        "aluminum":    "Aluminum (Al)",
        "nickel":      "Nickel (Ni)",
        "chromium":    "Chromium (Cr)",
        "lead":        "Lead (Pb)",
        "arsenic":     "Arsenic (As)",
        "cadmium":     "Cadmium (Cd)",
        "mercury":     "Mercury (Hg)",
        "silver":      "Silver (Ag)",
        "barium":      "Barium (Ba)",
        "calcium":     "Calcium (Ca)",
        "magnesium":   "Magnesium (Mg)",
        "boron":       "Boron (B)",
        "selenium":    "Selenium (Se)",
        "sodium":      "Sodium (Na)",
        "perchlorate": "Perchlorate (ClO₄⁻)",
        "ORP":         "ORP",
        "oil_grease":  "Oil & Grease",
    }
    _MATRIX_UNITS.update({
        "sodium": "mg/L", "perchlorate": "mg/L", "ORP": "mV", "oil_grease": "mg/L",
        # Customers report alkalinity as CaCO₃ or as HCO₃⁻; the value is shown as reported.
        "alkalinity": "mg/L (as reported)",
    })

    _heading("Water matrix screening (Module 3)", num)

    def _value(v) -> str:
        return f"{v:.4g}" if isinstance(v, float) and v == v else str(v)

    multi = bool(per_sample) and len(per_sample) > 1
    if m3.detected_params:
        labels, legend = _sample_labels(list(per_sample)) if multi else ({}, "")
        if legend:
            st.markdown(legend, unsafe_allow_html=True)
        rows = []
        for k, v in m3.detected_params.items():
            sev, label = _MATRIX_STATUS.get(_param_traffic_light(k, v), ("none", ""))
            row = {"Parameter": html.escape(_MATRIX_NICE.get(k, k))}
            if multi:
                for name, own in per_sample.items():
                    row[labels[name]] = _value(own[k]) if own.get(k) is not None else "n/r"
                row["Screened on"] = _value(v)
            else:
                row["Value"] = _value(v)
            row["Unit"] = html.escape(_MATRIX_UNITS.get(k, ""))
            row["Screening"] = (
                _sev_html(sev, label) if label else '<span class="muted">No threshold</span>'
            )
            row["Spec"] = _MATRIX_SPEC.get(k, "")
            rows.append(row)
        _html_table(
            rows,
            num_cols=tuple(c for c in rows[0] if c not in ("Parameter", "Unit", "Screening", "Spec")),
            html_cols=("Parameter", "Unit", "Screening"),
        )
        if multi:
            st.caption(
                "Each parameter is screened on its highest value across the samples. "
                "n/r = not reported for that sample."
            )
    else:
        st.markdown(
            '<div class="muted">No water matrix parameters detected. Add them to the notes, '
            'e.g. "COD = 120 mg/L, nitrate = 8 mg/L, hardness = 150 mg/L".</div>',
            unsafe_allow_html=True,
        )

    if m3.missing_required_params:
        _note(
            "<strong>Required matrix parameters missing:</strong> "
            + ", ".join(html.escape(str(p)) for p in m3.missing_required_params),
            "critical",
        )

    _flags_table(m3.flags)

    if m3.missing_params:
        st.caption(f"Not provided: {', '.join(str(p) for p in m3.missing_params)}")


def _build_text_report(result: EvaluationResult, parsed: "ParsedData | None" = None) -> str:
    """Build a plain-text version of the full technical report for download."""
    from datetime import date

    lines = [
        "=" * 70,
        "PFAS MATERIAL EVALUATION ENGINE: TECHNICAL REPORT",
        "Claros Product Development Team  |  Framework Architecture by Zack Liu",
        f"Generated: {date.today().strftime('%B %d, %Y')}",
        "=" * 70,
        "",
        f"OVERALL STATUS: {result.overall_status}",
        "",
        "STATUS REASONS:",
        *[f"  - {r}" for r in result.status_reasons],
        "",
    ]

    if parsed and parsed.source_files:
        lines.append("SOURCE FILES:")
        lines += [f"  - {r['file']} ({r['format']}): {r['read_as']}" for r in parsed.source_files]
        lines.append("")
    if parsed and parsed.data_checks:
        lines.append("DATA CHECKS (confirm against the source files):")
        lines += [f"  - [{c['level'].upper()}] {c['message']}" for c in parsed.data_checks]
        lines.append("")

    if result.comparison is not None:
        comparison = result.comparison
        lines += ["SAMPLE COMPARISON AND BASIS OF DESIGN:", f"  {comparison.headline}", ""]
        short = {name: f"S{i}" for i, name in enumerate(comparison.samples, 1)}
        if comparison.legend:
            short = comparison.labels
        lines += [f"  {short[name]} = {name}" for name in comparison.samples if short[name] != name]
        lines.append("")
        lines.append(f"  {'Parameter':<30}{'Unit':<8}" + "".join(f"{short[n][:11]:>12}" for n in comparison.samples) + f"{'Max/Min':>10}")
        for r in comparison.rows:
            cells = "".join(f"{(r.values.get(n) or 'n/r'):>12}" for n in comparison.samples)
            ratio = f"{r.ratio:.1f}x" if r.ratio is not None else ""
            lines.append(f"  {r.label[:29]:<30}{r.unit:<8}{cells}{ratio:>10}")
        lines.append("")
        lines += [f"  - {t}" for t in comparison.findings]
        lines += [f"  - {t}" for t in comparison.design_notes]
        lines.append("")
    flow_lines = [
        f"  - {name}: {describe_flow(flow)}"
        + (f"; design {design_flow_gpm(flow):,.0f} GPM" if design_flow_gpm(flow) is not None else "")
        + (f"; {flow['hours_avg']:g} h/day" if flow.get("hours_avg") is not None else "")
        for name, flow in result.sample_flow.items()
    ]
    if flow_lines:
        lines += ["FLOW AND OPERATION:", *flow_lines, ""]

    if getattr(result, "analytical_flag", None):
        af = result.analytical_flag
        lines += [
            "ANALYTICAL FEASIBILITY:",
            f"  [{af.severity.upper()}][{af.rule_id}] {af.message}",
            f"    → {af.detail}",
            "",
        ]

    if result.missing_info:
        lines += [
            "MISSING / REQUIRED INFORMATION:",
            *[f"  - {m}" for m in result.missing_info],
            "",
        ]

    for sr in result.samples:
        m1, m2 = sr.module1, sr.module2
        lines += [
            "-" * 70,
            f"SAMPLE: {sr.sample_name}  |  Status: {sr.sample_status}",
            "-" * 70,
            "",
            "MODULE 1: PFAS COMPOSITION ANALYSIS",
            f"  Total PFAS: {format_conc_auto(m1.total_conc_mg_L)}",
            f"  Primary Set (≥5%): {', '.join(str(n) for n in m1.primary_set) if m1.primary_set else 'None'}",
            "",
        ]
        if m1.species:
            lines.append("  SPECIES BREAKDOWN:")
            for s in m1.species:
                if s.conc_mg_L > 0:
                    ps = " [PRIMARY]" if s.in_primary_set else ""
                    lines.append(
                        f"    {s.name:<14} {format_conc_auto(s.conc_mg_L):<22} "
                        f"{format_pct(s.percentage):<8} "
                        f"{CATEGORY_LABELS.get(s.category, s.category)}{ps}"
                    )
        lines.append("")
        lines.append("MODULE 2: REACTIVITY SCREENING")
        for f in m2.flags:
            lines.append(f"  [{f.severity.upper()}][{f.rule_id}] {f.message}")
            if f.detail:
                lines.append(f"    → {f.detail}")
        lines += ["", "  TREATMENT IMPLICATIONS:"]
        lines += [f"    - {item}" for item in m2.treatment_implications]
        lines.append("")
        if sr.tof_result is not None:
            t = sr.tof_result
            lines += [
                "TOF COVERAGE ANALYSIS",
                f"  Theoretical TOF (from identified PFAS): {format_conc_auto(t.theoretical_tof_mg_L)} as F",
                f"  Reported {t.measured_type}: {format_conc_auto(t.measured_mg_L)} as F",
                f"  Coverage ratio: {t.coverage_ratio * 100:.1f}%",
                f"  Unknown PFAS flag: {'YES (significant unknown PFAS likely present)' if t.unknown_pfas_flag else 'No'}",
                "",
            ]

    lines += [
        "-" * 70,
        "MODULE 3: WATER MATRIX SCREENING",
        "-" * 70,
    ]
    if result.module3.detected_params:
        for k, v in result.module3.detected_params.items():
            lines.append(f"  {k}: {v}")
    else:
        lines.append("  No matrix parameters detected.")
    lines.append("")
    for f in result.module3.flags:
        lines.append(f"  [{f.severity.upper()}][{f.rule_id}] {f.message}")
        if f.detail:
            lines.append(f"    → {f.detail}")

    lines += [
        "",
        "-" * 70,
        "TREATMENT TECHNOLOGY GUIDANCE",
        "-" * 70,
        *[f"  - {item}" for item in result.treatment_summary],
        "",
        "=" * 70,
        "PFAS Evaluation Engine v1.0",
        "Claros Product Development Team  |  Framework Architecture by Zack Liu",
        "=" * 70,
    ]
    return "\n".join(lines)


def _render_comparison(comparison) -> None:
    """How far apart the samples are, and what that means for the basis of design."""
    severity = {
        "uniform": "ok", "moderately variable": "warning", "non-uniform": "warning",
    }.get(comparison.uniformity, "info")
    _note(f"<strong>{html.escape(comparison.headline)}</strong>", severity)

    labels = comparison.labels
    if comparison.legend:
        st.markdown(
            f'<div class="muted" style="margin:4px 0 6px 0;">{html.escape(comparison.legend)}</div>',
            unsafe_allow_html=True,
        )
    many = len(comparison.samples) > 6
    for group in ("PFAS", "Water matrix", "Flow and operation"):
        group_rows = [r for r in comparison.rows if r.group == group]
        if not group_rows:
            continue
        _subheading(group)
        table = []
        for r in group_rows:
            row = {"Parameter": r.label, "Unit": r.unit}
            if many:
                row.update({
                    "Lowest": r.low, "Highest": r.high,
                    "Highest in": labels.get(r.high_sample, ""),
                })
            else:
                row.update({labels[name]: r.values.get(name) or "n/r" for name in comparison.samples})
            row["Max / Min"] = f"{r.ratio:.1f}×" if r.ratio is not None else ""
            table.append(row)
        _html_table(
            table,
            num_cols=tuple(c for c in table[0] if c not in ("Parameter", "Unit", "Highest in")),
        )
    st.caption(
        "n/r = not reported, ND = not detected. Max / Min is the highest value divided by "
        "the lowest among the samples that report a number."
    )

    _subheading("Findings")
    st.markdown(
        '<ul class="rpt-list">' + "".join(f"<li>{html.escape(t)}</li>" for t in comparison.findings) + "</ul>",
        unsafe_allow_html=True,
    )
    _subheading("Basis of design")
    st.markdown(
        '<ul class="rpt-list">' + "".join(f"<li>{html.escape(t)}</li>" for t in comparison.design_notes) + "</ul>",
        unsafe_allow_html=True,
    )


def _flow_rows(sample_flow: dict) -> list:
    def number(value) -> str:
        return f"{value:,.4g}" if value is not None else ""

    rows = []
    for name, flow in sample_flow.items():
        design = design_flow_gpm(flow)
        hours = flow.get("hours_avg")
        span = [flow.get("hours_min"), flow.get("hours_max")]
        hours_text = number(hours)
        if span[0] is not None and span[1] is not None:
            hours_text = f"{number(span[0])} to {number(span[1])}" + (f" (avg {number(hours)})" if hours is not None else "")
        volume = flow.get("daily_volume")
        rows.append({
            "Sample": name,
            "Flow min": number(flow.get("min")),
            "Flow avg": number(flow.get("avg")),
            "Flow max": number(flow.get("max")),
            "Unit": flow.get("unit", ""),
            "Design flow (GPM)": f"{design:,.0f}" if design is not None else "",
            "Hours per day": hours_text,
            "Daily volume": f"{number(volume)} {flow.get('daily_volume_unit', '')}".strip() if volume is not None else "",
        })
    return rows


def _render_data_intake(parsed: "ParsedData") -> None:
    """What was read from each file, and every value a person should check by eye."""
    if parsed.source_files:
        _html_table([
            {
                "File": row["file"],
                "Format": row["format"],
                "Read as": row["read_as"],
                **({"Note": row["note"]} if any(r["note"] for r in parsed.source_files) else {}),
            }
            for row in parsed.source_files
        ])
    if parsed.data_checks:
        order = {"critical": 0, "warning": 1}
        rows = sorted(parsed.data_checks, key=lambda c: order.get(c["level"], 2))
        _html_table(
            [
                {
                    "Level": _sev_html(c["level"], "Problem" if c["level"] == "critical" else "Check"),
                    "Item": html.escape(c["message"]),
                }
                for c in rows
            ],
            html_cols=("Level", "Item"),
        )
    elif parsed.source_files:
        st.markdown(
            '<div class="muted">No read problems found. Every text value was located in its '
            "source, and any image values agreed across both reads.</div>"
            if parsed.llm_raw_response else
            '<div class="muted">No read problems found by the standard parser.</div>',
            unsafe_allow_html=True,
        )


def _render_technical_output(result: EvaluationResult, parsed: "ParsedData | None" = None) -> None:
    """Render the full technical report as numbered report sections."""
    sec = iter(range(1, 20))

    # ── 1 Summary ─────────────────────────────────────────────────────────────
    _heading("Summary", str(next(sec)))
    src_str = ", ".join(result.data_sources) if result.data_sources else "n/a"
    st.markdown(
        f'<div class="status-line">Overall status: '
        f'{_sev_html(_STATUS_SEV.get(result.overall_status, "none"), result.overall_status)}'
        f'<span class="muted" style="margin-left:18px;">Sources: {html.escape(src_str)}</span></div>',
        unsafe_allow_html=True,
    )
    if result.status_reasons:
        st.markdown(
            '<ol class="rpt-list">'
            + "".join(f"<li>{r}</li>" for r in result.status_reasons)
            + "</ol>",
            unsafe_allow_html=True,
        )

    if result.variability_flag:
        vf = result.variability_flag
        _note(
            f'<span class="mono">{html.escape(str(vf.rule_id if vf.severity == "info" else "M1-VAR"))}</span> '
            f"{vf.message}",
            "info" if vf.severity == "info" else "warning",
            detail=vf.detail or "",
        )
    elif result.variability_ratio is not None and result.comparison is None:
        st.caption(
            f"Multi-sample variability ratio: {result.variability_ratio:.1f} (within acceptable range)"
        )

    # ── What was read ─────────────────────────────────────────────────────────
    if parsed and (parsed.source_files or parsed.data_checks):
        _heading("Source files and data checks", str(next(sec)))
        _render_data_intake(parsed)

    # ── Several samples: how uniform they are ─────────────────────────────────
    if result.comparison is not None:
        _heading("Sample comparison and basis of design", str(next(sec)))
        _render_comparison(result.comparison)

    # ── Analytical feasibility (AN-1) ─────────────────────────────────────────
    if getattr(result, "analytical_flag", None):
        _heading("Analytical feasibility", str(next(sec)))
        _flags_table([result.analytical_flag])

    # ── Project context and flow ──────────────────────────────────────────────
    ctx = parsed.llm_project_context if parsed else {}
    pairs: list[tuple[str, str]] = []
    for key, label in (("customer_name", "Customer"), ("site_name", "Site"), ("country", "Country")):
        if ctx.get(key):
            pairs.append((label, str(ctx[key])))
    gpm = ctx.get("throughput_gpm")
    if ctx.get("flow_rate_display"):
        pairs.append(("Project flow", ctx["flow_rate_display"] + (f" ({gpm:.0f} GPM)" if gpm else "")))
    elif gpm:
        pairs.append(("Project flow", f"{gpm:.0f} GPM"))
    flow_rows = _flow_rows(result.sample_flow)
    if pairs or flow_rows:
        _heading("Project context and flow", str(next(sec)))
        if pairs:
            _html_table([{"Item": k, "Value": v} for k, v in pairs])
        if flow_rows:
            _html_table(
                flow_rows,
                num_cols=("Flow min", "Flow avg", "Flow max", "Design flow (GPM)"),
            )
            st.caption("Design flow is the maximum where one is reported, otherwise the average.")
        if gpm and float(gpm) > 100:
            _note(
                f"<strong>Large-scale application.</strong> {float(gpm):.0f} GPM exceeds the "
                "100 GPM threshold. Full commercial and engineering review is required before quoting.",
                "warning",
            )

    # ── Missing information ───────────────────────────────────────────────────
    if result.missing_info:
        _heading("Missing information", str(next(sec)))
        st.markdown(
            '<div class="muted">Not found in the uploaded material:</div><ul class="rpt-list">'
            + "".join(f"<li>{item}</li>" for item in result.missing_info)
            + "</ul>",
            unsafe_allow_html=True,
        )

    # ── Per-sample results ────────────────────────────────────────────────────
    n_samples = len(result.samples)
    if n_samples:
        _heading("PFAS results by sample" if n_samples > 1 else "PFAS results", str(next(sec)))
    nd_lookup = parsed.nd_species if parsed else {}
    for i, sr in enumerate(result.samples):
        _render_sample_section(
            sr,
            expanded=(n_samples == 1 or i == 0),
            nd_species=nd_lookup.get(sr.sample_name),
        )

    # ── Water matrix ──────────────────────────────────────────────────────────
    _render_module3(
        result.module3, str(next(sec)),
        per_sample=sample_matrices(parsed) if parsed else None,
    )

    # ── Treatment guidance ────────────────────────────────────────────────────
    if result.treatment_summary:
        _heading("Treatment technology guidance", str(next(sec)))
        st.markdown(
            '<ul class="rpt-list">'
            + "".join(f"<li>{item}</li>" for item in result.treatment_summary)
            + "</ul>",
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    st.download_button(
        "Download technical report (.txt)",
        data=_build_text_report(result, parsed),
        file_name="PFAS_Evaluation_Report.txt",
        mime="text/plain",
    )


def _render_email_draft(result: EvaluationResult, email_text_override: str | None = None) -> None:
    """Render the business email draft tab."""
    email_text = email_text_override if email_text_override is not None else result.email_draft

    _heading("Internal email draft")
    st.caption("Generated from the evaluation output. Review and edit before sending.")
    st.markdown(
        f'<div class="email-box">{email_text}</div>',
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "Download email draft (.txt)",
            data=email_text,
            file_name="PFAS_Email_Draft.txt",
            mime="text/plain",
        )
    with col2:
        with st.expander("Select text to copy"):
            st.text_area(
                "Email text:",
                value=email_text,
                height=300,
                label_visibility="collapsed",
            )


def _render_debug_logs(result: EvaluationResult, parsed: ParsedData | None) -> None:
    """Render parser and engine diagnostic logs."""
    import pandas as pd

    _heading("Parser and engine trace")

    if result.logs:
        st.code("\n".join(str(x) for x in result.logs), language="text")

    if parsed:
        # ── LLM-specific debug info ───────────────────────────────────────────
        if parsed.llm_parse_notes:
            st.markdown("**LLM Parse Notes:**")
            for note in parsed.llm_parse_notes:
                st.info(note)

        if parsed.sample_metadata:
            st.markdown("**LLM Sample Metadata:**")
            meta_rows = [
                {
                    "Sample": name,
                    "Statistical Summary": "Yes" if m.is_statistical_summary else "No",
                    "Type": m.summary_type or "n/a",
                }
                for name, m in parsed.sample_metadata.items()
            ]
            st.dataframe(pd.DataFrame(meta_rows), hide_index=True, use_container_width=True)

        if parsed.llm_project_context:
            st.markdown("**LLM Project Context:**")
            ctx_rows = [{"Field": k, "Value": str(v)} for k, v in parsed.llm_project_context.items()]
            st.dataframe(pd.DataFrame(ctx_rows), hide_index=True, use_container_width=True)

        if parsed.llm_raw_response:
            with st.expander("LLM Raw Response (JSON)", expanded=False):
                st.code(parsed.llm_raw_response, language="json")

        # ── Standard debug info ───────────────────────────────────────────────
        if parsed.warnings:
            st.markdown("**Parser Warnings:**")
            for w in parsed.warnings:
                st.warning(w)
        else:
            st.success("No parser warnings.")

        if parsed.errors:
            st.markdown("**Parser Errors:**")
            for e in parsed.errors:
                st.error(e)

        _screened, _origin = screening_matrix(parsed)
        if _screened:
            st.markdown("**Matrix parameters used for screening:**")
            st.dataframe(
                pd.DataFrame([
                    {"Parameter": k, "Value": v, "From sample": _origin.get(k, "(not tied to a sample)")}
                    for k, v in _screened.items()
                ]),
                hide_index=True,
            )

        if parsed.nd_species:
            st.markdown("**Non-detect (ND) species (analyzed but below detection limit):**")
            for sample, nd_list in parsed.nd_species.items():
                st.info(f"Sample '{sample}': {', '.join(str(s) for s in nd_list)}")

        if parsed.keyword_species:
            st.markdown("**Keyword Species Detected (from text, no concentration):**")
            st.code(", ".join(str(s) for s in parsed.keyword_species))


_SCHEDULE_LABELS = {
    "short":      "Short (≤ 2 h)",
    "long":       "Long (≤ 24 h)",
    "long_dense": "Long + dense first 2 h",
}
_SCHEDULE_TIMEPOINTS = {
    "short":      SHORT_TIMEPOINTS,
    "long":       LONG_TIMEPOINTS,
    "long_dense": LONG_DENSE_TIMEPOINTS,
}


def _render_editable_plan(plan: "TestPlan", idx: int) -> None:
    """Render one bench test plan as editable fields; return nothing (state via widget keys)."""
    k = f"plan{idx}"
    _heading(f"Bench test plan: {html.escape(str(plan.sample_name))}", str(idx + 1))
    st.caption(
        f"Total PFAS {format_conc_auto(plan.total_pfas_mg_L)}  ·  "
        "750 mL Amazon Reactor  ·  Proof of concept. All fields are editable."
    )

    # ── Reaction conditions ──────────────────────────────────────────────────
    _subheading("Reaction conditions")
    cc1, cc2 = st.columns(2)
    with cc1:
        st.markdown(f"*{plan.conditions[0].label}*")
        st.number_input("Sulfite, reagent A (mM)", min_value=0.0, step=1.0,
                        value=float(plan.conditions[0].sulfite_mM), key=f"{k}_sA")
        st.number_input("Iodide, reagent B (mM)", min_value=0.0, step=0.5,
                        value=float(plan.conditions[0].iodide_mM), key=f"{k}_iA")
    with cc2:
        if len(plan.conditions) > 1:
            st.markdown(f"*{plan.conditions[1].label}*")
            st.number_input("Sulfite, reagent A (mM)", min_value=0.0, step=1.0,
                            value=float(plan.conditions[1].sulfite_mM), key=f"{k}_sB")
            st.caption(plan.conditions[1].note)   # sulfite dose rationale — under sulfite
            st.number_input("Iodide, reagent B (mM)", min_value=0.0, step=0.5,
                            value=float(plan.conditions[1].iodide_mM), key=f"{k}_iB")
        else:
            st.markdown("*Water-adjusted condition*")
            st.caption("Equals the reference (no adjustment needed for this water).")

    # ── Sampling schedule ────────────────────────────────────────────────────
    _subheading("Sampling schedule")
    sched_keys = list(_SCHEDULE_LABELS.keys())
    sel = st.selectbox(
        "Schedule", options=sched_keys,
        index=sched_keys.index(plan.schedule_type),
        format_func=lambda s: _SCHEDULE_LABELS[s],
        key=f"{k}_sched", label_visibility="collapsed",
    )
    st.caption("Timepoints: " + ", ".join(_SCHEDULE_TIMEPOINTS[sel]))
    st.caption(plan.schedule_rationale)

    # ── Pretreatment / dilution ──────────────────────────────────────────────
    _subheading("Pretreatment")
    st.text_area("Pretreatment", value="\n".join(plan.pretreatment),
                 key=f"{k}_pre", label_visibility="collapsed", height=90)
    st.text_input("Dilution", value=plan.dilution, key=f"{k}_dil")

    # ── pH ───────────────────────────────────────────────────────────────────
    _subheading("pH")
    st.number_input("Reaction pH setpoint", min_value=0.0, max_value=14.0, step=0.5,
                    value=float(plan.reaction_ph), key=f"{k}_ph")
    st.caption(plan.ph_monitoring)

    # ── External TOF ─────────────────────────────────────────────────────────
    _subheading("External TOF (Novem)")
    tof_val = plan.external_tof + (("\n" + plan.fluoride_handling) if plan.fluoride_handling else "")
    st.text_area("External TOF", value=tof_val, key=f"{k}_tof",
                 label_visibility="collapsed", height=90)

    # ── Notes ────────────────────────────────────────────────────────────────
    if plan.customer_info_requests:
        st.warning("**Confirm with customer:** " + " | ".join(plan.customer_info_requests))
    if plan.optimization_notes:
        st.caption("Optimization (later stage): " + " | ".join(plan.optimization_notes))

    # ── Download (reads current edited widget values) ────────────────────────
    st.download_button(
        "Download this plan (.txt)",
        data=_plan_export_from_state(plan, idx),
        file_name=f"BenchTestPlan_{plan.sample_name}.txt".replace(" ", "_"),
        mime="text/plain",
        key=f"{k}_dl",
    )
    st.markdown("---")


def _plan_export_from_state(plan: "TestPlan", idx: int) -> str:
    """Build export text from the current (possibly edited) widget state."""
    k = f"plan{idx}"
    ss = st.session_state
    sched = ss.get(f"{k}_sched", plan.schedule_type)
    lines = [
        "=" * 66,
        f"BENCH TEST PLAN: {plan.sample_name}",
        "750 mL Amazon Reactor  |  Proof-of-Concept stage",
        "=" * 66,
        f"Total PFAS: {format_conc_auto(plan.total_pfas_mg_L)}",
        "",
        "REACTION CONDITIONS",
        f"  {plan.conditions[0].label}: {ss.get(f'{k}_sA', plan.conditions[0].sulfite_mM):g} mM sulfite "
        f"+ {ss.get(f'{k}_iA', plan.conditions[0].iodide_mM):g} mM iodide",
    ]
    if len(plan.conditions) > 1:
        lines.append(
            f"  {plan.conditions[1].label}: {ss.get(f'{k}_sB', plan.conditions[1].sulfite_mM):g} mM sulfite "
            f"+ {ss.get(f'{k}_iB', plan.conditions[1].iodide_mM):g} mM iodide"
        )
    lines += [
        "",
        f"SAMPLING SCHEDULE: {_SCHEDULE_LABELS.get(sched, sched)}",
        f"  Timepoints: {', '.join(_SCHEDULE_TIMEPOINTS.get(sched, plan.schedule_timepoints))}",
        "",
        "PRETREATMENT",
        f"{ss.get(f'{k}_pre', chr(10).join(plan.pretreatment))}",
        "",
        f"DILUTION: {ss.get(f'{k}_dil', plan.dilution)}",
        "",
        f"pH: setpoint {ss.get(f'{k}_ph', plan.reaction_ph):g}. {plan.ph_monitoring}",
        "",
        "EXTERNAL TOF (Novem)",
        f"{ss.get(f'{k}_tof', plan.external_tof)}",
    ]
    if plan.optimization_notes:
        lines += ["", "OPTIMIZATION (later stage)", *[f"  - {n}" for n in plan.optimization_notes]]
    if plan.customer_info_requests:
        lines += ["", "CONFIRM WITH CUSTOMER", *[f"  - {n}" for n in plan.customer_info_requests]]
    lines += ["", "=" * 66, "Claros Product Development Team  |  Bench Test Plan (POC)", "=" * 66]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# PROGRESS INDICATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _render_progress_circle(pct: int) -> str:
    """Thin progress bar with a plain status line."""
    pct = max(0, min(100, pct))
    return (
        '<div class="prog">'
        f'Reading inputs and running the evaluation: {pct}%'
        f'<div class="prog-bar"><div style="width:{pct}%"></div></div>'
        '</div>'
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═══════════════════════════════════════════════════════════════════════════════

if "eval_result" not in st.session_state:
    st.session_state.eval_result = None
if "parsed_data" not in st.session_state:
    st.session_state.parsed_data = None
if "llm_email" not in st.session_state:
    st.session_state.llm_email = None
if "test_plans" not in st.session_state:
    st.session_state.test_plans = None

result: EvaluationResult | None = st.session_state.eval_result

# ═══════════════════════════════════════════════════════════════════════════════
# HEADER BANNER — full-width
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <div class="rpt-header">
        <div>
            <div class="org">Claros Product Development</div>
            <h1>PFAS Material Evaluation Engine</h1>
            <div class="sub">Preliminary treatment feasibility screening</div>
        </div>
        <div class="meta">Lead framework: Zack Liu<br>Internal Product Development use only · v1.0</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ═══════════════════════════════════════════════════════════════════════════════
# INPUT ROW — horizontal single row
# ═══════════════════════════════════════════════════════════════════════════════

inp_a, inp_c, inp_d = st.columns([1.2, 1.4, 1.4], gap="medium")

with inp_a:
    st.markdown('<div class="field-label">Lab and water-quality data</div>', unsafe_allow_html=True)
    lab_files = st.file_uploader(
        "Lab data",
        accept_multiple_files=True,
        key="lab_files",
        help=(
            f"{ACCEPTED_FORMATS_LABEL}. PFAS results and general water chemistry can be in "
            "separate files. Photos and scans need Enhanced document reading."
        ),
        label_visibility="collapsed",
    )
    st.markdown(
        f'<div class="field-sublabel">{ACCEPTED_FORMATS_LABEL}. Any file can be added; '
        "the report lists anything that could not be read.</div>",
        unsafe_allow_html=True,
    )

with inp_c:
    st.markdown('<div class="field-label">Customer email / notes</div>', unsafe_allow_html=True)
    email_text = st.text_area(
        "Notes",
        height=110,
        placeholder=(
            "PFOA 250 ng/L, PFOS 180 ng/L, TFA detected...\n"
            "DOC = 4.5 mg/L, sulfate = 180 mg/L, pH = 7.4"
        ),
        help="Scanned for PFAS species, inline concentrations, and matrix parameters.",
        label_visibility="collapsed",
    )
    st.markdown('<div class="field-sublabel">Email files</div>', unsafe_allow_html=True)
    notes_pdfs = st.file_uploader(
        "Email files",
        accept_multiple_files=True,
        key="notes_pdfs",
        help=(
            "Email threads as .msg, .eml or PDF, or any other document. Their text is read "
            "with the notes above. Attachments holding results are read as lab data."
        ),
        label_visibility="collapsed",
    )

with inp_d:
    st.markdown('<div class="field-label">Treatment goals / context</div>', unsafe_allow_html=True)
    goals_text = st.text_area(
        "Goals",
        height=110,
        placeholder=(
            "Target <70 ng/L PFOA+PFOS per EPA MCL.\n"
            "Drinking water. Flow 2 MGD. Customer: Veolia France."
        ),
        help=(
            "Treatment objectives, regulatory targets, flow rate, site context. "
            "Merged with any goals found in uploaded documents when AI parsing is on."
        ),
        label_visibility="collapsed",
    )
    st.markdown('<div class="field-sublabel">Context files</div>', unsafe_allow_html=True)
    goals_pdfs = st.file_uploader(
        "Context files",
        accept_multiple_files=True,
        key="goals_pdfs",
        help="Specs, RFQs or email threads describing treatment goals, flow rate or site context.",
        label_visibility="collapsed",
    )

_any_input = bool(
    lab_files or notes_pdfs or goals_pdfs
    or email_text.strip() or goals_text.strip()
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONTROLS ROW — inputs summary + reading mode + run button
# ═══════════════════════════════════════════════════════════════════════════════

ctrl_a, ctrl_b = st.columns([3, 1], gap="medium", vertical_alignment="center")

_api_key = _get_api_key()

with ctrl_a:
    _inputs: list[str] = []
    if lab_files:
        _inputs.append(f"{len(lab_files)} data file(s)")
    if email_text.strip() or notes_pdfs:
        _inputs.append("customer notes")
    if goals_text.strip() or goals_pdfs:
        _inputs.append("treatment goals")
    st.markdown(
        '<div class="muted">Inputs: '
        + (html.escape(", ".join(_inputs)) if _inputs else "none yet. Add a file or paste text above.")
        + "</div>",
        unsafe_allow_html=True,
    )
    if _api_key:
        use_llm = st.toggle(
            "Enhanced document reading",
            value=True,
            help=(
                "Reads uploaded files with Claude: flexible layouts, multilingual labels, "
                "statistical-summary detection and large CofA files with 50+ analytes. "
                "Falls back to standard parsing automatically if unavailable."
            ),
        )
    else:
        use_llm = False
        st.markdown(
            '<div class="muted">Enhanced document reading is off (no API key configured); '
            "standard parsing is used.</div>",
            unsafe_allow_html=True,
        )

with ctrl_b:
    run_clicked = st.button("Run evaluation", type="primary", key="run_btn")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# LOADING INDICATOR (shown while evaluation runs)
# ═══════════════════════════════════════════════════════════════════════════════

loading_placeholder = st.empty()

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT TABS
# ═══════════════════════════════════════════════════════════════════════════════

tab_tech, tab_email, tab_plan, tab_debug = st.tabs([
    "Technical report",
    "Email draft",
    "Bench test plan",
    "Logs",
])

# ─── Handle Run button ───────────────────────────────────────────────────────
if run_clicked:
    if not _any_input:
        with tab_tech:
            st.warning("Please provide at least one input source before running.")
    else:
        import threading as _threading
        import time as _time

        # Read file bytes in main thread (UploadedFile is not thread-safe)
        _lab_uploads   = [(f.getvalue(), f.name) for f in lab_files]
        _notes_uploads = [(f.getvalue(), f.name) for f in notes_pdfs]
        _goals_uploads = [(f.getvalue(), f.name) for f in goals_pdfs]

        # Shared state between main and background threads
        _shared: dict = {"parsed": None, "eval_result": None, "error": None, "done": False}

        def _read_context(typed: str, uploads: list) -> tuple:
            """Typed text plus uploaded context files; results inside them count as lab data."""
            docs = [d for _bytes, _name in uploads for d in read_upload(_bytes, _name)]
            attached, lab_docs = split_context_documents(docs)
            return "\n\n".join(t for t in (typed.strip(), attached) if t), lab_docs

        def _background_work() -> None:
            """Run parse + evaluate in background so main thread can animate progress."""
            try:
                _documents: list = []
                _email_bodies: list = []
                for _bytes, _name in _lab_uploads:
                    for _doc in read_upload(_bytes, _name):
                        if _doc.is_correspondence:
                            _email_bodies.append(_doc)
                        else:
                            _documents.append(_doc)
                _notes_full, _notes_lab = _read_context(email_text, _notes_uploads)
                _goals_full, _goals_lab = _read_context(goals_text, _goals_uploads)
                _documents += _notes_lab + _goals_lab
                if _email_bodies:
                    _body_text, _body_lab = split_context_documents(_email_bodies)
                    _notes_full = "\n\n".join(t for t in (_notes_full, _body_text) if t)
                    _documents += _body_lab

                if use_llm and _api_key:
                    from llm_parser import parse_with_llm
                    _p = parse_with_llm(
                        _documents,
                        goals_text=_goals_full,
                        api_key=_api_key,
                        notes_text=_notes_full,
                    )
                    if _notes_full:
                        from parser import parse_text
                        _t = parse_text(_notes_full, "")
                        # The LLM already read the notes; keep the regex pass for
                        # matrix params / keywords but don't duplicate PFAS samples.
                        if _p.has_pfas_data:
                            _t.pfas_samples = {}
                        _p.merge(_t)
                        _p.customer_notes_text = _notes_full
                else:
                    _p = parse_all(_documents, email_text=_notes_full, goals_text=_goals_full)
                _shared["parsed"]      = _p
                _shared["eval_result"] = evaluate(_p)
            except Exception as _exc:
                _shared["error"] = _exc
            finally:
                _shared["done"] = True

        # Show initial state and start background thread
        loading_placeholder.markdown(_render_progress_circle(1), unsafe_allow_html=True)
        _thread = _threading.Thread(target=_background_work, daemon=True)
        _thread.start()

        # ── Smooth progress animation (main thread) ───────────────────────
        # Increment ~0.9 % every 0.3 s → reaches ~88 % in ≈ 29 s.
        # Caps at 88 % until the background thread finishes, then snaps to 100 %.
        _pct       = 1.0
        _step      = 0.9          # % per tick
        _interval  = 0.3          # seconds between ticks
        _cap       = 88.0         # simulated ceiling while waiting

        while not _shared["done"]:
            _pct = min(_cap, _pct + _step)
            loading_placeholder.markdown(
                _render_progress_circle(int(_pct)), unsafe_allow_html=True
            )
            _time.sleep(_interval)

        # Background work finished — handle result or error
        if _shared["error"]:
            loading_placeholder.empty()
            import traceback as _tb
            st.error(f"Evaluation error: {_shared['error']}")
            st.code(_tb.format_exc(), language="text")
        else:
            # Snap to 100 % then rerun
            loading_placeholder.markdown(
                _render_progress_circle(100), unsafe_allow_html=True
            )
            st.session_state.eval_result = _shared["eval_result"]
            st.session_state.parsed_data = _shared["parsed"]
            st.session_state.llm_email   = None   # generated lazily in email tab
            st.session_state.test_plans  = None   # regenerate on demand for new data
            st.rerun()

# Re-bind after potential rerun
result = st.session_state.eval_result

# ── Tab 1: Technical Output ───────────────────────────────────────────────────
with tab_tech:
    if result is None:
        st.markdown(
            '<div class="empty-state">'
            'No results yet. Run an evaluation to produce the technical report.'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        _render_technical_output(result, st.session_state.parsed_data)

# ── Tab 2: Email Draft — generated lazily here so Technical Output is visible first
with tab_email:
    if result is None:
        st.markdown(
            '<div class="empty-state">'
            'The email draft is written after an evaluation has been run.'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        llm_email = st.session_state.get("llm_email")
        _parsed_data = st.session_state.get("parsed_data")
        if _parsed_data and _parsed_data.data_checks:
            st.markdown(
                f'<div class="note note-warning">{len(_parsed_data.data_checks)} data check(s) '
                "are open. Review them in the technical report before sending this draft.</div>",
                unsafe_allow_html=True,
            )

        # Generate email now (user is already looking at Technical Output tab)
        if llm_email is None and use_llm and _api_key and _parsed_data:
            _email_ph = st.empty()
            _email_ph.markdown(
                '<div class="muted">Writing the email draft…</div>',
                unsafe_allow_html=True,
            )
            try:
                llm_email = _generate_llm_email(
                    result,
                    _parsed_data.llm_project_context,
                    _api_key,
                )
                st.session_state.llm_email = llm_email
            except Exception:
                llm_email = None
                st.session_state.llm_email = None
            _email_ph.empty()

        if llm_email:
            email_version = st.radio(
                "Email version:",
                ["Enhanced Draft", "Template-Generated"],
                horizontal=True,
                label_visibility="collapsed",
            )
            chosen_email = llm_email if "Enhanced" in email_version else None
            _render_email_draft(result, email_text_override=chosen_email)
        else:
            _render_email_draft(result)

# ── Tab 3: Bench Test Plan ────────────────────────────────────────────────────
with tab_plan:
    if result is None:
        st.markdown(
            '<div class="empty-state">'
            'Run an evaluation first. Generate the bench test plan once the physical '
            'water samples have arrived.'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        st.caption(
            "Bench-scale proof-of-concept plan per sample. Generate it once the samples "
            "have physically arrived; every recommended value is editable before export."
        )
        if st.button("Generate bench test plan", key="gen_plan_btn"):
            st.session_state.test_plans = generate_test_plans(
                result, st.session_state.parsed_data
            )

        _plans = st.session_state.get("test_plans")
        if _plans:
            for _i, _plan in enumerate(_plans):
                _render_editable_plan(_plan, _i)
        else:
            st.markdown(
                '<div class="muted">Builds a per-sample test protocol from the evaluation results.</div>',
                unsafe_allow_html=True,
            )

# ── Tab 4: Debug / Logs ───────────────────────────────────────────────────────
with tab_debug:
    if result is None:
        st.markdown(
            '<div class="empty-state">'
            'Parser and engine logs appear here after a run.'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        _render_debug_logs(result, st.session_state.parsed_data)


# ═══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <div class="rpt-footer">
        PFAS Material Evaluation Engine v1.0 · Claros Product Development Team · Framework architecture by Zack Liu.
        For internal screening only. Not a substitute for detailed engineering design.
    </div>
    """,
    unsafe_allow_html=True,
)
