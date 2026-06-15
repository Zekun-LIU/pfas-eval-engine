"""
app.py — PFAS Material Evaluation Engine
Streamlit web application entry point.

Claros R&D Team | Framework Architecture by Zack Liu
"""

from __future__ import annotations

import sys
import os

# Ensure local modules are importable regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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
from parser import ParsedData, parse_all
from utils import (
    CATEGORY_LABELS,
    format_conc_auto,
    format_pct,
    severity_badge,
    status_badge_html,
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
    Use Claude Sonnet to write a professional business email from evaluation results.
    Falls back to the template draft if the API call fails.
    """
    try:
        import anthropic
    except ImportError:
        return result.email_draft

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
            key=lambda sr: {"CRITICAL": 2, "CONDITIONAL": 1, "PROCEED": 0}.get(sr.sample_status, 0),
        )
    if worst and worst.module1.total_conc_mg_L > 0:
        m1 = worst.module1
        lines.append(f"\nPFAS PROFILE — WORST CASE ({worst.sample_name}):")
        lines.append(f"  Total PFAS: {format_conc_auto(m1.total_conc_mg_L)}")
        for s in m1.species[:6]:
            if s.detected:
                lines.append(f"  {s.name}: {format_conc_auto(s.conc_mg_L)} ({s.percentage:.1f}%)")
        for cat, frac in sorted(m1.category_fractions.items(), key=lambda x: -x[1]):
            if frac > 0.01:
                lines.append(f"  Category — {CATEGORY_LABELS.get(cat, cat)}: {format_pct(frac * 100)}")
        # TOF coverage
        if worst.tof_result is not None:
            tof = worst.tof_result
            lines.append(
                f"  TOF Coverage: {tof.coverage_ratio * 100:.1f}% "
                f"(theoretical {format_conc_auto(tof.theoretical_tof_mg_L)} vs "
                f"reported {tof.measured_type} {format_conc_auto(tof.measured_mg_L)})"
            )
            if tof.unknown_pfas_flag:
                lines.append("  ⚠️ LOW TOF COVERAGE — significant unknown/unidentified PFAS present")

        # Average case if available
        if worst.avg_module1 is not None:
            avg_m1 = worst.avg_module1
            lines.append(f"\nPFAS PROFILE — AVERAGE / STEADY-STATE:")
            lines.append(f"  Total PFAS: {format_conc_auto(avg_m1.total_conc_mg_L)}")
            for s in avg_m1.species[:4]:
                if s.detected:
                    lines.append(f"  {s.name}: {format_conc_auto(s.conc_mg_L)}")

    # Key flags — include all severities for Sonnet to reason over
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
        "Subject: PFAS Treatment Feasibility — [customer/site descriptor + status]\n\n"
        "[Opening — 2 sentences: opportunity context, overall verdict]\n\n"
        "[PFAS Profile — 3 sentences: key species with actual concentrations, composition "
        "category, TOF coverage finding if relevant]\n\n"
        "[Key Technical Findings — 4-6 bullet points drawn from the evaluation flags]\n\n"
        "[Scale & Project Context — 1 short paragraph: throughput, any large-scale flag, "
        "site/country context]\n\n"
        "[Recommendation — 1 paragraph: PROCEED / PROCEED WITH CONDITIONS / DO NOT PROCEED "
        "with specific reasoning]\n\n"
        "[Next Steps — 3 concrete action items]\n\n"
        "Claros R&D Team | PFAS Evaluation Engine\n\n"
        "RULES:\n"
        "- Use actual species names and concentrations from the data — never invent numbers.\n"
        "- If CRITICAL flag exists, lead with it in the opening.\n"
        "- If TOF coverage is low (<50%), name it as a key risk: unknown PFAS cannot be "
        "guaranteed to be treated.\n"
        "- If throughput >100 GPM, call out the large-scale flag explicitly.\n"
        "- Return ONLY the email text — no markdown fencing, no preamble."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1500,
            system=system,
            messages=[{"role": "user", "content": f"EVALUATION RESULTS:\n{summary}"}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        return f"[AI email generation failed: {exc}]\n\n{result.email_draft}"

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CSS
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* ── Global typography & background ──────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background-color: #FFFFFF;
        color: #1D1D1F;
        line-height: 1.65;
    }

    /* ── Page chrome ──────────────────────────────────────────── */
    .block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1400px; }
    section[data-testid="stSidebar"] { display: none; }

    /* ── Header — flat full-width banner ──────────────────────── */
    .pfas-header {
        background: #1D1D1F;
        color: white;
        padding: 30px 36px 24px 36px;
        border-radius: 12px;
        margin-bottom: 24px;
    }
    .pfas-header h1 {
        margin: 0; font-size: 1.65rem; font-weight: 700;
        color: white; letter-spacing: -0.5px; line-height: 1.2;
    }
    .pfas-header .subtitle {
        font-size: 0.9rem; color: rgba(255,255,255,0.6); margin-top: 6px; font-weight: 400;
    }
    .pfas-header .byline {
        font-size: 0.75rem; color: rgba(255,255,255,0.35); margin-top: 4px;
    }

    /* ── Section headers ──────────────────────────────────────── */
    .section-header {
        font-size: 0.72rem; font-weight: 600; letter-spacing: 1.8px;
        text-transform: uppercase; color: #6E6E73;
        margin: 28px 0 10px 0;
        padding-bottom: 7px;
        border-bottom: 1px solid #F0F0F2;
    }

    /* ── Flag rows ────────────────────────────────────────────── */
    .flag-row {
        padding: 13px 18px; border-radius: 10px;
        margin-bottom: 10px; font-size: 0.86rem; line-height: 1.65;
        color: #1D1D1F !important;
    }
    .flag-critical        { background: #FFF1F2; border-left: 4px solid #FF3B30; }
    .flag-warning         { background: #FFFBEB; border-left: 4px solid #FF9500; }
    .flag-info            { background: #F0F5FF; border-left: 4px solid #007AFF; }
    .flag-ok              { background: #F0FFF5; border-left: 4px solid #34C759; }
    .flag-commercial      { background: #F5F0FF; border-left: 4px solid #AF52DE; }
    .flag-technical       { background: #FFF8F0; border-left: 4px solid #FF6B00; }
    .flag-pathway         { background: #F0FFF9; border-left: 4px solid #30D158; }
    .flag-special_handling{ background: #F8F8F9; border-left: 4px solid #8E8E93; }
    .flag-row strong      { color: #1D1D1F !important; }
    .flag-detail          { font-size: 0.80rem; color: #6E6E73 !important; margin-top: 6px; font-style: italic; line-height: 1.55; }

    /* ── Variability banner ───────────────────────────────────── */
    .variability-banner {
        background: #FFF8EC; border: 1px solid #FFCC00; border-radius: 10px;
        padding: 12px 18px; font-size: 0.86rem; color: #5C3D00 !important; margin: 10px 0;
    }

    /* ── Metric cards — flat with hover ──────────────────────── */
    .metric-card {
        background: #F5F5F7;
        border: 1px solid rgba(0,0,0,0.06);
        border-radius: 14px; padding: 18px 18px; text-align: center;
        transition: border-color 0.15s ease, background 0.15s ease;
    }
    .metric-card:hover {
        border-color: rgba(0,122,255,0.22);
        background: #EEF4FF;
    }
    .metric-card .val {
        font-size: 1.45rem; font-weight: 700; color: #1D1D1F;
        letter-spacing: -0.5px;
    }
    .metric-card .lbl {
        font-size: 0.68rem; color: #6E6E73; margin-top: 5px;
        text-transform: uppercase; letter-spacing: 0.5px; line-height: 1.4;
    }

    /* ── Email box — flat ─────────────────────────────────────── */
    .email-box {
        background: #F5F5F7;
        border: 1px solid rgba(0,0,0,0.07);
        border-radius: 14px;
        padding: 24px 28px;
        font-family: 'Inter', sans-serif;
        font-size: 0.88rem;
        white-space: pre-wrap;
        line-height: 1.85;
        color: #1D1D1F !important;
    }

    /* ── Input panel ──────────────────────────────────────────── */
    .input-card {
        background: #F5F5F7; border: 1px solid rgba(0,0,0,0.06);
        border-radius: 12px; padding: 16px 18px; margin-bottom: 12px;
    }
    .input-label {
        font-size: 0.72rem; font-weight: 600; letter-spacing: 0.6px;
        text-transform: uppercase; color: #6E6E73; margin-bottom: 8px;
    }

    /* ── Run button — flat ────────────────────────────────────── */
    div[data-testid="stButton"] > button {
        width: 100%;
        background: #007AFF;
        color: white !important; font-weight: 600; font-size: 0.95rem;
        padding: 13px; border-radius: 10px; border: none;
        letter-spacing: 0.2px;
        transition: background 0.15s ease;
        cursor: pointer;
    }
    div[data-testid="stButton"] > button:hover { background: #0066DD; }

    /* ── Download buttons — ghost style ──────────────────────── */
    [data-testid="stDownloadButton"] > button {
        background: transparent !important;
        border: 1px solid #D1D1D6 !important;
        color: #1D1D1F !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        border-radius: 8px !important;
        transition: background 0.15s ease, border-color 0.15s ease !important;
        cursor: pointer !important;
    }
    [data-testid="stDownloadButton"] > button:hover {
        background: #F5F5F7 !important;
        border-color: #8E8E93 !important;
    }

    /* ── Source indicator pills ───────────────────────────────── */
    .src-pill {
        display: inline-block; border-radius: 20px;
        padding: 3px 10px; font-size: 0.72rem; font-weight: 600;
        margin-right: 4px; letter-spacing: 0.3px;
    }
    .src-on  { background: rgba(52,199,89,0.12); color: #1A7A37; border: 1px solid rgba(52,199,89,0.3); }
    .src-off { background: rgba(142,142,147,0.1); color: #8E8E93; border: 1px solid rgba(142,142,147,0.2); }

    /* ── Footer ───────────────────────────────────────────────── */
    .pfas-footer {
        text-align: center; font-size: 0.70rem; color: #8E8E93;
        margin-top: 48px; padding-top: 16px;
        border-top: 1px solid #E5E5EA;
        letter-spacing: 0.3px;
    }

    /* ── Streamlit tab overrides ──────────────────────────────── */
    button[data-baseweb="tab"] {
        font-size: 0.88rem !important; font-weight: 500 !important;
        padding: 10px 4px !important; margin-right: 24px !important;
        color: #6E6E73 !important;
        border-bottom: 2px solid transparent !important;
        transition: color 0.15s ease !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        font-weight: 600 !important;
        color: #1D1D1F !important;
        border-bottom: 2px solid #007AFF !important;
    }
    button[data-baseweb="tab"]:hover { color: #1D1D1F !important; }

    /* ── Streamlit divider ────────────────────────────────────── */
    hr { border-color: #E5E5EA !important; }

    /* ── Streamlit dataframe ──────────────────────────────────── */
    [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }

    /* ── Streamlit expander ───────────────────────────────────── */
    [data-testid="stExpander"] {
        border: 1px solid #E5E5EA !important;
        border-radius: 12px !important;
        margin-bottom: 10px;
    }

    /* ── Streamlit alerts ─────────────────────────────────────── */
    [data-testid="stAlert"] { border-radius: 10px !important; }

    /* ── Empty state placeholder ──────────────────────────────── */
    .empty-state {
        color: #8E8E93; padding: 64px 0 48px 0;
        text-align: center; font-size: 0.92rem; line-height: 1.6;
    }
    .empty-state-icon { font-size: 2.2rem; margin-bottom: 14px; opacity: 0.45; display: block; }

    /* ── Hide native Streamlit running indicator ──────────────── */
    [data-testid="stStatusWidget"] { display: none !important; }

    /* ── Circular progress ring ──────────────────────────────── */
    .progress-ring-container {
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        gap: 12px; padding: 32px 36px; margin: 8px 0 16px 0;
        background: #F5F5F7; border: 1px solid #E5E5EA; border-radius: 12px;
    }
    .progress-ring-wrapper {
        position: relative; width: 110px; height: 110px;
    }
    .progress-ring-wrapper svg { width: 110px; height: 110px; }
    .progress-ring-pct {
        position: absolute; top: 50%; left: 50%;
        transform: translate(-50%, -50%);
        font-size: 1.45rem; font-weight: 700; color: #1D1D1F; letter-spacing: -1px;
    }
    .progress-ring-step {
        font-size: 0.68rem; font-weight: 600; color: #8E8E93;
        text-transform: uppercase; letter-spacing: 1.5px;
    }
    .progress-ring-label {
        font-size: 0.92rem; font-weight: 500; color: #1D1D1F;
    }
    /* ── Email generating indicator ──────────────────────────── */
    .email-generating {
        color: #6E6E73; font-size: 0.9rem; font-weight: 500;
        padding: 20px 0; text-align: center; letter-spacing: 0.1px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════════════
# RENDERING FUNCTIONS  (defined before any UI calls)
# ═══════════════════════════════════════════════════════════════════════════════

def _render_flag(flag) -> None:
    """Render a single FlagItem as a styled HTML block — supports all spec severity types."""
    css_class = f"flag-{flag.severity}"
    icon = {
        "critical":        "🔴",
        "commercial":      "🟣",
        "technical":       "🟠",
        "pathway":         "🔵",
        "special_handling":"⚪",
        "warning":         "🟡",
        "info":            "🔵",
        "ok":              "🟢",
    }.get(flag.severity, "⚪")
    label = {
        "critical":        "CRITICAL",
        "commercial":      "COMMERCIAL",
        "technical":       "TECHNICAL",
        "pathway":         "PATHWAY",
        "special_handling":"SPECIAL HANDLING",
        "warning":         "WARNING",
        "info":            "INFO",
        "ok":              "OK",
    }.get(flag.severity, flag.severity.upper())
    detail_html = f'<div class="flag-detail">{flag.detail}</div>' if flag.detail else ""
    st.markdown(
        f'<div class="flag-row {css_class}">'
        f'<strong>{icon} [{label} — {flag.rule_id}]</strong> {flag.message}'
        f'{detail_html}</div>',
        unsafe_allow_html=True,
    )


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
    import pandas as pd

    st.markdown(
        '<div class="section-header">TOF Coverage Analysis — Theoretical vs Reported Organic Fluorine</div>',
        unsafe_allow_html=True,
    )

    ratio_pct = tof.coverage_ratio * 100
    status_icon = "⚠️" if tof.unknown_pfas_flag else "✅"
    status_text = (
        f"**{status_icon} Theoretical TOF covers {ratio_pct:.1f}% of reported {tof.measured_type}**"
    )
    st.markdown(status_text)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="val">{format_conc_auto(tof.theoretical_tof_mg_L)}</div>'
            f'<div class="lbl">Theoretical TOF (as F)</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="val">{format_conc_auto(tof.measured_mg_L)}</div>'
            f'<div class="lbl">Reported {tof.measured_type} (as F)</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        pct_color = "#EF4444" if tof.unknown_pfas_flag else "#22C55E"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="val" style="color:{pct_color}">{ratio_pct:.1f}%</div>'
            f'<div class="lbl">Coverage ratio</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Species contributions table
    if tof.species_contributions:
        contrib_rows = [
            {
                "Species": name,
                "Concentration": format_conc_auto(conc),
                "F Contribution": format_conc_auto(f_c) + " as F",
                "% of Theoretical TOF": (
                    f"{f_c / tof.theoretical_tof_mg_L * 100:.1f}%"
                    if tof.theoretical_tof_mg_L > 0 else "—"
                ),
            }
            for name, conc, f_c in tof.species_contributions
        ]
        st.dataframe(pd.DataFrame(contrib_rows), use_container_width=True, hide_index=True)

    for f in tof.flags:
        _render_flag(f)

    if tof.unknown_pfas_flag:
        st.caption(
            f"Threshold: theoretical TOF must be ≥ {TOF_COVERAGE_THRESHOLD * 100:.0f}% "
            f"of reported {tof.measured_type} to clear the unknown-PFAS flag."
        )


def _render_sample_section(
    sr: SampleResult,
    expanded: bool = True,
    nd_species: "List[str] | None" = None,
) -> None:
    """Render Module 1 + 2 results for a single sample."""
    import pandas as pd

    m1 = sr.module1
    m2 = sr.module2

    status_icon = {"PROCEED": "🟢", "CONDITIONAL": "🟡", "CRITICAL": "🔴"}.get(sr.sample_status, "⚪")

    with st.expander(
        f"{status_icon}  Sample: **{sr.sample_name}**  |  "
        f"Status: {sr.sample_status}  |  "
        f"Total PFAS: {format_conc_auto(m1.total_conc_mg_L)}",
        expanded=expanded,
    ):
        # ── Module 1 ──────────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Module 1 — PFAS Composition Analysis</div>',
                    unsafe_allow_html=True)

        if m1.species:
            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1:
                st.markdown(
                    f'<div class="metric-card"><div class="val">{format_conc_auto(m1.total_conc_mg_L)}</div>'
                    f'<div class="lbl">Total PFAS (detected)</div></div>',
                    unsafe_allow_html=True,
                )
            with mc2:
                st.markdown(
                    f'<div class="metric-card"><div class="val">{len(m1.primary_set)}</div>'
                    f'<div class="lbl">Primary Set (Top5 + ≥5%)</div></div>',
                    unsafe_allow_html=True,
                )
            with mc3:
                st.markdown(
                    f'<div class="metric-card"><div class="val">{format_pct(m1.top5_cumulative_pct)}</div>'
                    f'<div class="lbl">Top-5 cumulative %</div></div>',
                    unsafe_allow_html=True,
                )
            with mc4:
                st.markdown(
                    f'<div class="metric-card"><div class="val">{format_pct(m1.other_fraction_pct)}</div>'
                    f'<div class="lbl">Other fraction %</div></div>',
                    unsafe_allow_html=True,
                )

            st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

            # Species table
            rows = [
                {
                    "Analyte": s.name,
                    "Full Name": s.full_name,
                    "Category": CATEGORY_LABELS.get(s.category, s.category),
                    "Concentration": format_conc_auto(s.conc_mg_L),
                    "% of Total": f"{s.percentage:.1f}%",
                    "Primary Set": "✓" if s.in_primary_set else "",
                }
                for s in m1.species
                if s.conc_mg_L > 0 or s.in_primary_set
            ]
            if rows:
                st.dataframe(
                    pd.DataFrame(rows),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Primary Set": st.column_config.TextColumn(width="small"),
                        "% of Total": st.column_config.TextColumn(width="small"),
                    },
                )

            # Category breakdown
            if m1.category_fractions:
                st.markdown("**Composition by Category:**")
                cat_items = sorted(m1.category_fractions.items(), key=lambda x: -x[1])
                cat_cols = st.columns(min(len(cat_items), 4))
                for i, (cat, frac) in enumerate(cat_items):
                    if frac > 0.001:
                        with cat_cols[i % len(cat_cols)]:
                            st.metric(
                                label=CATEGORY_LABELS.get(cat, cat).split(" (")[0],
                                value=format_pct(frac * 100),
                            )
        else:
            if nd_species:
                st.warning(
                    f"All {len(nd_species)} analyte(s) in this sample are **below the detection limit (ND)**. "
                    "No concentrations were quantified — values shown as ND/non-detect in the source file."
                )
                nd_rows = [
                    {
                        "Analyte": name,
                        "Full Name": __import__("utils").get_pfas_info(name).get("full_name", name),
                        "Category": CATEGORY_LABELS.get(
                            __import__("utils").get_pfas_info(name).get("category", "unknown"), "Unclassified"
                        ),
                        "Result": "< MDL (ND)",
                    }
                    for name in nd_species
                ]
                st.dataframe(pd.DataFrame(nd_rows), use_container_width=True, hide_index=True)
                st.caption(
                    "ND species are excluded from totals per spec policy (do_not_assume_zero = true). "
                    "No treatment flag can be raised without a quantified concentration."
                )
            else:
                st.warning("No concentration data for this sample.")

        # ── TOF / AOF Coverage inline summary ────────────────────────────────
        # Surfaces the key "unknown PFAS" question right in the Profile Summary,
        # before the detailed TOF section below.
        if sr.tof_result is not None:
            tof = sr.tof_result
            cov_pct = tof.coverage_ratio * 100
            if tof.unknown_pfas_flag:
                _tof_bg = "#FFF1F2"; _tof_border = "#FF3B30"; _tof_icon = "⚠️"
                _tof_note = (
                    f"Only <strong>{cov_pct:.1f}%</strong> of reported "
                    f"{tof.measured_type} is accounted for by identified species. "
                    "A significant unknown PFAS fraction is present — "
                    "treatment efficacy for unidentified species cannot be guaranteed."
                )
            else:
                _tof_bg = "#F0FFF5"; _tof_border = "#34C759"; _tof_icon = "✅"
                _tof_note = (
                    f"Identified species account for <strong>{cov_pct:.1f}%</strong> of "
                    f"reported {tof.measured_type} — good coverage, low unknown-PFAS risk."
                )
            st.markdown(
                f'<div style="background:{_tof_bg}; border:1px solid {_tof_border}; '
                f'border-radius:10px; padding:10px 16px; margin:8px 0; font-size:0.85rem;">'
                f'{_tof_icon} <strong>TOF/AOF Coverage:</strong> '
                f'Theoretical TOF {format_conc_auto(tof.theoretical_tof_mg_L)} as F &nbsp;|&nbsp; '
                f'Reported {tof.measured_type} {format_conc_auto(tof.measured_mg_L)} as F &nbsp;|&nbsp; '
                f'Coverage {cov_pct:.1f}%<br>'
                f'<span style="font-size:0.80rem; opacity:0.8;">{_tof_note}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        # ── M1 flags (e.g. MAX-UNKNOWN) ───────────────────────────────────────
        for f in m1.flags:
            _render_flag(f)

        # ── Average Case Analysis (steady-state, for stat-summary datasets) ───
        if getattr(sr, "avg_module1", None) is not None:
            avg_m1 = sr.avg_module1
            with st.expander(
                f"📊  Average / Steady-State PFAS Profile  |  "
                f"Total PFAS: {format_conc_auto(avg_m1.total_conc_mg_L)}",
                expanded=False,
            ):
                st.caption(
                    "Steady-state operating concentrations (Average). "
                    "Equipment is sized on the Maximum (worst-case) above; "
                    "this view shows the expected day-to-day loading."
                )
                if avg_m1.species:
                    import pandas as _pd
                    avg_rows = [
                        {
                            "Analyte": s.name,
                            "Full Name": s.full_name,
                            "Avg Concentration": format_conc_auto(s.conc_mg_L),
                            "% of Avg Total": f"{s.percentage:.1f}%",
                            "Category": CATEGORY_LABELS.get(s.category, s.category),
                        }
                        for s in avg_m1.species
                        if s.conc_mg_L > 0
                    ]
                    if avg_rows:
                        st.dataframe(
                            _pd.DataFrame(avg_rows),
                            use_container_width=True,
                            hide_index=True,
                        )
                    # Category fractions
                    if avg_m1.category_fractions:
                        avg_cat_items = sorted(
                            avg_m1.category_fractions.items(), key=lambda x: -x[1]
                        )
                        avg_cat_cols = st.columns(min(len(avg_cat_items), 4))
                        for i, (cat, frac) in enumerate(avg_cat_items):
                            if frac > 0.001:
                                with avg_cat_cols[i % len(avg_cat_cols)]:
                                    st.metric(
                                        label=CATEGORY_LABELS.get(cat, cat).split(" (")[0],
                                        value=format_pct(frac * 100),
                                    )

        # ── Module 2 ──────────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Module 2 — Species-Based Reactivity Screening</div>',
                    unsafe_allow_html=True)

        if not m2.flags:
            st.info("No reactivity flags triggered.")
        else:
            for f in m2.flags:
                _render_flag(f)

        if m2.treatment_implications:
            st.markdown("**Treatment Implications:**")
            for item in m2.treatment_implications:
                st.markdown(f"- {item}")

        if m2.operating_scenarios:
            with st.expander("Operating Scenario Details"):
                for sc in m2.operating_scenarios:
                    st.markdown(f"- {sc}")


def _render_module3(m3: Module3Result) -> None:
    """Render Module 3 water matrix screening results — spec-aligned parameters."""
    import pandas as pd

    # All recognised matrix parameters (spec required + supplementary)
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
        "COD":         "COD (Chemical Oxygen Demand) ★",
        "TOC":         "TOC (Total Organic Carbon) ★",
        "DOC":         "DOC (Dissolved Organic Carbon)",
        "BOD":         "BOD₅ (Biological Oxygen Demand)",
        "nitrate":     "Nitrate NO₃⁻ ★",
        "NO2":         "Nitrite NO₂⁻ ★",
        "nitrite":     "Nitrite NO₂⁻ ★",       # fallback key
        "ammonia":     "Ammonia / Ammonium (NH₃/NH₄⁺)",
        "TKN":         "TKN (Total Kjeldahl Nitrogen)",
        "TN":          "Total Nitrogen",
        "TP":          "Total Phosphorus",
        "phosphate":   "Phosphate (PO₄³⁻)",
        "UV254":       "UV₂₅₄ Absorbance ☆",
        "UVT254":      "UV₂₅₄ Transmittance ☆",
        "chloride":    "Chloride Cl⁻",
        "fluoride":    "Fluoride F⁻",
        "hardness":    "Total Hardness",
        "alkalinity":  "Total Alkalinity",
        "TDS":         "TDS (Total Dissolved Solids)",
        "sulfate":     "Sulfate SO₄²⁻",
        "pH":          "pH",
        "turbidity":   "Turbidity",
        "TSS":         "TSS (Total Suspended Solids)",
        "temperature": "Temperature",
        "conductivity":"Conductivity",
        "flow_rate":   "Flow Rate",
        "sample_color":"Sample Color",
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
    }

    st.markdown('<div class="section-header">Module 3 — Water Matrix Screening</div>',
                unsafe_allow_html=True)

    st.caption(
        "★ Required by spec  ☆ Recommended by spec  "
        "| Traffic light: 🟢 OK · 🟡 Watch · 🔴 Concern · ⚪ No threshold"
    )

    if m3.detected_params:
        param_rows = [
            {
                "Status": _param_traffic_light(k, v),
                "Parameter": _MATRIX_NICE.get(k, k),
                "Value": (
                    f"{v:.4g}" if isinstance(v, float) and v == v else str(v)
                ),
                "Unit": _MATRIX_UNITS.get(k, ""),
            }
            for k, v in m3.detected_params.items()
        ]
        st.dataframe(
            pd.DataFrame(param_rows),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Status": st.column_config.TextColumn(width="small"),
                "Value":  st.column_config.TextColumn(width="small"),
                "Unit":   st.column_config.TextColumn(width="small"),
            },
        )
    else:
        st.info(
            "No water matrix parameters detected. "
            "Paste them into the text input — e.g. 'COD = 120 mg/L, nitrate = 8 mg/L, hardness = 150 mg/L, chloride = 200 mg/L'."
        )

    if m3.missing_required_params:
        st.error(
            "**Required matrix parameters missing:** " +
            " | ".join(m3.missing_required_params),
            icon="🚨",
        )

    for f in m3.flags:
        _render_flag(f)

    if m3.missing_params:
        st.caption(f"Not provided: {', '.join(str(p) for p in m3.missing_params)}")


def _build_text_report(result: EvaluationResult) -> str:
    """Build a plain-text version of the full technical report for download."""
    from datetime import date

    lines = [
        "=" * 70,
        "PFAS MATERIAL EVALUATION ENGINE — TECHNICAL REPORT",
        "Claros R&D Team  |  Framework Architecture by Zack Liu",
        f"Generated: {date.today().strftime('%B %d, %Y')}",
        "=" * 70,
        "",
        f"OVERALL STATUS: {result.overall_status}",
        "",
        "STATUS REASONS:",
        *[f"  - {r}" for r in result.status_reasons],
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
            "MODULE 1 — PFAS COMPOSITION ANALYSIS",
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
        lines.append("MODULE 2 — REACTIVITY SCREENING")
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
                f"  Unknown PFAS flag: {'YES — significant unknown PFAS likely present' if t.unknown_pfas_flag else 'No'}",
                "",
            ]

    lines += [
        "-" * 70,
        "MODULE 3 — WATER MATRIX SCREENING",
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
        "Claros R&D Team  |  Framework Architecture by Zack Liu",
        "=" * 70,
    ]
    return "\n".join(lines)


def _render_technical_output(result: EvaluationResult, parsed: "ParsedData | None" = None) -> None:
    """Render the full structured technical report."""
    # ── Overall Status Banner ─────────────────────────────────────────────────
    st.markdown('<div class="section-header">Overall Evaluation Status</div>',
                unsafe_allow_html=True)

    status_col, reason_col = st.columns([1, 3])
    with status_col:
        st.markdown(status_badge_html(result.overall_status), unsafe_allow_html=True)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
        src_str = ", ".join(result.data_sources) if result.data_sources else "N/A"
        st.caption(f"Sources: {src_str}")

    with reason_col:
        for reason in result.status_reasons:
            st.markdown(f"- {reason}")

    # ── Variability Banner ────────────────────────────────────────────────────
    if result.variability_flag:
        vf = result.variability_flag
        if vf.severity == "info":
            # Stat-range note (Average + Maximum from same stream)
            _vbg  = "#EFF6FF"; _vborder = "#BFDBFE"; _vcolor = "#1E40AF"
            _vicon = "🔵"; _vkey = vf.rule_id
        else:
            # True high-variability warning
            _vbg  = "#FFF8EC"; _vborder = "#FFCC00"; _vcolor = "#5C3D00"
            _vicon = "🟡"; _vkey = "M1-VAR"
        st.markdown(
            f'<div style="background:{_vbg}; border:1px solid {_vborder}; border-radius:10px; '
            f'padding:12px 18px; font-size:0.86rem; color:{_vcolor}; margin:10px 0;">'
            f'{_vicon} <strong>[{_vkey}]</strong> '
            f'{vf.message}<br>'
            f'<span style="font-size:0.80rem; opacity:0.75;">{vf.detail}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    elif result.variability_ratio is not None:
        st.caption(f"Multi-sample variability ratio: {result.variability_ratio:.1f} (within acceptable range)")

    # ── Project Context (throughput, site, customer) ─────────────────────────
    if parsed and parsed.llm_project_context:
        ctx = parsed.llm_project_context
        ctx_items: list[str] = []
        if ctx.get("customer_name"):
            ctx_items.append(f"**Customer:** {ctx['customer_name']}")
        if ctx.get("site_name"):
            ctx_items.append(f"**Site:** {ctx['site_name']}")
        if ctx.get("country"):
            ctx_items.append(f"**Country:** {ctx['country']}")
        if ctx.get("flow_rate_display"):
            gpm = ctx.get("throughput_gpm")
            gpm_str = f" → **{gpm:.0f} GPM**" if gpm else ""
            flag_str = " 🔴 >100 GPM — large-scale flag" if (gpm and gpm > 100) else ""
            ctx_items.append(f"**Throughput:** {ctx['flow_rate_display']}{gpm_str}{flag_str}")
        elif ctx.get("throughput_gpm"):
            gpm = ctx["throughput_gpm"]
            flag_str = " 🔴 >100 GPM — large-scale flag" if gpm > 100 else ""
            ctx_items.append(f"**Throughput:** {gpm:.0f} GPM{flag_str}")
        if ctx_items:
            st.markdown('<div class="section-header">Project Context</div>', unsafe_allow_html=True)
            st.markdown("  &nbsp;&nbsp;".join(ctx_items))
            if ctx.get("throughput_gpm") and float(ctx["throughput_gpm"]) > 100:
                st.warning(
                    f"⚠️ **Large-scale application** — {ctx['throughput_gpm']:.0f} GPM exceeds the "
                    "100 GPM threshold. Full commercial & engineering review required before quoting.",
                    icon=None,
                )

    # ── Missing Information ───────────────────────────────────────────────────
    if result.missing_info:
        with st.expander("⚠️  Missing / Required Information", expanded=True):
            st.markdown("The following information was not found in uploaded materials:")
            for item in result.missing_info:
                st.markdown(f"- {item}")

    st.markdown("---")

    # ── Per-Sample Sections ───────────────────────────────────────────────────
    n_samples = len(result.samples)
    nd_lookup = parsed.nd_species if parsed else {}
    for i, sr in enumerate(result.samples):
        _render_sample_section(
            sr,
            expanded=(n_samples == 1 or i == 0),
            nd_species=nd_lookup.get(sr.sample_name),
        )

    # ── Module 3: Water Matrix ────────────────────────────────────────────────
    _render_module3(result.module3)

    # ── Treatment Summary ─────────────────────────────────────────────────────
    if result.treatment_summary:
        st.markdown('<div class="section-header">Treatment Technology Guidance</div>',
                    unsafe_allow_html=True)
        for item in result.treatment_summary:
            st.markdown(f"- {item}")

    # ── Download ──────────────────────────────────────────────────────────────
    st.markdown("---")
    report_text = _build_text_report(result)
    st.download_button(
        "⬇  Download Full Technical Report (.txt)",
        data=report_text,
        file_name="PFAS_Evaluation_Report.txt",
        mime="text/plain",
    )


def _render_email_draft(result: EvaluationResult, email_text_override: str | None = None) -> None:
    """Render the business email draft tab."""
    email_text = email_text_override if email_text_override is not None else result.email_draft

    st.markdown('<div class="section-header">Internal Business Email Draft</div>',
                unsafe_allow_html=True)
    st.caption(
        "Auto-generated from evaluation output. Review and edit as needed before sending."
    )
    st.markdown(
        f'<div class="email-box">{email_text}</div>',
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "⬇  Download Email Draft (.txt)",
            data=email_text,
            file_name="PFAS_Email_Draft.txt",
            mime="text/plain",
        )
    with col2:
        with st.expander("📋  Select text to copy"):
            st.text_area(
                "Email text:",
                value=email_text,
                height=300,
                label_visibility="collapsed",
            )


def _render_debug_logs(result: EvaluationResult, parsed: ParsedData | None) -> None:
    """Render parser and engine diagnostic logs."""
    import pandas as pd

    st.markdown('<div class="section-header">Parser & Engine Trace</div>',
                unsafe_allow_html=True)

    if result.logs:
        st.code("\n".join(str(x) for x in result.logs), language="text")

    if parsed:
        # ── LLM-specific debug info ───────────────────────────────────────────
        if parsed.llm_parse_notes:
            st.markdown("**🤖 LLM Parse Notes:**")
            for note in parsed.llm_parse_notes:
                st.info(note)

        if parsed.sample_metadata:
            st.markdown("**🤖 LLM Sample Metadata:**")
            meta_rows = [
                {
                    "Sample": name,
                    "Statistical Summary": "Yes" if m.is_statistical_summary else "No",
                    "Type": m.summary_type or "—",
                }
                for name, m in parsed.sample_metadata.items()
            ]
            st.dataframe(pd.DataFrame(meta_rows), hide_index=True, use_container_width=True)

        if parsed.llm_project_context:
            st.markdown("**🤖 LLM Project Context:**")
            ctx_rows = [{"Field": k, "Value": str(v)} for k, v in parsed.llm_project_context.items()]
            st.dataframe(pd.DataFrame(ctx_rows), hide_index=True, use_container_width=True)

        if parsed.llm_raw_response:
            with st.expander("🤖 LLM Raw Response (JSON)", expanded=False):
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

        if parsed.matrix_params:
            st.markdown("**Detected Matrix Parameters:**")
            st.dataframe(
                pd.DataFrame([{"Parameter": k, "Value": v} for k, v in parsed.matrix_params.items()]),
                hide_index=True,
            )

        if parsed.nd_species:
            st.markdown("**Non-Detect (ND) Species — Analyzed but Below Detection Limit:**")
            for sample, nd_list in parsed.nd_species.items():
                st.info(f"Sample '{sample}': {', '.join(str(s) for s in nd_list)}")

        if parsed.keyword_species:
            st.markdown("**Keyword Species Detected (from text, no concentration):**")
            st.code(", ".join(str(s) for s in parsed.keyword_species))


# ═══════════════════════════════════════════════════════════════════════════════
# PROGRESS RING HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _render_progress_circle(pct: int, step: str, label: str) -> str:
    """Render an SVG circular progress ring with percentage, step label, and description."""
    r = 44
    circumference = 2 * 3.14159 * r   # ≈ 276.5
    offset = circumference * (1 - pct / 100)
    return (
        '<div class="progress-ring-container">'
        '<div class="progress-ring-wrapper">'
        '<svg viewBox="0 0 100 100">'
        f'<circle cx="50" cy="50" r="{r}" fill="none" stroke="#E5E5EA" stroke-width="9"/>'
        f'<circle cx="50" cy="50" r="{r}" fill="none" stroke="#007AFF" stroke-width="9"'
        f' stroke-dasharray="{circumference:.1f}" stroke-dashoffset="{offset:.1f}"'
        f' stroke-linecap="round" transform="rotate(-90 50 50)"/>'
        '</svg>'
        f'<div class="progress-ring-pct">{pct}%</div>'
        '</div>'
        f'<div class="progress-ring-step">{step}</div>'
        f'<div class="progress-ring-label">{label}</div>'
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

result: EvaluationResult | None = st.session_state.eval_result

# ═══════════════════════════════════════════════════════════════════════════════
# HEADER BANNER — full-width
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <div class="pfas-header">
        <h1>PFAS Material Evaluation Engine</h1>
        <div class="subtitle">Claros R&amp;D &nbsp;·&nbsp; Preliminary Treatment Feasibility Screening</div>
        <div class="byline">Lead Framework by Zack Liu &nbsp;·&nbsp; Internal R&amp;D Use Only</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ═══════════════════════════════════════════════════════════════════════════════
# INPUT ROW — horizontal single row
# ═══════════════════════════════════════════════════════════════════════════════

inp_a, inp_b, inp_c, inp_d = st.columns([1, 1, 1.5, 1.5], gap="medium")

with inp_a:
    st.markdown('<div class="input-label">Excel PFAS Data</div>', unsafe_allow_html=True)
    excel_file = st.file_uploader(
        "Excel",
        type=["xlsx", "xls", "csv"],
        help=(
            "Column A = PFAS analyte names, Columns B+ = sample concentrations. "
            "Unit detected automatically."
        ),
        label_visibility="collapsed",
    )

with inp_b:
    st.markdown('<div class="input-label">PDF Lab Report</div>', unsafe_allow_html=True)
    pdf_file = st.file_uploader(
        "PDF",
        type=["pdf"],
        help="PDF lab report with quantified PFAS results. Table and text-based PDFs supported.",
        label_visibility="collapsed",
    )

with inp_c:
    st.markdown('<div class="input-label">Customer Email / Notes</div>', unsafe_allow_html=True)
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

with inp_d:
    st.markdown('<div class="input-label">Treatment Goals / Context</div>', unsafe_allow_html=True)
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

# ═══════════════════════════════════════════════════════════════════════════════
# CONTROLS ROW — parsing mode + source pills + run button
# ═══════════════════════════════════════════════════════════════════════════════

ctrl_a, ctrl_b, ctrl_c = st.columns([2.5, 1, 1], gap="medium")

_api_key = _get_api_key()

with ctrl_a:
    st.markdown('<div class="section-header">Parsing Mode</div>', unsafe_allow_html=True)
    if _api_key:
        use_llm = st.toggle(
            "✨ AI-Assisted Parsing (Claude Sonnet)",
            value=True,
            help=(
                "Use Claude Sonnet to read the uploaded documents and extract all data. "
                "Handles flexible layouts, multilingual labels, statistical-summary detection, "
                "and large CofA files with 50+ analytes. "
                "Falls back to rule-based parsing automatically if the API call fails."
            ),
        )
        st.caption("🟢 API key configured — AI parsing available")
    else:
        use_llm = False
        st.caption("⚪ AI parsing unavailable — add ANTHROPIC_API_KEY to Streamlit secrets to enable.")

with ctrl_b:
    st.markdown('<div class="section-header">Active Sources</div>', unsafe_allow_html=True)
    xl_cls  = "src-on" if excel_file else "src-off"
    pdf_cls = "src-on" if pdf_file else "src-off"
    txt_cls = "src-on" if (email_text.strip() or goals_text.strip()) else "src-off"
    st.markdown(
        f'<div style="padding-top:4px;">'
        f'<span class="src-pill {xl_cls}">Excel</span>'
        f'<span class="src-pill {pdf_cls}">PDF</span>'
        f'<span class="src-pill {txt_cls}">Text</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

with ctrl_c:
    st.markdown('<div class="section-header">&nbsp;</div>', unsafe_allow_html=True)
    run_clicked = st.button("▶  Run Evaluation", type="primary")

if not (excel_file or pdf_file or email_text.strip() or goals_text.strip()):
    st.info(
        "Upload an Excel file or PDF, or paste text to begin. At least one source is required.",
        icon="ℹ️",
    )

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# LOADING INDICATOR (shown while evaluation runs)
# ═══════════════════════════════════════════════════════════════════════════════

loading_placeholder = st.empty()

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT TABS — three-layer
# ═══════════════════════════════════════════════════════════════════════════════

tab_tech, tab_email, tab_debug = st.tabs([
    "📋  Technical Output",
    "✉  Business Email Draft",
    "🔍  Debug / Logs",
])

# ─── Handle Run button ───────────────────────────────────────────────────────
if run_clicked:
    if not (excel_file or pdf_file or email_text.strip() or goals_text.strip()):
        with tab_tech:
            st.warning("Please provide at least one input source before running.")
    else:
        try:
            # ── Step 1: Read file bytes ──────────────────────────────────────
            loading_placeholder.markdown(
                _render_progress_circle(5, "Step 1 of 2", "Reading uploaded files…"),
                unsafe_allow_html=True,
            )
            excel_bytes = excel_file.read() if excel_file else None
            pdf_bytes   = pdf_file.read()   if pdf_file   else None

            if use_llm and _api_key:
                # ── Step 2: LLM parsing ──────────────────────────────────────
                loading_placeholder.markdown(
                    _render_progress_circle(15, "Step 1 of 2", "Sending to AI parser (Claude Sonnet)…"),
                    unsafe_allow_html=True,
                )
                from llm_parser import parse_with_llm
                parsed = parse_with_llm(
                    excel_bytes=excel_bytes,
                    excel_filename=excel_file.name if excel_file else None,
                    pdf_bytes=pdf_bytes,
                    pdf_filename=pdf_file.name if pdf_file else None,
                    goals_text=goals_text,
                    api_key=_api_key,
                )
                if email_text.strip():
                    from parser import parse_text
                    text_result = parse_text(email_text, "")
                    parsed.merge(text_result)
                    parsed.customer_notes_text = email_text.strip()
            else:
                loading_placeholder.markdown(
                    _render_progress_circle(30, "Step 1 of 2", "Parsing document…"),
                    unsafe_allow_html=True,
                )
                parsed = parse_all(
                    excel_bytes=excel_bytes,
                    excel_filename=excel_file.name if excel_file else None,
                    pdf_bytes=pdf_bytes,
                    pdf_filename=pdf_file.name if pdf_file else None,
                    email_text=email_text,
                    goals_text=goals_text,
                )

            # ── Step 3: Engine evaluation ────────────────────────────────────
            loading_placeholder.markdown(
                _render_progress_circle(65, "Step 2 of 2", "Running evaluation engine…"),
                unsafe_allow_html=True,
            )
            eval_result = evaluate(parsed)

            loading_placeholder.markdown(
                _render_progress_circle(95, "Step 2 of 2", "Preparing output…"),
                unsafe_allow_html=True,
            )
            st.session_state.eval_result  = eval_result
            st.session_state.parsed_data  = parsed
            st.session_state.llm_email    = None   # generated lazily in email tab
            st.rerun()

        except Exception as exc:
            import traceback as _tb
            loading_placeholder.empty()
            st.error(f"Evaluation error: {exc}")
            st.code(_tb.format_exc(), language="text")

# Re-bind after potential rerun
result = st.session_state.eval_result

# ── Tab 1: Technical Output ───────────────────────────────────────────────────
with tab_tech:
    if result is None:
        st.markdown(
            '<div class="empty-state">'
            '<span class="empty-state-icon">⚗</span>'
            'Technical evaluation output will appear here after running the engine.'
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
            '<span class="empty-state-icon">✉</span>'
            'Business email draft will be generated after running the evaluation.'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        llm_email = st.session_state.get("llm_email")
        _parsed_data = st.session_state.get("parsed_data")

        # Generate email now (user is already looking at Technical Output tab)
        if llm_email is None and use_llm and _api_key and _parsed_data:
            _email_ph = st.empty()
            _email_ph.markdown(
                '<div class="email-generating">✉&nbsp; Generating AI email draft with Claude Sonnet…</div>',
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
                ["✨ AI-Written (Claude Sonnet)", "📄 Template-Generated"],
                horizontal=True,
                label_visibility="collapsed",
            )
            chosen_email = llm_email if "AI" in email_version else None
            _render_email_draft(result, email_text_override=chosen_email)
        else:
            _render_email_draft(result)

# ── Tab 3: Debug / Logs ───────────────────────────────────────────────────────
with tab_debug:
    if result is None:
        st.markdown(
            '<div class="empty-state">'
            '<span class="empty-state-icon">🔍</span>'
            'Parser and engine logs will appear here after running.'
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
    <div class="pfas-footer">
        PFAS Evaluation Engine v1.0 &nbsp;|&nbsp; Claros R&amp;D Team &nbsp;|&nbsp;
        Framework Architecture by Zack Liu &nbsp;|&nbsp;
        For internal R&amp;D screening use only — not a substitute for detailed engineering design.
    </div>
    """,
    unsafe_allow_html=True,
)
