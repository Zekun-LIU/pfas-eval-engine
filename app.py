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
from engine import EvaluationResult, Module1Result, Module2Result, Module3Result, SampleResult, evaluate
from parser import ParsedData, parse_all
from utils import (
    CATEGORY_LABELS,
    format_conc_auto,
    format_pct,
    severity_badge,
    status_badge_html,
)

# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL CSS
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <style>
    .pfas-header {
        background: linear-gradient(135deg, #1a2942, #2c4066);
        color: white;
        padding: 20px 28px 16px 28px;
        border-radius: 10px;
        margin-bottom: 18px;
    }
    .pfas-header h1 { margin: 0; font-size: 1.75rem; font-weight: 700; color: white; }
    .pfas-header .subtitle { font-size: 0.95rem; color: #aab8cc; margin-top: 4px; }
    .pfas-header .byline { font-size: 0.78rem; color: #7a9bbf; margin-top: 2px; }

    .section-header {
        border-left: 4px solid #2980b9;
        padding: 4px 0 4px 10px;
        font-weight: 600; font-size: 1.05rem; color: #1a2942;
        margin: 16px 0 8px 0;
    }

    .flag-row {
        padding: 8px 12px; border-radius: 5px;
        margin-bottom: 6px; font-size: 0.88rem; line-height: 1.5;
    }
    .flag-critical        { background: #fde8e8; border-left: 4px solid #C0392B; }
    .flag-warning         { background: #fff8e3; border-left: 4px solid #D4AC0D; }
    .flag-info            { background: #e8f4fd; border-left: 4px solid #2980b9; }
    .flag-ok              { background: #eafbea; border-left: 4px solid #1E8449; }
    /* Spec M2 classification types */
    .flag-commercial      { background: #f5eefb; border-left: 4px solid #6C3483; }
    .flag-technical       { background: #fef5ec; border-left: 4px solid #BA4A00; }
    .flag-pathway         { background: #e8f8f5; border-left: 4px solid #117A65; }
    .flag-special_handling{ background: #f2f3f4; border-left: 4px solid #5D6D7E; }
    .flag-detail          { font-size: 0.82rem; color: #555; margin-top: 4px; font-style: italic; }
    /* Variability banner */
    .variability-banner {
        background: #fff8e3; border: 1px solid #D4AC0D; border-radius: 6px;
        padding: 8px 14px; font-size: 0.88rem; margin: 6px 0;
    }

    .metric-card {
        background: #f7f9fc; border: 1px solid #dde3ee;
        border-radius: 8px; padding: 12px 16px; text-align: center;
    }
    .metric-card .val { font-size: 1.35rem; font-weight: 700; color: #1a2942; }
    .metric-card .lbl { font-size: 0.75rem; color: #666; margin-top: 2px; }

    .email-box {
        background: #f7f9fc; border: 1px solid #dde3ee; border-radius: 8px;
        padding: 18px 20px; font-family: 'Courier New', monospace;
        font-size: 0.85rem; white-space: pre-wrap; line-height: 1.6;
    }

    .pfas-footer {
        text-align: center; font-size: 0.72rem; color: #aaa;
        margin-top: 30px; padding-top: 10px; border-top: 1px solid #eee;
    }

    div[data-testid="stButton"] > button {
        width: 100%; background-color: #2c4066; color: white;
        font-weight: 600; font-size: 1.0rem; padding: 10px;
        border-radius: 6px; border: none;
    }
    div[data-testid="stButton"] > button:hover { background-color: #1a2942; }
    .block-container { padding-top: 1rem; }
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


def _render_sample_section(sr: SampleResult, expanded: bool = True) -> None:
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
            st.warning("No concentration data for this sample.")

        for f in m1.flags:
            _render_flag(f)

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
        "nitrate": "mg/L", "NO2": "mg/L",
        "UV254": "cm⁻¹", "UVT254": "%",
        "chloride": "mg/L", "fluoride": "mg/L",
        "hardness": "mg/L as CaCO₃", "TDS": "mg/L",
        "sulfate": "mg/L", "pH": "", "turbidity": "NTU",
        "TSS": "mg/L", "temperature": "°C", "flow_rate": "(raw)",
        "sample_color": "",
    }
    _MATRIX_NICE = {
        "COD": "COD (Chemical Oxygen Demand) ★",
        "TOC": "TOC (Total Organic Carbon) ★",
        "DOC": "DOC (Dissolved Organic Carbon)",
        "nitrate": "Nitrate NO₃⁻ ★",
        "NO2": "Nitrite NO₂⁻ ★",
        "UV254": "UV₂₅₄ Absorbance ☆",
        "UVT254": "UV₂₅₄ Transmittance ☆",
        "chloride": "Chloride Cl⁻",
        "fluoride": "Fluoride F⁻",
        "hardness": "Total Hardness",
        "TDS": "TDS", "sulfate": "Sulfate SO₄²⁻",
        "pH": "pH", "turbidity": "Turbidity",
        "TSS": "TSS", "temperature": "Temperature",
        "flow_rate": "Flow Rate", "sample_color": "Sample Color",
    }

    st.markdown('<div class="section-header">Module 3 — Water Matrix Screening</div>',
                unsafe_allow_html=True)

    st.caption("★ Required by spec  ☆ Recommended by spec")

    if m3.detected_params:
        param_rows = [
            {
                "Parameter": _MATRIX_NICE.get(k, k),
                "Value": str(v),
                "Unit": _MATRIX_UNITS.get(k, ""),
            }
            for k, v in m3.detected_params.items()
        ]
        st.dataframe(pd.DataFrame(param_rows), use_container_width=True, hide_index=True)
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
        st.caption(f"Not provided: {', '.join(m3.missing_params)}")


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
            f"  Primary Set (≥5%): {', '.join(m1.primary_set) if m1.primary_set else 'None'}",
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


def _render_technical_output(result: EvaluationResult) -> None:
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
        st.markdown(
            f'<div class="variability-banner">🟡 <strong>[M1-VAR]</strong> '
            f'{result.variability_flag.message}<br>'
            f'<span style="font-size:0.8rem;color:#666;">{result.variability_flag.detail}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    elif result.variability_ratio is not None:
        st.caption(f"Multi-sample variability ratio: {result.variability_ratio:.1f} (within acceptable range)")

    # ── Missing Information ───────────────────────────────────────────────────
    if result.missing_info:
        with st.expander("⚠️  Missing / Required Information", expanded=True):
            st.markdown("The following information was not found in uploaded materials:")
            for item in result.missing_info:
                st.markdown(f"- {item}")

    st.markdown("---")

    # ── Per-Sample Sections ───────────────────────────────────────────────────
    n_samples = len(result.samples)
    for i, sr in enumerate(result.samples):
        _render_sample_section(sr, expanded=(n_samples == 1 or i == 0))

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


def _render_email_draft(result: EvaluationResult) -> None:
    """Render the business email draft tab."""
    st.markdown('<div class="section-header">Internal Business Email Draft</div>',
                unsafe_allow_html=True)
    st.caption(
        "Auto-generated from evaluation output. Review and edit as needed before sending."
    )
    st.markdown(
        f'<div class="email-box">{result.email_draft}</div>',
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "⬇  Download Email Draft (.txt)",
            data=result.email_draft,
            file_name="PFAS_Email_Draft.txt",
            mime="text/plain",
        )
    with col2:
        with st.expander("📋  Select text to copy"):
            st.text_area(
                "Email text:",
                value=result.email_draft,
                height=300,
                label_visibility="collapsed",
            )


def _render_debug_logs(result: EvaluationResult, parsed: ParsedData | None) -> None:
    """Render parser and engine diagnostic logs."""
    import pandas as pd

    st.markdown('<div class="section-header">Parser & Engine Trace</div>',
                unsafe_allow_html=True)

    if result.logs:
        st.code("\n".join(result.logs), language="text")

    if parsed:
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

        if parsed.keyword_species:
            st.markdown("**Keyword Species Detected (from text, no concentration):**")
            st.code(", ".join(parsed.keyword_species))


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═══════════════════════════════════════════════════════════════════════════════

if "eval_result" not in st.session_state:
    st.session_state.eval_result = None
if "parsed_data" not in st.session_state:
    st.session_state.parsed_data = None


# ═══════════════════════════════════════════════════════════════════════════════
# HEADER BANNER
# ═══════════════════════════════════════════════════════════════════════════════

result: EvaluationResult | None = st.session_state.eval_result

header_left, header_right = st.columns([4, 1])
with header_left:
    st.markdown(
        """
        <div class="pfas-header">
            <h1>⚗ PFAS Material Evaluation Engine</h1>
            <div class="subtitle">Claros R&amp;D Team &nbsp;|&nbsp; Preliminary Treatment Feasibility Screening</div>
            <div class="byline">Developed within Claros R&amp;D &nbsp;|&nbsp; Lead Framework by Zack Liu &nbsp;|&nbsp; v1.0</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with header_right:
    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    if result:
        st.markdown(status_badge_html(result.overall_status), unsafe_allow_html=True)
    else:
        st.markdown(
            '<div style="color:#aaa; font-size:0.85rem; text-align:center; padding-top:18px;">'
            'Run evaluation<br>to see status</div>',
            unsafe_allow_html=True,
        )

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LAYOUT
# ═══════════════════════════════════════════════════════════════════════════════

left_col, right_col = st.columns([1, 2.2], gap="large")

# ─── LEFT PANEL: Inputs ──────────────────────────────────────────────────────
with left_col:
    st.markdown('<div class="section-header">Input Materials</div>', unsafe_allow_html=True)

    excel_file = st.file_uploader(
        "Excel PFAS Data Table (.xlsx / .xls / .csv)",
        type=["xlsx", "xls", "csv"],
        help=(
            "Expected layout: Column A = PFAS analyte names, "
            "Columns B+ = sample concentrations. Unit detected automatically."
        ),
    )

    pdf_file = st.file_uploader(
        "PDF Lab Report",
        type=["pdf"],
        help="PDF lab report with quantified PFAS results. Table and text-based PDFs supported.",
    )

    st.markdown('<div class="section-header">Text Input</div>', unsafe_allow_html=True)

    email_text = st.text_area(
        "Customer Email / Notes",
        height=130,
        placeholder=(
            "Paste customer email or notes here.\n"
            "Example: 'Site water has PFOA 250 ng/L, PFOS 180 ng/L, TFA detected...'\n"
            "Matrix: 'DOC = 4.5 mg/L, sulfate = 180 mg/L, pH = 7.4'"
        ),
        help="Scanned for PFAS species, inline concentrations, and matrix parameters.",
    )

    goals_text = st.text_area(
        "Treatment Goals / Business Notes",
        height=100,
        placeholder=(
            "Example: 'Target <70 ng/L combined PFOA+PFOS per EPA MCL.\n"
            "Drinking water application. Flow 2 MGD. Groundwater site.'"
        ),
        help="Treatment objectives, regulatory targets, flow rate, site context.",
    )

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    run_clicked = st.button("▶  Run Evaluation", type="primary")

    # Input status indicators
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    ind1, ind2, ind3 = st.columns(3)
    with ind1:
        st.markdown(f"{'🟢' if excel_file else '⚪'} Excel")
    with ind2:
        st.markdown(f"{'🟢' if pdf_file else '⚪'} PDF")
    with ind3:
        st.markdown(f"{'🟢' if (email_text.strip() or goals_text.strip()) else '⚪'} Text")

    if not (excel_file or pdf_file or email_text.strip() or goals_text.strip()):
        st.info(
            "Upload an Excel file, PDF, or paste text to begin. "
            "At least one source is required.",
            icon="ℹ️",
        )

# ─── RIGHT PANEL: Outputs ────────────────────────────────────────────────────
with right_col:
    tab_tech, tab_email, tab_debug = st.tabs([
        "📋  Technical Output",
        "✉  Business Email Draft",
        "🔍  Debug / Logs",
    ])

    # ── Handle Run button ────────────────────────────────────────────────────
    if run_clicked:
        if not (excel_file or pdf_file or email_text.strip() or goals_text.strip()):
            with tab_tech:
                st.warning("Please provide at least one input source before running.")
        else:
            with st.spinner("Parsing inputs and running evaluation engine…"):
                try:
                    parsed = parse_all(
                        excel_bytes=excel_file.read() if excel_file else None,
                        excel_filename=excel_file.name if excel_file else None,
                        pdf_bytes=pdf_file.read() if pdf_file else None,
                        pdf_filename=pdf_file.name if pdf_file else None,
                        email_text=email_text,
                        goals_text=goals_text,
                    )
                    eval_result = evaluate(parsed)
                    st.session_state.eval_result = eval_result
                    st.session_state.parsed_data = parsed
                    st.rerun()
                except Exception as exc:
                    import traceback
                    st.error(f"Evaluation error: {exc}")
                    with tab_debug:
                        st.code(traceback.format_exc(), language="text")

    # Re-bind after potential rerun
    result = st.session_state.eval_result

    # ── Tab 1: Technical Output ───────────────────────────────────────────────
    with tab_tech:
        if result is None:
            st.markdown(
                "<div style='color:#888; padding:40px 0; text-align:center; font-size:0.95rem;'>"
                "Technical evaluation output will appear here after running the engine."
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            _render_technical_output(result)

    # ── Tab 2: Email Draft ────────────────────────────────────────────────────
    with tab_email:
        if result is None:
            st.markdown(
                "<div style='color:#888; padding:40px 0; text-align:center; font-size:0.95rem;'>"
                "Business email draft will be generated after running the evaluation."
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            _render_email_draft(result)

    # ── Tab 3: Debug / Logs ───────────────────────────────────────────────────
    with tab_debug:
        if result is None:
            st.markdown(
                "<div style='color:#888; padding:40px 0; text-align:center; font-size:0.95rem;'>"
                "Parser and engine logs will appear here after running."
                "</div>",
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
