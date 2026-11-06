"""
email_drafts.py: email drafts written by Claude from an evaluation result.

Two drafts share one summary of the evaluation:
  Enhanced    a structured internal briefing.
  Supervised  the short reply Zack sends to a sales colleague, written to
              email_style_guide.md. That guide is distilled from replies he
              actually sent; it holds rules only, never email text.

Claros Product Development Team | Framework Architecture by Zack Liu
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from engine import EvaluationResult
from parser import describe_flow
from utils import CATEGORY_LABELS, format_conc_auto, format_pct

STYLE_GUIDE_PATH = Path(__file__).with_name("email_style_guide.md")

# The model writes this token in the greeting; the app replaces it with the
# colleague's first name, so changing the name does not need a new draft.
RECIPIENT_TOKEN = "{RECIPIENT}"


def evaluation_summary(
    result: EvaluationResult, project_context: Dict[str, Any], open_checks: int = 0
) -> str:
    """The evaluation as compact text for the model to write from."""
    lines: List[str] = [f"OVERALL STATUS: {result.overall_status}"]
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
            lines.append("\nPFAS PROFILE (AVERAGE / STEADY-STATE):")
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

    if open_checks:
        lines.append(
            f"\nOPEN DATA CHECKS: {open_checks} value(s) in the report still have to be "
            "checked against the source files."
        )

    return "\n".join(lines)


_ENHANCED_SYSTEM = (
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


def generate_enhanced_email(
    result: EvaluationResult, project_context: Dict[str, Any], api_key: str
) -> str:
    """
    A structured internal briefing written by Claude from the evaluation.
    Falls back to the template draft if the API call fails.
    """
    summary = evaluation_summary(result, project_context)
    try:
        from llm_parser import call_claude
        return call_claude(
            api_key, _ENHANCED_SYSTEM, f"EVALUATION RESULTS:\n{summary}",
            max_tokens=16_000, effort="medium",
        )
    except Exception as exc:
        return f"[AI email generation failed: {exc}]\n\n{result.email_draft}"


def load_style_guide() -> Tuple[str, Dict[str, str]]:
    """The style guide text and its header fields (version, updated, based on, status)."""
    text = STYLE_GUIDE_PATH.read_text(encoding="utf-8")
    meta = {
        key.lower(): value.strip()
        for key, value in re.findall(r"^(Version|Updated|Based on|Status):\s*(.+)$", text, re.MULTILINE)
    }
    return text, meta


def _supervised_system(style_guide: str) -> str:
    return (
        "You draft the short internal email that Zack Liu, Director of Product Development at "
        "Claros Technologies, sends to a sales colleague after screening a customer's water "
        "data for PFAS treatment. The colleague uses it to decide what to tell the customer "
        "and what to ask next. Zack edits the draft and sends it under his own name, so it "
        "has to read the way he writes and every statement in it has to be correct. It does "
        "not have to be complete: the full technical report goes with it.\n\n"
        "Write to this style guide, which was distilled from replies Zack actually sent:\n\n"
        f"<style_guide>\n{style_guide}\n</style_guide>\n\n"
        "The evaluation you are writing from is in the user message.\n"
        "- Use only what it contains. Never add a number, a species, a customer name or a "
        "recommendation that is not there.\n"
        "- It uses the engine's vocabulary. Turn that into plain words for the email.\n"
        "- If it lists missing information or open data checks that affect the verdict, say "
        "so plainly rather than sounding more certain than the data allows.\n"
        "- Do not use em dashes.\n"
        f"- Begin with the line \"Hi {RECIPIENT_TOKEN},\" exactly as written, braces included. "
        "The application replaces it with the colleague's first name.\n"
        "- Return only the email text, with no subject line, no markdown and no preamble."
    )


def generate_supervised_email(
    result: EvaluationResult,
    project_context: Dict[str, Any],
    api_key: str,
    open_checks: int = 0,
) -> str:
    """
    The reply to sales, written to the style guide learned from Zack's sent
    emails. Falls back to the template draft if the guide or the API call fails.
    """
    summary = evaluation_summary(result, project_context, open_checks)
    try:
        from llm_parser import call_claude
        style_guide, _ = load_style_guide()
        return call_claude(
            api_key, _supervised_system(style_guide), f"EVALUATION RESULTS:\n{summary}",
            max_tokens=16_000, effort="medium",
        )
    except Exception as exc:
        return f"[Supervised draft could not be written: {exc}]\n\n{result.email_draft}"


def fill_recipient(email_text: str, first_name: str) -> str:
    """Put the colleague's first name into the greeting."""
    return email_text.replace(RECIPIENT_TOKEN, first_name.strip() or "[Name]")
