"""
engine.py — PFAS Material Intelligence Engine
Rule-based expert system — fully aligned to PFAS_Material_Intelligence_Engine spec v1.0.

Module 1: PFAS Composition & Variability
Module 2: Species-Based Reactivity Screening  (R1–R6, R99 per spec)
Module 3: Water Matrix Screening              (M3_R1–M3_R5 per spec)

All logic is deterministic. No ML. No generative inference.

Claros R&D Team | Framework Architecture by Zack Liu
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from utils import (
    CATEGORY_LABELS,
    PFAS_ALIASES,
    PFAS_SPECIES_DB,
    PFESA_2PLUS2_EXCLUDES,
    classify_pfas,
    format_conc_auto,
    format_pct,
    get_pfas_info,
    is_ether_carboxylate,
    is_pfca_only,
    is_pfsa_sulfonate,
    is_short_telomer,
    normalize_pfas_name,
)
from parser import ParsedData

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE THRESHOLDS  (spec-aligned)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Module 1 ────────────────────────────────────────────────────────────────
PRIMARY_SET_TOP_N          = 5      # Top N species always included in Primary Set
PRIMARY_SET_PCT_THRESHOLD  = 5.0   # Plus all species ≥ this % (secondary extension)
VARIABILITY_HIGH_THRESHOLD = 10.0  # max/min total PFAS ratio flagged as high variation

# ── Module 2 ────────────────────────────────────────────────────────────────
PFSA_KINETICS_PCT          = 20.0  # R4: PFSA fraction > 20% in Primary Set → TECHNICAL
PROCEED_TOTAL_MAX_MG_L     = 50.0  # R99: total PFAS must be < 50 ppm (mg/L)

# ── Module 3 (spec thresholds) ───────────────────────────────────────────────
M3_COD_MAX_MG_L            = 250.0
M3_TOC_MAX_MG_L            = 100.0
M3_NITRATE_MANAGEABLE      = 1.0   # < 1 mg/L (as ion)
M3_NITRATE_HIGH            = 20.0  # > 20 mg/L (as ion)
M3_CHLORIDE_CORROSION      = 1000.0
M3_FLUORIDE_TOF            = 100.0
M3_HARDNESS_PRECIP         = 100.0  # mg/L as CaCO3 — caustic dosing precipitation risk


# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class FlagItem:
    """
    A single evaluation flag.

    Severity types:
      critical        — CRITICAL: technology cannot treat this species / extreme concern
      commercial      — COMMERCIAL: differentiated capability, highlight to customer
      technical       — TECHNICAL: engineering/kinetics consideration
      pathway         — PATHWAY: treatment pathway note
      special_handling — SPECIAL_HANDLING: no immediate concern, monitor
      warning         — general matrix/engineering warning
      info            — informational
      ok              — proceed / no concern
    """
    severity: str
    rule_id: str
    message: str
    detail: str = ""


@dataclass
class SpeciesEntry:
    name: str
    full_name: str
    category: str
    group: str
    chain: Optional[int]
    conc_mg_L: float
    percentage: float
    in_primary_set: bool
    detected: bool           # False if ND/unknown (spec: do_not_assume_zero)


@dataclass
class Module1Result:
    """PFAS Composition & Variability — per sample."""
    sample_name: str
    total_conc_mg_L: float
    species: List[SpeciesEntry]         # sorted descending by concentration
    primary_set: List[str]              # canonical names in Primary Set
    top5: List[str]                     # top 5 by concentration (spec step)
    top5_cumulative_pct: float          # sum of top-5 percentages
    other_fraction_pct: float           # 100 − top5_cumulative_pct
    category_fractions: Dict[str, float]
    flags: List[FlagItem]


@dataclass
class Module2Result:
    """Species-Based Reactivity Screening — per sample."""
    sample_name: str
    flags: List[FlagItem]
    treatment_implications: List[str]
    operating_scenarios: List[str]
    status_contribution: str     # "CRITICAL" | "PROCEED" | "INFORMATIONAL"


@dataclass
class Module3Result:
    """Water Matrix Screening — shared across all samples."""
    detected_params: Dict[str, Any]
    missing_params: List[str]
    missing_required_params: List[str]   # COD/TOC AND nitrate/nitrite
    flags: List[FlagItem]
    status_contribution: str             # "CONDITIONAL" | "PROCEED"


@dataclass
class SampleResult:
    sample_name: str
    module1: Module1Result
    module2: Module2Result
    sample_status: str


@dataclass
class EvaluationResult:
    """Top-level result returned by the engine."""
    samples: List[SampleResult]
    module3: Module3Result
    overall_status: str
    status_reasons: List[str]
    missing_info: List[str]
    treatment_summary: List[str]
    email_draft: str
    logs: List[str]
    has_pfas_data: bool
    data_sources: List[str]
    variability_ratio: Optional[float]
    variability_flag: Optional[FlagItem]


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 1 — PFAS COMPOSITION & VARIABILITY
# ═══════════════════════════════════════════════════════════════════════════════

def run_module1(sample_name: str, pfas_data: Dict[str, Optional[float]]) -> Module1Result:
    """
    Spec steps:
      1. sort_species_descending_by_concentration
      2. compute_total_pfas              (ND/unknown excluded per spec)
      3. compute_species_percent
      4. identify_top5
      5. compute_top5_cumulative_percent
      6. identify_secondary_species      (percent >= 5%)
      7. compute_primary_set_coverage    (top5 ∪ secondary)
      8. compute_other_fraction          (100 - top5_cumulative_percent)
    """
    flags: List[FlagItem] = []

    if not pfas_data:
        return Module1Result(
            sample_name=sample_name, total_conc_mg_L=0.0, species=[],
            primary_set=[], top5=[], top5_cumulative_pct=0.0,
            other_fraction_pct=100.0, category_fractions={},
            flags=[FlagItem("warning", "M1-EMPTY", "No PFAS data for this sample.", "")],
        )

    # Build SpeciesEntry list — only detected (non-None) species
    species_list: List[SpeciesEntry] = []
    for raw_name, conc in pfas_data.items():
        name = normalize_pfas_name(raw_name)
        info = get_pfas_info(name)
        detected = conc is not None
        species_list.append(SpeciesEntry(
            name=name,
            full_name=info.get("full_name", name),
            category=info.get("category", "unknown"),
            group=info.get("group", "unknown"),
            chain=info.get("chain"),
            conc_mg_L=conc if conc is not None else 0.0,
            percentage=0.0,  # calculated below
            in_primary_set=False,
            detected=detected,
        ))

    # Step 1: Sort descending by concentration
    species_list.sort(key=lambda s: s.conc_mg_L, reverse=True)

    # Step 2: Total PFAS (exclude ND/unknown per spec)
    total = sum(s.conc_mg_L for s in species_list if s.detected)

    # Step 3: Percentages
    for s in species_list:
        s.percentage = (s.conc_mg_L / total * 100) if total > 0 and s.detected else 0.0

    # Step 4: Top 5 (by concentration, detected only)
    detected_species = [s for s in species_list if s.detected]
    top5_entries = detected_species[:PRIMARY_SET_TOP_N]
    top5_names = [s.name for s in top5_entries]

    # Step 5: Top-5 cumulative %
    top5_cumulative_pct = sum(s.percentage for s in top5_entries)

    # Step 6: Secondary species (percent >= 5%, not already in top 5)
    secondary = [s for s in detected_species if s.percentage >= PRIMARY_SET_PCT_THRESHOLD
                 and s.name not in top5_names]

    # Step 7: Primary Set = Top5 ∪ Secondary
    primary_set_names = list(dict.fromkeys(top5_names + [s.name for s in secondary]))
    for s in species_list:
        s.in_primary_set = s.name in primary_set_names

    # Step 8: Other fraction
    other_fraction_pct = max(0.0, 100.0 - top5_cumulative_pct)

    # Category fractions (of total detected)
    cat_totals: Dict[str, float] = {}
    for s in species_list:
        if s.detected:
            cat_totals[s.category] = cat_totals.get(s.category, 0.0) + s.conc_mg_L
    category_fractions = {
        cat: val / total if total > 0 else 0.0
        for cat, val in cat_totals.items()
    }

    # Summary flag
    n_detected = len(detected_species)
    n_nd = len([s for s in species_list if not s.detected])
    nd_note = f" | {n_nd} analyte(s) ND/unknown (excluded from totals)" if n_nd else ""
    flags.append(FlagItem(
        severity="info", rule_id="M1-SUMMARY",
        message=(
            f"{n_detected} detected analytes | total: {format_conc_auto(total)} | "
            f"{len(primary_set_names)} in Primary Set (Top5 + ≥5%){nd_note}"
        ),
    ))

    if total == 0:
        flags.append(FlagItem(
            "warning", "M1-ZERO",
            "All PFAS concentrations are ND/unknown for this sample.",
            "Verify detection limits and sample provenance.",
        ))

    return Module1Result(
        sample_name=sample_name,
        total_conc_mg_L=total,
        species=species_list,
        primary_set=primary_set_names,
        top5=top5_names,
        top5_cumulative_pct=top5_cumulative_pct,
        other_fraction_pct=other_fraction_pct,
        category_fractions=category_fractions,
        flags=flags,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 2 — SPECIES-BASED REACTIVITY SCREENING
# ═══════════════════════════════════════════════════════════════════════════════

# Keyword sets for TFMS and 2+2 PFESA (spec keyword lists, uppercase for matching)
_TFMS_KEYWORDS = frozenset([
    "TFMS", "TRIFLUOROMETHANESULFONATE", "TRIFLUOROMETHANESULFONIC ACID",
    "TRIFLATE", "CF3SO3-", "CF3SO3H", "CF3SO2OH", "1493-13-6", "358-23-6",
])
_TFA_KEYWORDS = frozenset([
    "TFA", "TRIFLUOROACETIC ACID", "TRIFLUOROACETATE",
    "CF3COOH", "CF3COO-", "C2HF3O2", "76-05-1",
])
_PFESA_KEYWORDS = frozenset([
    "2+2 PFESA", "2:2 PFESA",
    "PERFLUORO(2-ETHOXYETHANE)SULFONIC ACID",
    "PERFLUORO(2-ETHOXYETHANE)SULFONATE",
    "113507-82-7",
])
# F-53B exclusion — these must NOT trigger the PFESA CRITICAL rule
_F53B_KEYWORDS = frozenset(["F-53B", "73606-19-6"])


def _text_contains_any(text: str, keywords: frozenset) -> bool:
    """Case-insensitive check: does text contain any keyword from the set?"""
    upper = text.upper()
    return any(kw in upper for kw in keywords)


def _in_keyword_species(keyword_species: List[str], keywords: frozenset) -> bool:
    return any(k.upper() in keywords for k in keyword_species)


def run_module2(
    module1: Module1Result,
    keyword_species: Optional[List[str]] = None,
    goals_text: str = "",
    email_text: str = "",
) -> Module2Result:
    """
    Spec rules (priority order):

    R1  (priority 1) — TFMS_Critical:        TFMS in primary_set OR goal        → CRITICAL
    R2  (priority 2) — PFESA_2plus2_Critical: 2+2 PFESA in primary_set OR goal  → CRITICAL
    R3  (priority 3) — TFA_Commercial:        TFA present                        → COMMERCIAL
    R4  (priority 4) — PFSA_Kinetics:         PFSA > 20% in Primary Set          → TECHNICAL
    R5  (priority 5) — Short_Telomer_Pathway: telomer detected AND m < 4         → PATHWAY
    R6  (priority 6) — Ether_Carboxylate:     ether carboxylate (not PFESA_2+2)  → SPECIAL_HANDLING
    R99 (priority 99)— Proceed_Condition:     all primary = PFCA AND total < 50 ppm → PROCEED
    """
    flags: List[FlagItem] = []
    treatment_implications: List[str] = []
    operating_scenarios: List[str] = []
    status = "INFORMATIONAL"   # Default: no M2 escalation

    primary_set = module1.primary_set
    primary_set_upper = {s.upper() for s in primary_set}
    kw = keyword_species or []
    all_text = f"{goals_text}\n{email_text}"

    # Helper: is a given canonical species name in the Primary Set?
    def in_primary(canonical: str) -> bool:
        return canonical.upper() in primary_set_upper

    # ── R1: TFMS Critical (priority 1) ──────────────────────────────────────
    tfms_in_primary = in_primary("TFMS") or any(
        normalize_pfas_name(s) == "TFMS" for s in primary_set
    )
    tfms_in_goal = _text_contains_any(goals_text, _TFMS_KEYWORDS)
    tfms_in_text = _text_contains_any(all_text, _TFMS_KEYWORDS)
    tfms_in_kw   = _in_keyword_species(kw, _TFMS_KEYWORDS)

    if tfms_in_primary or tfms_in_goal or tfms_in_kw:
        source = "Primary Set" if tfms_in_primary else ("treatment goal" if tfms_in_goal else "keyword detection")
        flags.append(FlagItem(
            severity="critical", rule_id="R1",
            message=f"TFMS detected ({source}). Technology cannot treat TFMS. Confirm whether TFMS is part of primary goal.",
            detail=(
                "Trifluoromethanesulfonate (triflate) is not removable by any conventional PFAS "
                "treatment technology (GAC, IX, RO, AOP, or standard electrochemical oxidation). "
                "If TFMS is a required treatment target, re-evaluate technology scope with engineering team."
            ),
        ))
        treatment_implications.append(
            "TFMS cannot be treated by current technology. Confirm treatment scope with customer."
        )
        status = "CRITICAL"

    # ── R2: 2+2 PFESA Critical (priority 2) ─────────────────────────────────
    # Exclude F-53B from triggering this rule (spec explicit exclusion)
    pfesa_in_primary = in_primary("2+2 PFESA") or any(
        normalize_pfas_name(s) == "2+2 PFESA"
        and s.upper() not in {e.upper() for e in PFESA_2PLUS2_EXCLUDES}
        for s in primary_set
    )
    pfesa_in_goal = (
        _text_contains_any(goals_text, _PFESA_KEYWORDS)
        and not _text_contains_any(goals_text, _F53B_KEYWORDS)
    )
    pfesa_in_kw = (
        _in_keyword_species(kw, _PFESA_KEYWORDS)
        and not _in_keyword_species(kw, _F53B_KEYWORDS)
    )

    if pfesa_in_primary or pfesa_in_goal or pfesa_in_kw:
        source = "Primary Set" if pfesa_in_primary else ("treatment goal" if pfesa_in_goal else "keyword detection")
        flags.append(FlagItem(
            severity="critical", rule_id="R2",
            message=f"2+2 PFESA detected ({source}). Technology cannot treat 2+2 PFESA. Confirm whether this is a primary goal.",
            detail=(
                "Perfluoro(2-ethoxyethane) sulfonic acid (2+2 PFESA) is not treatable by "
                "standard PFAS technologies. Its ether-sulfonate structure confers high resistance "
                "to oxidative, reductive, and adsorptive treatment pathways. "
                "Note: F-53B (which may co-occur) is a separate species — not classified as 2+2 PFESA."
            ),
        ))
        treatment_implications.append(
            "2+2 PFESA cannot be treated by current technology. Confirm treatment scope."
        )
        if status != "CRITICAL":
            status = "CRITICAL"

    # ── R3: TFA Commercial (priority 3) ─────────────────────────────────────
    tfa_in_primary = in_primary("TFA") or any(
        normalize_pfas_name(s) == "TFA" for s in primary_set
    )
    tfa_in_any = tfa_in_primary or _text_contains_any(all_text, _TFA_KEYWORDS) or _in_keyword_species(kw, _TFA_KEYWORDS)

    if tfa_in_any:
        source = "Primary Set" if tfa_in_primary else "detected in materials"
        flags.append(FlagItem(
            severity="commercial", rule_id="R3",
            message=f"TFA detected ({source}). Highlight differentiated TFA treatment capability.",
            detail=(
                "Trifluoroacetic acid (TFA) is a commercially significant PFAS target. "
                "Claros has differentiated capability for TFA treatment. "
                "This is a commercial opportunity — lead with TFA capability in customer discussions."
            ),
        ))
        treatment_implications.append(
            "TFA detected — highlight Claros differentiated TFA treatment capability to customer."
        )
        operating_scenarios.append(
            "TFA treatment scenario: Confirm TFA concentration and target. "
            "Reference Claros TFA performance data in proposal."
        )

    # ── R4: PFSA Kinetics (priority 4) ──────────────────────────────────────
    # PFSA fraction in Primary Set species
    primary_species_data = [s for s in module1.species if s.in_primary_set and s.detected]
    primary_total = sum(s.conc_mg_L for s in primary_species_data)
    pfsa_in_primary_conc = sum(s.conc_mg_L for s in primary_species_data if is_pfsa_sulfonate(s.name))
    pfsa_pct_of_primary = (pfsa_in_primary_conc / primary_total * 100) if primary_total > 0 else 0.0

    if pfsa_pct_of_primary > PFSA_KINETICS_PCT:
        pfsa_names = [s.name for s in primary_species_data if is_pfsa_sulfonate(s.name)]
        flags.append(FlagItem(
            severity="technical", rule_id="R4",
            message=f"PFSA fraction in Primary Set: {pfsa_pct_of_primary:.1f}% (threshold: {PFSA_KINETICS_PCT}%). Reaction rate may be impacted.",
            detail=(
                f"PFSA species in Primary Set: {', '.join(pfsa_names)}. "
                "Perfluoroalkyl sulfonates (PFSA) exhibit slower reaction kinetics than PFCA "
                "in electrochemical and reductive treatment processes due to the stronger C–S bond. "
                "Account for extended contact time or adjusted reagent dosing in design."
            ),
        ))
        operating_scenarios.append(
            f"PFSA-heavy composition ({pfsa_pct_of_primary:.0f}%): "
            "evaluate extended HRT or higher energy input for electrochemical treatment."
        )

    # ── R5: Short Telomer Pathway (priority 5) ───────────────────────────────
    short_telomers_found = []
    for s in module1.species:
        if s.detected:
            is_telo, m_val = is_short_telomer(s.name)
            if is_telo and m_val is not None and m_val < 4:
                short_telomers_found.append((s.name, m_val))

    if short_telomers_found:
        telo_str = ", ".join(f"{n} (m={m})" for n, m in short_telomers_found)
        flags.append(FlagItem(
            severity="pathway", rule_id="R5",
            message=f"Short fluorotelomer(s) detected (m < 4): {telo_str}. Oxidation often preferred over reduction.",
            detail=(
                "Short-chain fluorotelomer sulfonates/carboxylates with m < 4 are more amenable "
                "to oxidative pathways (AOP, electrochemical oxidation) than reductive pathways. "
                "Design treatment pathway accordingly."
            ),
        ))
        operating_scenarios.append(
            "Short telomer pathway: prefer oxidative AOP over reductive treatment for these species."
        )

    # ── R6: Ether Carboxylate (priority 6) ───────────────────────────────────
    # Trigger: ether carboxylate detected AND not 2+2 PFESA
    # Spec ether_carboxylate group: HFPO-DA, GenX, ADONA, F-53B
    ether_carb_in_primary = [
        s.name for s in primary_species_data
        if is_ether_carboxylate(s.name) and normalize_pfas_name(s.name) != "2+2 PFESA"
    ]
    # Also check keyword detections for ether carboxylates (not via 2+2 PFESA rule)
    ether_carb_from_kw = [
        k for k in kw
        if is_ether_carboxylate(k)
        and normalize_pfas_name(k) != "2+2 PFESA"
        and k.upper() not in {e.upper() for e in PFESA_2PLUS2_EXCLUDES}
    ]

    all_ether_carb = list(dict.fromkeys(ether_carb_in_primary + ether_carb_from_kw))
    if all_ether_carb:
        flags.append(FlagItem(
            severity="special_handling", rule_id="R6",
            message=f"Ether PFAS (carboxylate) detected: {', '.join(all_ether_carb)}. No immediate composition-based concern.",
            detail=(
                "HFPO-DA (GenX), ADONA, F-53B and similar ether-carboxylate PFAS do not trigger "
                "a critical reactivity flag based on composition alone. "
                "Monitor for treatment performance — some ether-PFAS may have different kinetics. "
                "If F-53B is present, note that it is NOT classified as 2+2 PFESA."
            ),
        ))

    # ── R99: Proceed Condition (priority 99) ─────────────────────────────────
    # Trigger: ALL primary species are PFCA AND total PFAS < 50 ppm
    all_pfca = is_pfca_only(primary_set) if primary_set else False
    total_under_50ppm = module1.total_conc_mg_L < PROCEED_TOTAL_MAX_MG_L

    if all_pfca and total_under_50ppm and status not in ("CRITICAL",):
        flags.append(FlagItem(
            severity="ok", rule_id="R99",
            message=(
                f"No composition-based concern. Proceed. "
                f"(All Primary Set species are PFCA | "
                f"total PFAS {format_conc_auto(module1.total_conc_mg_L)} < 50 ppm)"
            ),
            detail="PFCA-only composition below concentration threshold. Standard treatment pathway applicable.",
        ))
        treatment_implications.append(
            "PFCA-only composition with low total concentration — standard treatment applicable."
        )
        status = "PROCEED"
    elif not flags or all(f.severity in ("info", "ok") for f in flags):
        # No flags triggered other than info/ok
        status = "INFORMATIONAL"

    # Default treatment note if no implications set
    if not treatment_implications:
        treatment_implications.append(
            "No composition-based treatment barriers identified from available PFAS data."
        )

    return Module2Result(
        sample_name=module1.sample_name,
        flags=flags,
        treatment_implications=treatment_implications,
        operating_scenarios=operating_scenarios,
        status_contribution=status,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 3 — WATER MATRIX SCREENING
# ═══════════════════════════════════════════════════════════════════════════════

_PARAM_NICE = {
    "COD":          "COD (Chemical Oxygen Demand)",
    "TOC":          "TOC (Total Organic Carbon)",
    "DOC":          "DOC (Dissolved Organic Carbon)",
    "nitrate":      "Nitrate (NO₃⁻)",
    "NO2":          "Nitrite (NO₂⁻)",
    "UV254":        "UV₂₅₄ Absorbance",
    "UVT254":       "UV₂₅₄ Transmittance",
    "chloride":     "Chloride (Cl⁻)",
    "fluoride":     "Fluoride (F⁻)",
    "hardness":     "Total Hardness",
    "TDS":          "TDS (Total Dissolved Solids)",
    "sulfate":      "Sulfate (SO₄²⁻)",
    "pH":           "pH",
    "turbidity":    "Turbidity",
    "TSS":          "TSS (Total Suspended Solids)",
    "temperature":  "Temperature",
    "flow_rate":    "Flow Rate",
    "sample_color": "Sample Color",
}
_PARAM_UNITS = {
    "COD": "mg/L", "TOC": "mg/L", "DOC": "mg/L",
    "nitrate": "mg/L", "NO2": "mg/L",
    "UV254": "cm⁻¹", "UVT254": "%",
    "chloride": "mg/L", "fluoride": "mg/L",
    "hardness": "mg/L as CaCO₃", "TDS": "mg/L",
    "sulfate": "mg/L", "pH": "", "turbidity": "NTU",
    "TSS": "mg/L", "temperature": "°C", "flow_rate": "(raw)",
    "sample_color": "",
}


def _no3_as_ion(val: float, label: str) -> float:
    """Convert nitrate-as-N to nitrate-as-ion if needed (spec: NO3_asN * 4.43)."""
    if label == "mg/L-N":
        return val * 4.43
    return val


def _no2_as_ion(val: float, label: str) -> float:
    """Convert nitrite-as-N to nitrite-as-ion if needed (spec: NO2_asN * 3.29)."""
    if label == "mg/L-N":
        return val * 3.29
    return val


def run_module3(matrix_params: Dict[str, Any]) -> Module3Result:
    """
    Spec rules:

    M3_R1 — Organics Check:
        COD ≤ 250 mg/L AND TOC ≤ 100 mg/L
        If exceeded: pretreatment required, extend timeline to ~8 weeks.

    M3_R2 — Nitrate/Nitrite Inhibition:
        Tiers (mg/L as ion):
          manageable: < 1 mg/L
          moderate:   1–20 mg/L  → adjust reagent ratio
          high:       > 20 mg/L  → pretreatment required
        Use worst-case of NO3 and NO2.

    M3_R3 — Chloride Corrosion:
        Chloride > 1000 mg/L → engineering corrosion concern.

    M3_R4 — Fluoride TOF:
        Fluoride > 100 mg/L → TOF quantification uncertainty.

    M3_R5 — Hardness Precipitation:
        Hardness > 100 mg/L as CaCO3 → precipitation during caustic dosing.

    Required inputs: (COD or TOC) AND (Nitrate or Nitrite)
    """
    flags: List[FlagItem] = []
    status = "PROCEED"

    detected = {k: v for k, v in matrix_params.items()
                if v is not None and str(v).strip() not in ("", "nan")}

    # ── Required information check ────────────────────────────────────────────
    has_organics  = ("COD" in detected) or ("TOC" in detected) or ("DOC" in detected)
    has_nitrogen  = ("nitrate" in detected) or ("NO2" in detected)
    missing_required: List[str] = []
    if not has_organics:
        missing_required.append("COD or TOC (required for organics screening)")
    if not has_nitrogen:
        missing_required.append("Nitrate or Nitrite (required for inhibition screening)")

    all_key_params = ["COD", "TOC", "nitrate", "NO2", "UV254", "UVT254", "chloride", "fluoride", "hardness"]
    missing_all = [p for p in all_key_params if p not in detected]

    # ── M3_R1: Organics Check ────────────────────────────────────────────────
    cod = detected.get("COD")
    toc = detected.get("TOC") or detected.get("DOC")  # DOC as fallback

    if cod is not None and toc is not None:
        # Both present — both must pass
        if cod > M3_COD_MAX_MG_L or toc > M3_TOC_MAX_MG_L:
            exceeded = []
            if cod > M3_COD_MAX_MG_L:
                exceeded.append(f"COD {cod:.0f} mg/L > {M3_COD_MAX_MG_L:.0f} mg/L")
            if toc > M3_TOC_MAX_MG_L:
                exceeded.append(f"TOC {toc:.0f} mg/L > {M3_TOC_MAX_MG_L:.0f} mg/L")
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R1",
                message=f"Organic load high: {' | '.join(exceeded)}. Pretreatment likely required.",
                detail=(
                    "High organic loading will compete with PFAS in oxidative treatment systems "
                    "and increase reagent consumption. "
                    "Pre-treatment (e.g., coagulation, sedimentation, biological pre-treatment) required. "
                    "Extend project timeline to approximately 8 weeks."
                ),
            ))
            status = "CONDITIONAL"
        else:
            flags.append(FlagItem(
                severity="ok", rule_id="M3_R1",
                message=f"Organics within acceptable range — COD: {cod:.0f} mg/L, TOC: {toc:.0f} mg/L.",
            ))
    elif cod is not None:
        if cod > M3_COD_MAX_MG_L:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R1",
                message=f"COD high: {cod:.0f} mg/L > {M3_COD_MAX_MG_L:.0f} mg/L. Pretreatment likely required.",
                detail="Extend project timeline to ~8 weeks. Pretreatment design required.",
            ))
            status = "CONDITIONAL"
        else:
            flags.append(FlagItem(
                severity="ok", rule_id="M3_R1",
                message=f"COD within acceptable range: {cod:.0f} mg/L.",
            ))
    elif toc is not None:
        if toc > M3_TOC_MAX_MG_L:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R1",
                message=f"TOC high: {toc:.0f} mg/L > {M3_TOC_MAX_MG_L:.0f} mg/L. Pretreatment likely required.",
                detail="Extend project timeline to ~8 weeks. Pre-treatment design required.",
            ))
            status = "CONDITIONAL"
        else:
            flags.append(FlagItem(
                severity="ok", rule_id="M3_R1",
                message=f"TOC within acceptable range: {toc:.0f} mg/L.",
            ))

    # ── M3_R2: Nitrate/Nitrite Inhibition ────────────────────────────────────
    # Spec: use worst-case between NO3 and NO2 (as ion, mg/L)
    no3_raw = detected.get("nitrate")
    no2_raw = detected.get("NO2")

    # Convert as-N values if stored with unit tag
    no3_val = float(no3_raw) if no3_raw is not None else None
    no2_val = float(no2_raw) if no2_raw is not None else None

    worst_nitrogen = None
    nitrogen_label = ""
    if no3_val is not None and no2_val is not None:
        if no3_val >= no2_val:
            worst_nitrogen, nitrogen_label = no3_val, f"NO₃⁻ {no3_val:.1f} mg/L"
        else:
            worst_nitrogen, nitrogen_label = no2_val, f"NO₂⁻ {no2_val:.1f} mg/L"
    elif no3_val is not None:
        worst_nitrogen, nitrogen_label = no3_val, f"NO₃⁻ {no3_val:.1f} mg/L"
    elif no2_val is not None:
        worst_nitrogen, nitrogen_label = no2_val, f"NO₂⁻ {no2_val:.1f} mg/L"

    if worst_nitrogen is not None:
        if worst_nitrogen < M3_NITRATE_MANAGEABLE:
            flags.append(FlagItem(
                severity="ok", rule_id="M3_R2",
                message=f"Nitrate/Nitrite competition impact manageable — {nitrogen_label} (< {M3_NITRATE_MANAGEABLE} mg/L).",
            ))
        elif worst_nitrogen <= M3_NITRATE_HIGH:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R2",
                message=f"Moderate nitrate/nitrite — {nitrogen_label}. Adjust reagent ratio to overcome inhibition.",
                detail=(
                    "Nitrogen species compete with PFAS in oxidative treatment systems. "
                    "At moderate levels (1–20 mg/L), reagent ratio adjustment is typically sufficient. "
                    "Validate with bench-scale testing."
                ),
            ))
            status = "CONDITIONAL"
        else:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R2",
                message=f"High nitrate/nitrite — {nitrogen_label} (> {M3_NITRATE_HIGH} mg/L). Pretreatment required.",
                detail=(
                    "High nitrogen loading will significantly inhibit PFAS oxidative treatment efficiency. "
                    "Electrochemical nitrate reduction or biological denitrification pre-treatment required "
                    "before PFAS treatment."
                ),
            ))
            status = "CONDITIONAL"

    # ── M3_R3: Chloride Corrosion ─────────────────────────────────────────────
    if (cl := detected.get("chloride")) is not None:
        cl = float(cl)
        if cl > M3_CHLORIDE_CORROSION:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R3",
                message=f"Chloride {cl:.0f} mg/L > {M3_CHLORIDE_CORROSION:.0f} mg/L. Engineering corrosion concern.",
                detail=(
                    "High chloride concentrations increase corrosion risk for stainless steel and titanium "
                    "electrochemical reactor components. "
                    "Specify corrosion-resistant materials (Hastelloy, PTFE-lined) in system design."
                ),
            ))
            status = "CONDITIONAL"

    # ── M3_R4: Fluoride TOF Uncertainty ──────────────────────────────────────
    if (fl := detected.get("fluoride")) is not None:
        fl = float(fl)
        if fl > M3_FLUORIDE_TOF:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R4",
                message=f"Fluoride {fl:.0f} mg/L > {M3_FLUORIDE_TOF:.0f} mg/L. TOF quantification uncertainty likely increased.",
                detail=(
                    "High background fluoride interferes with Total Oxidizable Fluorine (TOF) "
                    "analytical quantification. "
                    "Consult analytical lab regarding matrix-matched calibration and "
                    "reporting detection limits. "
                    "TOF-based treatment verification may require alternative confirmation method."
                ),
            ))
            status = "CONDITIONAL"

    # ── M3_R5: Hardness Precipitation ────────────────────────────────────────
    if (hard := detected.get("hardness")) is not None:
        hard = float(hard)
        if hard > M3_HARDNESS_PRECIP:
            flags.append(FlagItem(
                severity="warning", rule_id="M3_R5",
                message=f"Hardness {hard:.0f} mg/L as CaCO₃ > {M3_HARDNESS_PRECIP:.0f} mg/L. Precipitation likely during caustic dosing.",
                detail=(
                    "High calcium/magnesium hardness will cause carbonate and hydroxide precipitation "
                    "when caustic (NaOH) is dosed for pH adjustment in oxidative treatment systems. "
                    "Softening pre-treatment (lime softening or IX) recommended. "
                    "Alternatively, design for periodic reactor cleaning."
                ),
            ))
            status = "CONDITIONAL"

    # ── Missing required params flag ──────────────────────────────────────────
    if missing_required:
        flags.append(FlagItem(
            severity="warning", rule_id="M3_MISSING",
            message=f"Required matrix parameters not provided: {' | '.join(missing_required)}",
            detail=(
                "Per spec requirements, COD or TOC AND nitrate or nitrite are required inputs. "
                "Request these from the customer before proceeding with feasibility assessment."
            ),
        ))
        status = "CONDITIONAL"

    # ── Informational: UV254 / UVT ────────────────────────────────────────────
    if "UV254" in detected or "UVT254" in detected:
        uv_str = (
            f"UV254={detected['UV254']} cm⁻¹" if "UV254" in detected else ""
        ) + (
            f" UVT254={detected['UVT254']}%" if "UVT254" in detected else ""
        )
        flags.append(FlagItem(
            severity="info", rule_id="M3_UV",
            message=f"UV optical data available: {uv_str.strip()}",
            detail="UV data supports organic characterisation and UV-AOP design if applicable.",
        ))

    # ── Clean matrix signal ───────────────────────────────────────────────────
    if status == "PROCEED" and detected and not missing_required:
        flags.append(FlagItem(
            severity="ok", rule_id="M3_CLEAN",
            message="Water matrix parameters within acceptable ranges. No treatment-limiting factors identified.",
        ))

    return Module3Result(
        detected_params=detected,
        missing_params=missing_all,
        missing_required_params=missing_required,
        flags=flags,
        status_contribution=status,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-SAMPLE VARIABILITY  (spec: M1 detect_multi_sample_variability)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_variability(
    sample_results: List[SampleResult],
) -> Tuple[Optional[float], Optional[FlagItem]]:
    """
    Spec: max(total_pfas) / min(total_pfas)
    Flag if ratio >= 10.
    """
    totals = [sr.module1.total_conc_mg_L for sr in sample_results
              if sr.module1.total_conc_mg_L > 0]
    if len(totals) < 2:
        return None, None

    ratio = max(totals) / min(totals)
    if ratio >= VARIABILITY_HIGH_THRESHOLD:
        flag = FlagItem(
            severity="warning", rule_id="M1-VAR",
            message=(
                f"High multi-sample variability: max/min total PFAS ratio = {ratio:.1f} "
                f"(threshold: {VARIABILITY_HIGH_THRESHOLD:.0f})"
            ),
            detail=(
                "Large variation in total PFAS across samples may indicate temporal or spatial "
                "concentration variability, different source zones, or sampling anomalies. "
                "Use worst-case sample as design basis. Investigate source characterisation."
            ),
        )
        return ratio, flag
    return ratio, None


# ═══════════════════════════════════════════════════════════════════════════════
# OVERALL STATUS (spec: final_status_logic)
# ═══════════════════════════════════════════════════════════════════════════════

def _determine_status(
    sample_results: List[SampleResult],
    module3: Module3Result,
    variability_flag: Optional[FlagItem],
    has_pfas_data: bool,
) -> Tuple[str, List[str]]:
    """
    Spec final_status_logic:
      if any CRITICAL:                          status = CRITICAL
      else if matrix_high_or_missing_required:  status = CONDITIONAL
      else:                                     status = PROCEED
    """
    reasons: List[str] = []

    if not has_pfas_data and not sample_results:
        reasons.append("No PFAS concentration data — assessment based on keyword/text only.")
        return "CONDITIONAL", reasons

    # Collect all critical flags from M2 (M1 and M3 don't produce CRITICAL in spec)
    critical_msgs = [
        f.message for sr in sample_results
        for f in sr.module2.flags if f.severity == "critical"
    ]
    sample_critical = [sr.sample_status for sr in sample_results if sr.sample_status == "CRITICAL"]

    if critical_msgs or sample_critical:
        reasons += critical_msgs[:3]
        return "CRITICAL", reasons

    # Matrix CONDITIONAL
    if module3.status_contribution == "CONDITIONAL":
        warning_flags = [f.message for f in module3.flags if f.severity == "warning"]
        reasons += warning_flags[:3]
        if module3.missing_required_params:
            reasons.append(f"Required matrix data missing: {', '.join(module3.missing_required_params)}")
        return "CONDITIONAL", reasons

    # Variability
    if variability_flag:
        reasons.append(variability_flag.message)
        return "CONDITIONAL", reasons

    # PROCEED
    reasons.append(
        "No critical reactivity flags identified. "
        "Water matrix within acceptable ranges. "
        "Proceed with standard evaluation pathway."
    )
    return "PROCEED", reasons


def _per_sample_status(m1: Module1Result, m2: Module2Result) -> str:
    if any(f.severity == "critical" for f in m2.flags):
        return "CRITICAL"
    if m2.status_contribution == "CRITICAL":
        return "CRITICAL"
    if any(f.severity == "warning" for f in m1.flags + m2.flags):
        return "CONDITIONAL"
    return "PROCEED"


# ═══════════════════════════════════════════════════════════════════════════════
# MISSING INFO TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

def _collect_missing_info(parsed: ParsedData, module3: Module3Result) -> List[str]:
    missing: List[str] = []
    if not parsed.has_pfas_data:
        missing.append("PFAS concentration data (Excel table or PDF lab report with quantified results)")
    for p in module3.missing_required_params:
        missing.append(p)
    if not parsed.treatment_goals_text.strip():
        missing.append("Treatment objectives: target concentration limits, regulatory driver, target analytes")
    if "flow_rate" not in parsed.matrix_params:
        missing.append("Site flow rate / design volume (required for sizing and cost estimation)")
    return missing


# ═══════════════════════════════════════════════════════════════════════════════
# TREATMENT SUMMARY BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _build_treatment_summary(
    sample_results: List[SampleResult],
    module3: Module3Result,
) -> List[str]:
    seen: set = set()
    summary: List[str] = []
    for sr in sample_results:
        for item in sr.module2.treatment_implications:
            if item not in seen:
                summary.append(item)
                seen.add(item)
    # Matrix-driven additions
    dp = module3.detected_params
    if dp.get("COD", 0) > M3_COD_MAX_MG_L or dp.get("TOC", 0) > M3_TOC_MAX_MG_L:
        note = "Organic pre-treatment required before PFAS treatment unit. Extend project timeline ~8 weeks."
        if note not in seen:
            summary.append(note)
    if dp.get("hardness", 0) > M3_HARDNESS_PRECIP:
        note = "Softening pre-treatment required to prevent precipitation during caustic dosing."
        if note not in seen:
            summary.append(note)
    return summary or ["Refer to technical output for technology-specific guidance."]


# ═══════════════════════════════════════════════════════════════════════════════
# BUSINESS EMAIL DRAFT
# ═══════════════════════════════════════════════════════════════════════════════

def _generate_email_draft(data: dict) -> str:
    overall_status: str = data["overall_status"]
    sample_results: List[SampleResult] = data["sample_results"]
    module3: Module3Result = data["module3"]
    missing_info: List[str] = data["missing_info"]
    treatment_summary: List[str] = data["treatment_summary"]
    data_sources: List[str] = data["data_sources"]
    goals_text: str = data.get("goals_text", "")
    variability_flag: Optional[FlagItem] = data.get("variability_flag")

    today = date.today().strftime("%B %d, %Y")

    # Worst-case sample
    worst: Optional[SampleResult] = None
    if sample_results:
        worst = max(
            sample_results,
            key=lambda sr: {"CRITICAL": 2, "CONDITIONAL": 1, "PROCEED": 0}.get(sr.sample_status, 0),
        )

    # PFAS profile lines
    pfas_lines: List[str] = []
    if worst and worst.module1.total_conc_mg_L > 0:
        m1 = worst.module1
        pfas_lines.append(f"  • Total PFAS (worst-case sample): {format_conc_auto(m1.total_conc_mg_L)}")
        top_str = ", ".join(
            f"{s.name} ({format_pct(s.percentage)})"
            for s in m1.species[:4] if s.detected
        )
        if top_str:
            pfas_lines.append(f"  • Top species: {top_str}")
        pfas_lines.append(f"  • Top-5 cumulative: {format_pct(m1.top5_cumulative_pct)} | Other fraction: {format_pct(m1.other_fraction_pct)}")
        for cat, frac in sorted(m1.category_fractions.items(), key=lambda x: -x[1]):
            if frac > 0.01:
                pfas_lines.append(f"    – {CATEGORY_LABELS.get(cat, cat)}: {format_pct(frac * 100)}")

    # Key flags
    key_flags: List[str] = []
    seen_f: set = set()
    for sr in sample_results:
        for f in sr.module2.flags:
            if f.severity in ("critical", "commercial", "technical", "pathway") and f.message not in seen_f:
                prefix = {
                    "critical": "[CRITICAL]",
                    "commercial": "[COMMERCIAL]",
                    "technical": "[TECHNICAL]",
                    "pathway": "[PATHWAY]",
                }.get(f.severity, "[FLAG]")
                key_flags.append(f"  • {prefix} {f.message}")
                seen_f.add(f.message)
    for f in module3.flags:
        if f.severity == "warning" and f.message not in seen_f:
            key_flags.append(f"  • [MATRIX] {f.message}")
            seen_f.add(f.message)

    status_note = {
        "PROCEED":     "No critical treatment barriers identified. Standard evaluation pathway applicable.",
        "CONDITIONAL": "Conditional — matrix screening or missing data requires resolution before technology selection.",
        "CRITICAL":    "CRITICAL — one or more species cannot be treated. Confirm treatment scope with customer.",
    }.get(overall_status, "")

    src_str = ", ".join(data_sources) if data_sources else "provided customer materials"
    n_samples = len(sample_results)

    email = f"""SUBJECT: PFAS Treatment Feasibility Screening — {today}

Hi Team,

Preliminary PFAS treatment feasibility screening completed based on {src_str} ({n_samples} sample(s)).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OVERALL ASSESSMENT: {overall_status}
{status_note}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PFAS PROFILE SUMMARY
{chr(10).join(pfas_lines) if pfas_lines else "  • No quantified PFAS data available."}
"""
    if variability_flag:
        email += f"\n  • ⚠ {variability_flag.message}\n"

    email += f"""
KEY REACTIVITY FLAGS
{chr(10).join(key_flags) if key_flags else "  • No critical or commercial flags."}

TREATMENT / COMMERCIAL PATHWAY
{chr(10).join(f"  • {i}" for i in treatment_summary[:4])}
"""

    if goals_text:
        email += f"""
CUSTOMER TREATMENT OBJECTIVES
  {goals_text[:300]}{"..." if len(goals_text) > 300 else ""}
"""

    if missing_info:
        email += f"""
OUTSTANDING INFORMATION REQUIRED
{chr(10).join(f"  • {m}" for m in missing_info)}

Please follow up with customer to obtain the above before proceeding.
"""

    email += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Preliminary screening only — not a substitute for detailed engineering design.
Full technical output available on request.

Claros R&D Team | PFAS Evaluation Engine v1.0
"""
    return email.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENGINE ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate(parsed: ParsedData) -> EvaluationResult:
    """Run the full PFAS evaluation pipeline per spec."""
    logs: List[str] = list(parsed.logs)
    logs.append("=== Engine started (spec-aligned v1.0) ===")

    data_sources: List[str] = []
    if parsed.has_excel:
        data_sources.append("Excel file")
    if parsed.has_pdf:
        data_sources.append("PDF lab report")
    if parsed.has_text:
        data_sources.append("pasted text / notes")

    # ── Module 1 + 2 per sample ──────────────────────────────────────────────
    sample_results: List[SampleResult] = []

    pfas_source = parsed.pfas_samples if parsed.pfas_samples else {}
    if pfas_source:
        for sample_name, pfas_data in pfas_source.items():
            logs.append(f"[M1] Sample '{sample_name}': {len(pfas_data)} analytes")
            m1 = run_module1(sample_name, pfas_data)
            m2 = run_module2(
                m1,
                keyword_species=parsed.keyword_species,
                goals_text=parsed.treatment_goals_text,
                email_text=parsed.customer_notes_text,
            )
            s_status = _per_sample_status(m1, m2)
            sample_results.append(SampleResult(sample_name, m1, m2, s_status))
            logs.append(f"[M2] Sample '{sample_name}' → {s_status}")
    else:
        # Keyword-only path
        logs.append("[Engine] No concentration data — keyword-only evaluation")
        dummy_m1 = Module1Result(
            sample_name="(no quantified data)", total_conc_mg_L=0.0,
            species=[], primary_set=[], top5=[],
            top5_cumulative_pct=0.0, other_fraction_pct=100.0,
            category_fractions={},
            flags=[FlagItem("warning", "M1-NODATA", "No PFAS concentration data provided.", "")],
        )
        dummy_m2 = run_module2(
            dummy_m1,
            keyword_species=parsed.keyword_species,
            goals_text=parsed.treatment_goals_text,
            email_text=parsed.customer_notes_text,
        )
        dummy_status = _per_sample_status(dummy_m1, dummy_m2)
        sample_results.append(SampleResult("(no quantified data)", dummy_m1, dummy_m2, dummy_status))

    # ── Multi-sample variability ──────────────────────────────────────────────
    variability_ratio, variability_flag = _compute_variability(sample_results)
    if variability_ratio is not None:
        logs.append(f"[M1-VAR] Variability ratio: {variability_ratio:.2f}")

    # ── Module 3 ──────────────────────────────────────────────────────────────
    logs.append(f"[M3] {len(parsed.matrix_params)} matrix parameters")
    module3 = run_module3(parsed.matrix_params)
    logs.append(f"[M3] Status: {module3.status_contribution}")

    # ── Overall status ────────────────────────────────────────────────────────
    overall_status, status_reasons = _determine_status(
        sample_results, module3, variability_flag, parsed.has_pfas_data
    )
    logs.append(f"[Engine] Overall: {overall_status}")

    # ── Outputs ───────────────────────────────────────────────────────────────
    missing_info      = _collect_missing_info(parsed, module3)
    treatment_summary = _build_treatment_summary(sample_results, module3)
    email_draft       = _generate_email_draft({
        "overall_status":   overall_status,
        "sample_results":   sample_results,
        "module3":          module3,
        "missing_info":     missing_info,
        "treatment_summary": treatment_summary,
        "data_sources":     data_sources,
        "goals_text":       parsed.treatment_goals_text,
        "variability_flag": variability_flag,
    })

    logs.append("=== Engine complete ===")

    return EvaluationResult(
        samples=sample_results,
        module3=module3,
        overall_status=overall_status,
        status_reasons=status_reasons,
        missing_info=missing_info,
        treatment_summary=treatment_summary,
        email_draft=email_draft,
        logs=logs,
        has_pfas_data=parsed.has_pfas_data,
        data_sources=data_sources,
        variability_ratio=variability_ratio,
        variability_flag=variability_flag,
    )
