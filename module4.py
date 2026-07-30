"""
module4.py — Bench Test Plan Generator (Proof-of-Concept stage)

Generates a deterministic bench-scale test plan for each water sample, driven by
the Module 1-3 evaluation results. No ML, no generative inference.

Standard bench system: 750 mL, Amazon Reactor.

POC philosophy: the FIRST test proves treatment efficacy — it is NOT an
optimization sweep. Each plan therefore proposes two conditions:
  A) Reference : standard 10 mM sulfite + 2 mM iodide (always kept as a control)
  B) Water-adjusted : sulfite scaled by PFAS load + nitrate/nitrite electron demand

All outputs are recommendations — the UI renders them as editable fields.

Claros R&D Team | Framework Architecture by Zack Liu
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from utils import get_pfas_f_fraction, is_pfca_only, format_conc_auto
from engine import EvaluationResult, SampleResult, Module3Result
from parser import ParsedData


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS  (confirmed with Claros R&D)
# ═══════════════════════════════════════════════════════════════════════════════

IODIDE_MM_DEFAULT      = 2.0     # Reagent B — fixed for POC

# Reagent A (sulfite) base tiers by total PFAS (ppm ≈ mg/L)
SULFITE_BASE_LOW       = 10.0    # total PFAS < 20 ppm
SULFITE_BASE_MID       = 30.0    # 20–50 ppm
SULFITE_BASE_HIGH      = 50.0    # > 50 ppm
PFAS_TIER_MID_PPM      = 20.0
PFAS_TIER_HIGH_PPM     = 50.0

# Nitrate / nitrite → additional sulfite (electron-balance stoichiometry).
# Reduction to ammonia: NO3⁻ needs 8 e⁻, NO2⁻ needs 6 e⁻. 1 sulfite → 1 e⁻.
NITRATE_ELECTRONS      = 8
NITRITE_ELECTRONS      = 6
NITRATE_MW             = 62.0    # NO3⁻ g/mol (as ion)
NITRITE_MW             = 46.0    # NO2⁻ g/mol (as ion)
N_COMPENSATION_MIN_PPM = 1.0     # below this, no adjustment (manageable)
NITRATE_ELECTROCHEM_PPM = 20.0   # > this → also recommend electrochemical pretreatment

REACTION_PH            = 12.0
PFAS_MIDDOSE_PH_PPM    = 20.0    # > this → "consider" mid-run pH re-dose (optimization)

NOVEM_TOF_DL_MG_L         = 1.0   # Novem external TOF detection limit = 1 ppm (as F)
NOVEM_TOF_BORDERLINE_MG_L = 0.5   # theoretical TOF 0.5–1 ppm → borderline (lower-bound estimate;
                                  # actual organofluorine may exceed DL) — discuss before deciding
FLUORIDE_HANDLING_MG_L = 100.0   # high fluoride → Novem background note + pH>12 / HF caution

THROUGHPUT_HIGH_GPM        = 100.0   # ≥ this → prefer short / dense-first sampling
THROUGHPUT_DILUTION_MAX_GPM = 1.0    # > this → never propose dilution

COD_MAX_MG_L           = 250.0   # aligned with engine M3_R1
TSS_FILTER_MG_L        = 50.0    # solids → filtration (assumption, adjustable)

# Sampling schedules
SHORT_TIMEPOINTS      = ["0", "5 min", "10 min", "15 min", "30 min", "45 min", "60 min", "2 h"]
LONG_TIMEPOINTS       = ["0", "1 h", "2 h", "4 h", "6 h", "24 h"]
LONG_DENSE_TIMEPOINTS = ["0", "5 min", "10 min", "15 min", "30 min", "45 min", "60 min",
                         "2 h", "4 h", "6 h", "24 h"]

_COLORLESS = {"", "nan", "none", "clear", "colorless", "colourless", "0", "not detected", "nd"}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TestCondition:
    label: str
    sulfite_mM: float
    iodide_mM: float
    note: str = ""


@dataclass
class TestPlan:
    sample_name: str
    total_pfas_mg_L: float
    conditions: List[TestCondition]
    schedule_type: str                 # "short" | "long" | "long_dense"
    schedule_timepoints: List[str]
    schedule_rationale: str
    pretreatment: List[str]
    dilution: str
    reaction_ph: float
    ph_monitoring: str
    external_tof: str
    external_tof_needed: bool
    fluoride_handling: str
    customer_info_requests: List[str] = field(default_factory=list)
    optimization_notes: List[str] = field(default_factory=list)
    general_notes: List[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _r4_triggered(sr: SampleResult) -> bool:
    """PFSA-heavy (slow kinetics) — Module 2 rule R4 raised."""
    return any(f.rule_id == "R4" for f in sr.module2.flags)


def _has_fast_species(sr: SampleResult) -> bool:
    """Any detected PFCA (fast-reacting) present."""
    return any(is_pfca_only([s.name]) for s in sr.module1.species if s.detected and s.conc_mg_L > 0)


def _sulfite_base_mM(ppm: float) -> float:
    if ppm < PFAS_TIER_MID_PPM:
        return SULFITE_BASE_LOW
    if ppm <= PFAS_TIER_HIGH_PPM:
        return SULFITE_BASE_MID
    return SULFITE_BASE_HIGH


def _get_param(m3: Module3Result, key: str) -> Optional[float]:
    v = m3.detected_params.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _nitrogen_sulfite_addition(m3: Module3Result) -> Tuple[float, List[str]]:
    """
    Additional sulfite (mM) to offset electron demand from reducing nitrate/nitrite
    to ammonia. Returns (added_mM, explanation_parts).
    """
    added = 0.0
    parts: List[str] = []
    no3 = _get_param(m3, "nitrate")
    no2 = _get_param(m3, "NO2")
    if no3 is not None and no3 >= N_COMPENSATION_MIN_PPM:
        term = NITRATE_ELECTRONS * (no3 / NITRATE_MW)
        added += term
        parts.append(f"NO₃⁻ {no3:.0f} mg/L → +{term:.1f} mM")
    if no2 is not None and no2 >= N_COMPENSATION_MIN_PPM:
        term = NITRITE_ELECTRONS * (no2 / NITRITE_MW)
        added += term
        parts.append(f"NO₂⁻ {no2:.0f} mg/L → +{term:.1f} mM")
    return added, parts


def _theoretical_tof_mg_L(sr: SampleResult) -> float:
    """Sum of fluorine contributions from identified species (mg F/L)."""
    if sr.tof_result is not None:
        return sr.tof_result.theoretical_tof_mg_L
    total = 0.0
    for s in sr.module1.species:
        if s.detected and s.conc_mg_L > 0:
            ff = get_pfas_f_fraction(s.name)
            if ff:
                total += s.conc_mg_L * ff
    return total


# ═══════════════════════════════════════════════════════════════════════════════
# PLAN GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def _generate_one_plan(
    sr: SampleResult,
    m3: Module3Result,
    project_ctx: Dict,
) -> TestPlan:
    ppm = sr.module1.total_conc_mg_L                       # mg/L ≈ ppm
    throughput = None
    try:
        throughput = float(project_ctx.get("throughput_gpm") or 0) or None
    except (TypeError, ValueError):
        throughput = None
    high_throughput = throughput is not None and throughput > THROUGHPUT_HIGH_GPM

    # ── Reagent A (sulfite) ──────────────────────────────────────────────────
    base = _sulfite_base_mM(ppm)
    add, add_parts = _nitrogen_sulfite_addition(m3)
    calc_sulfite = round(base + add, 1)

    cond_ref = TestCondition("A · Reference (standard 10+2)", SULFITE_BASE_LOW, IODIDE_MM_DEFAULT,
                             note="Standard baseline / control.")
    conditions = [cond_ref]
    if abs(calc_sulfite - SULFITE_BASE_LOW) > 0.1:
        note_bits = [f"base {base:.0f} mM (PFAS {format_conc_auto(sr.module1.total_conc_mg_L)})"]
        if add_parts:
            note_bits.append("nitrogen offset " + " ; ".join(add_parts))
        cond_calc = TestCondition("B · Water-adjusted", calc_sulfite, IODIDE_MM_DEFAULT,
                                  note=" + ".join(note_bits))
        conditions.append(cond_calc)

    # ── Sampling schedule ────────────────────────────────────────────────────
    r4 = _r4_triggered(sr)
    fast = _has_fast_species(sr)
    if not r4:
        schedule_type = "short"
        timepoints = SHORT_TIMEPOINTS
        rationale = "No slow-reacting PFAS detected — standard short schedule (≤ 2 h)."
    else:
        if fast or high_throughput:
            schedule_type = "long_dense"
            timepoints = LONG_DENSE_TIMEPOINTS
            drivers = []
            if fast:
                drivers.append("fast + slow PFAS mixture")
            if high_throughput:
                drivers.append(f"high throughput ({throughput:.0f} GPM)")
            rationale = ("Slow-reacting PFAS present with " + " and ".join(drivers) +
                         " — long schedule (≤ 24 h) with dense sampling through the first 2 h.")
        else:
            schedule_type = "long"
            timepoints = LONG_TIMEPOINTS
            rationale = "Slow-reacting PFAS (R4) — long schedule (≤ 24 h)."

    customer_requests: List[str] = []
    if throughput is None:
        customer_requests.append(
            "Required throughput (GPM / commercial capacity) — confirms the sampling schedule."
        )

    # ── Pretreatment ─────────────────────────────────────────────────────────
    pretreatment: List[str] = []

    no3 = _get_param(m3, "nitrate")
    no2 = _get_param(m3, "NO2")
    worst_n = max(no3 or 0.0, no2 or 0.0)
    if worst_n > NITRATE_ELECTROCHEM_PPM:
        pretreatment.append(
            f"Nitrate/nitrite {worst_n:.0f} mg/L > {NITRATE_ELECTROCHEM_PPM:.0f} ppm — "
            "propose electrochemical pre-reduction (in addition to the increased sulfite dose)."
        )

    tss = _get_param(m3, "TSS")
    if tss is not None and tss > TSS_FILTER_MG_L:
        pretreatment.append(f"Suspended solids (TSS {tss:.0f} mg/L) — filter before reaction.")

    color = m3.detected_params.get("sample_color")
    if color is not None and str(color).strip().lower() not in _COLORLESS:
        pretreatment.append(
            f"Colored sample ({color}) — raise pH to 12 with base; check for metal precipitation "
            "and colour removal."
        )

    # ── COD handling + dilution ──────────────────────────────────────────────
    dilution = "None"
    cod = _get_param(m3, "COD")
    if cod is not None and cod > COD_MAX_MG_L:
        customer_requests.append(
            f"COD source (COD {cod:.0f} mg/L > {COD_MAX_MG_L:.0f}) — confirm whether it is "
            "methanol/ethanol or another organic solvent."
        )
        if throughput is not None and throughput > THROUGHPUT_DILUTION_MAX_GPM:
            pretreatment.append(
                f"COD {cod:.0f} mg/L > {COD_MAX_MG_L:.0f} — if from MeOH/EtOH proceed unchanged; "
                f"if another solvent, further evaluation required. Do NOT dilute "
                f"(throughput {throughput:.0f} GPM > {THROUGHPUT_DILUTION_MAX_GPM:.0f} GPM)."
            )
        else:
            factor = cod / COD_MAX_MG_L
            pretreatment.append(
                f"COD {cod:.0f} mg/L > {COD_MAX_MG_L:.0f} — if from MeOH/EtOH proceed unchanged; "
                "if another solvent, either further evaluation or (cautiously) dilute."
            )
            dilution = (
                f"Only if COD is from a non-alcohol solvent: dilute ~{factor:.1f}× so COD < "
                f"{COD_MAX_MG_L:.0f} ppm. Propose with caution."
            )

    if not pretreatment:
        pretreatment.append("None required — run water as received.")

    # ── pH ───────────────────────────────────────────────────────────────────
    ph_monitoring = "Measure pH at every sampling point; the final point is mandatory."
    optimization_notes: List[str] = []
    if ppm > PFAS_MIDDOSE_PH_PPM:
        optimization_notes.append(
            f"Total PFAS {format_conc_auto(sr.module1.total_conc_mg_L)} > "
            f"{PFAS_MIDDOSE_PH_PPM:.0f} ppm — consider mid-run pH re-dosing (optimization stage)."
        )

    # ── External TOF (Novem) ─────────────────────────────────────────────────
    fluoride = _get_param(m3, "fluoride")
    fluoride_handling = ""
    if fluoride is not None and fluoride > FLUORIDE_HANDLING_MG_L:
        fluoride_handling = (
            f"High inorganic fluoride background ({fluoride:.0f} mg/L): report this to Novem — "
            "they must pretreat the sample to remove background fluoride before TOF analysis. "
            "Keep the sample at pH > 12 throughout — never acidic (prevents HF formation)."
        )

    tof_reported = sr.tof_result is not None
    theo_tof = _theoretical_tof_mg_L(sr)
    if tof_reported:
        _meas = sr.tof_result.measured_mg_L
        _dl_rel = "above" if _meas > NOVEM_TOF_DL_MG_L else "below"
        if sr.tof_result.unknown_pfas_flag:
            if _meas > NOVEM_TOF_DL_MG_L:
                external_tof_needed = True
                external_tof = (
                    f"Known {sr.tof_result.measured_type} level: {format_conc_auto(_meas)} as F — "
                    f"above the Novem detection limit ({NOVEM_TOF_DL_MG_L:.0f} ppm). "
                    f"Identified species cover only {sr.tof_result.coverage_ratio * 100:.0f}% (< 50%) — "
                    "significant unknown PFAS. Send raw water to Novem for further identification (TOP assay)."
                )
            else:
                # Unknown PFAS present, but the level is below what Novem can quantify —
                # sending would return non-detects and waste the submission.
                external_tof_needed = False
                external_tof = (
                    f"Known {sr.tof_result.measured_type} level: {format_conc_auto(_meas)} as F — "
                    f"below the Novem detection limit ({NOVEM_TOF_DL_MG_L:.0f} ppm), so Novem "
                    "would not be able to quantify it — do NOT send. "
                    f"Identified species cover only {sr.tof_result.coverage_ratio * 100:.0f}% (< 50%): "
                    "the unknown-PFAS risk remains — track it through treatment performance "
                    "testing (targeted panel before/after) instead of external TOF."
                )
        else:
            external_tof_needed = False
            external_tof = (
                f"Known {sr.tof_result.measured_type} level: {format_conc_auto(_meas)} as F "
                f"({_dl_rel} the Novem detection limit of {NOVEM_TOF_DL_MG_L:.0f} ppm). "
                f"Adequately covered by identified species "
                f"({sr.tof_result.coverage_ratio * 100:.0f}%) — external TOF not required."
            )
    else:
        if theo_tof > NOVEM_TOF_DL_MG_L:
            external_tof_needed = True
            external_tof = (
                f"No customer TOF/AOF reported. Expected TOF from identified species: "
                f"{format_conc_auto(theo_tof)} as F — above the Novem detection limit "
                f"({NOVEM_TOF_DL_MG_L:.0f} ppm). Send raw water to Novem for TOF."
            )
        elif theo_tof > NOVEM_TOF_BORDERLINE_MG_L:
            # Borderline: theoretical TOF is a LOWER BOUND (unknown species excluded),
            # so the actual organofluorine load may still exceed the DL.
            external_tof_needed = True   # submission possible — keep fluoride guidance
            external_tof = (
                f"No customer TOF/AOF reported. Expected TOF from identified species: "
                f"{format_conc_auto(theo_tof)} as F — BORDERLINE: below the Novem detection "
                f"limit ({NOVEM_TOF_DL_MG_L:.0f} ppm), but this estimate is a lower bound "
                "(unknown species are not included), so the actual organofluorine load may "
                "exceed the DL. Action: discuss with Novem and Zack before deciding whether to submit."
            )
        else:
            external_tof_needed = False
            external_tof = (
                f"No customer TOF/AOF reported. Expected TOF from identified species: "
                f"{format_conc_auto(theo_tof)} as F — below the Novem detection limit "
                f"({NOVEM_TOF_DL_MG_L:.0f} ppm); external TOF would not be informative."
            )

    # Fluoride submission guidance only applies when a Novem submission is proposed
    if not external_tof_needed:
        fluoride_handling = ""

    return TestPlan(
        sample_name=sr.sample_name,
        total_pfas_mg_L=sr.module1.total_conc_mg_L,
        conditions=conditions,
        schedule_type=schedule_type,
        schedule_timepoints=timepoints,
        schedule_rationale=rationale,
        pretreatment=pretreatment,
        dilution=dilution,
        reaction_ph=REACTION_PH,
        ph_monitoring=ph_monitoring,
        external_tof=external_tof,
        external_tof_needed=external_tof_needed,
        fluoride_handling=fluoride_handling,
        customer_info_requests=customer_requests,
        optimization_notes=optimization_notes,
    )


def generate_test_plans(result: EvaluationResult, parsed: Optional[ParsedData]) -> List[TestPlan]:
    """Generate one bench test plan per sample that has quantified PFAS data."""
    project_ctx = {}
    if parsed is not None:
        project_ctx = getattr(parsed, "llm_project_context", {}) or {}

    plans: List[TestPlan] = []
    for sr in result.samples:
        if not sr.module1.species and sr.module1.total_conc_mg_L == 0:
            continue  # skip empty placeholder samples
        plans.append(_generate_one_plan(sr, result.module3, project_ctx))
    return plans


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT EXPORT
# ═══════════════════════════════════════════════════════════════════════════════

def build_plan_text(plan: TestPlan) -> str:
    lines = [
        "=" * 66,
        f"BENCH TEST PLAN — {plan.sample_name}",
        "750 mL Amazon Reactor  |  Proof-of-Concept stage",
        "=" * 66,
        f"Total PFAS: {format_conc_auto(plan.total_pfas_mg_L)}",
        "",
        "REACTION CONDITIONS",
    ]
    for c in plan.conditions:
        lines.append(f"  {c.label}: {c.sulfite_mM:g} mM sulfite (Reagent A) + "
                     f"{c.iodide_mM:g} mM iodide (Reagent B)")
        if c.note:
            lines.append(f"      ({c.note})")
    lines += [
        "",
        f"SAMPLING SCHEDULE: {plan.schedule_type}",
        f"  Timepoints: {', '.join(plan.schedule_timepoints)}",
        f"  {plan.schedule_rationale}",
        "",
        "PRETREATMENT",
        *[f"  - {p}" for p in plan.pretreatment],
        "",
        f"DILUTION: {plan.dilution}",
        "",
        f"pH: setpoint {plan.reaction_ph:g}. {plan.ph_monitoring}",
        "",
        "EXTERNAL TOF (Novem)",
        f"  {plan.external_tof}",
    ]
    if plan.fluoride_handling:
        lines.append(f"  {plan.fluoride_handling}")
    if plan.optimization_notes:
        lines += ["", "OPTIMIZATION (later stage)", *[f"  - {n}" for n in plan.optimization_notes]]
    if plan.customer_info_requests:
        lines += ["", "CONFIRM WITH CUSTOMER", *[f"  - {n}" for n in plan.customer_info_requests]]
    lines += ["", "=" * 66,
              "Claros R&D Team  |  Bench Test Plan (POC)  |  conditions are adjustable",
              "=" * 66]
    return "\n".join(lines)
