"""
utils.py — PFAS Evaluation Engine
Utility layer: PFAS species database, unit conversions, name normalization,
numeric parsing, display formatting, and species classification helpers.

Updated to align with PFAS_Material_Intelligence_Engine spec v1.0.

Claros R&D Team | Framework Architecture by Zack Liu
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

# ═══════════════════════════════════════════════════════════════════════════════
# PFAS SPECIES MASTER DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

PFAS_SPECIES_DB: Dict[str, Dict[str, Any]] = {
    # ── Ultra-short-chain (≤C3) ─────────────────────────────────────────────
    "TFA":           {"full_name": "Trifluoroacetic acid",                              "chain": 2,  "group": "carboxylate",        "category": "ultra_short"},
    "TFMS":          {"full_name": "Trifluoromethanesulfonic acid (triflate)",           "chain": 1,  "group": "sulfonate",           "category": "ultra_short"},
    "TFSI":          {"full_name": "Bis(trifluoromethylsulfonyl)imide",                 "chain": 1,  "group": "imide",               "category": "ultra_short"},
    "PFPrA":         {"full_name": "Perfluoropropanoic acid",                           "chain": 3,  "group": "carboxylate",         "category": "ultra_short"},
    "PFPrS":         {"full_name": "Perfluoropropane sulfonic acid",                    "chain": 3,  "group": "sulfonate",           "category": "ultra_short"},

    # ── Short-chain carboxylates (C4–C7) ────────────────────────────────────
    "PFBA":          {"full_name": "Perfluorobutanoic acid",                            "chain": 4,  "group": "carboxylate",         "category": "short_chain"},
    "PFPeA":         {"full_name": "Perfluoropentanoic acid",                           "chain": 5,  "group": "carboxylate",         "category": "short_chain"},
    "PFHxA":         {"full_name": "Perfluorohexanoic acid",                            "chain": 6,  "group": "carboxylate",         "category": "short_chain"},
    "PFHpA":         {"full_name": "Perfluoroheptanoic acid",                           "chain": 7,  "group": "carboxylate",         "category": "short_chain"},

    # ── Long-chain carboxylates (C8+) ───────────────────────────────────────
    "PFOA":          {"full_name": "Perfluorooctanoic acid",                            "chain": 8,  "group": "carboxylate",         "category": "long_chain"},
    "PFNA":          {"full_name": "Perfluorononanoic acid",                            "chain": 9,  "group": "carboxylate",         "category": "long_chain"},
    "PFDA":          {"full_name": "Perfluorodecanoic acid",                            "chain": 10, "group": "carboxylate",         "category": "long_chain"},
    "PFUnDA":        {"full_name": "Perfluoroundecanoic acid",                          "chain": 11, "group": "carboxylate",         "category": "long_chain"},
    "PFDoDA":        {"full_name": "Perfluorododecanoic acid",                          "chain": 12, "group": "carboxylate",         "category": "long_chain"},
    "PFTrDA":        {"full_name": "Perfluorotridecanoic acid",                         "chain": 13, "group": "carboxylate",         "category": "long_chain"},
    "PFTeDA":        {"full_name": "Perfluorotetradecanoic acid",                       "chain": 14, "group": "carboxylate",         "category": "long_chain"},

    # ── Short-chain sulfonates / PFSA (C4–C5) ───────────────────────────────
    "PFBS":          {"full_name": "Perfluorobutane sulfonic acid",                     "chain": 4,  "group": "sulfonate",           "category": "short_chain"},
    "PFPeS":         {"full_name": "Perfluoropentane sulfonic acid",                    "chain": 5,  "group": "sulfonate",           "category": "short_chain"},

    # ── Long-chain sulfonates / PFSA (C6+) ──────────────────────────────────
    "PFHxS":         {"full_name": "Perfluorohexane sulfonic acid",                     "chain": 6,  "group": "sulfonate",           "category": "long_chain"},
    "PFHpS":         {"full_name": "Perfluoroheptane sulfonic acid",                    "chain": 7,  "group": "sulfonate",           "category": "long_chain"},
    "PFOS":          {"full_name": "Perfluorooctane sulfonic acid",                     "chain": 8,  "group": "sulfonate",           "category": "long_chain"},
    "PFDS":          {"full_name": "Perfluorodecane sulfonic acid",                     "chain": 10, "group": "sulfonate",           "category": "long_chain"},

    # ── Precursors / transformation products ────────────────────────────────
    "PFOSA":         {"full_name": "Perfluorooctane sulfonamide",                       "chain": 8,  "group": "sulfonamide",         "category": "precursor"},
    "PFOSF":         {"full_name": "Perfluorooctane sulfonyl fluoride",                 "chain": 8,  "group": "sulfonyl_F",          "category": "precursor"},
    "N-MeFOSA":      {"full_name": "N-methyl perfluorooctane sulfonamide",              "chain": 8,  "group": "sulfonamide",         "category": "precursor"},
    "N-EtFOSA":      {"full_name": "N-ethyl perfluorooctane sulfonamide",               "chain": 8,  "group": "sulfonamide",         "category": "precursor"},
    "N-MeFOSE":      {"full_name": "N-methyl perfluorooctane sulfonamidoethanol",       "chain": 8,  "group": "sulfonamide",         "category": "precursor"},
    "N-EtFOSE":      {"full_name": "N-ethyl perfluorooctane sulfonamidoethanol",        "chain": 8,  "group": "sulfonamide",         "category": "precursor"},
    "6:2 FTOH":      {"full_name": "6:2 Fluorotelomer alcohol",                         "chain": 8,  "group": "FTOH",                "category": "precursor"},
    "8:2 FTOH":      {"full_name": "8:2 Fluorotelomer alcohol",                         "chain": 10, "group": "FTOH",                "category": "precursor"},
    "10:2 FTOH":     {"full_name": "10:2 Fluorotelomer alcohol",                        "chain": 12, "group": "FTOH",                "category": "precursor"},

    # ── Fluorotelomer sulfonates (FTS / FTSA) ────────────────────────────────
    "2:2 FTS":       {"full_name": "2:2 Fluorotelomer sulfonate",                       "chain": 4,  "group": "FTS",                 "category": "short_chain"},
    "4:2 FTS":       {"full_name": "4:2 Fluorotelomer sulfonate",                       "chain": 6,  "group": "FTS",                 "category": "short_chain"},
    "6:2 FTS":       {"full_name": "6:2 Fluorotelomer sulfonate",                       "chain": 8,  "group": "FTS",                 "category": "long_chain"},
    "8:2 FTS":       {"full_name": "8:2 Fluorotelomer sulfonate",                       "chain": 10, "group": "FTS",                 "category": "long_chain"},
    "10:2 FTS":      {"full_name": "10:2 Fluorotelomer sulfonate",                      "chain": 12, "group": "FTS",                 "category": "long_chain"},
    "2:2 FTSA":      {"full_name": "2:2 Fluorotelomer sulfonic acid",                   "chain": 4,  "group": "FTSA",                "category": "short_chain"},
    "4:2 FTSA":      {"full_name": "4:2 Fluorotelomer sulfonic acid",                   "chain": 6,  "group": "FTSA",                "category": "short_chain"},
    "6:2 FTSA":      {"full_name": "6:2 Fluorotelomer sulfonic acid",                   "chain": 8,  "group": "FTSA",                "category": "long_chain"},
    "8:2 FTSA":      {"full_name": "8:2 Fluorotelomer sulfonic acid",                   "chain": 10, "group": "FTSA",                "category": "long_chain"},

    # ── Emerging / non-standard / ether-linked ──────────────────────────────
    # Ether carboxylates (spec keyword group: ether_carboxylate)
    "HFPO-DA":       {"full_name": "Hexafluoropropylene oxide dimer acid (GenX)",       "chain": 6,  "group": "ether_carboxylate",   "category": "emerging"},
    "GenX":          {"full_name": "GenX (HFPO-DA)",                                    "chain": 6,  "group": "ether_carboxylate",   "category": "emerging"},
    "ADONA":         {"full_name": "4,8-Dioxa-3H-perfluorononanoic acid",               "chain": 9,  "group": "ether_carboxylate",   "category": "emerging"},
    "F-53B":         {"full_name": "F-53B (chlorinated PFESA mixture, 6:2 Cl-PFESA)",  "chain": 8,  "group": "ether_carboxylate",   "category": "emerging"},

    # Ether sulfonates (PFESA variants)
    "2+2 PFESA":     {"full_name": "Perfluoro(2-ethoxyethane) sulfonic acid",           "chain": 4,  "group": "ether_sulfonate",     "category": "emerging"},
    "4+2 PFESA":     {"full_name": "Perfluoro(4-ethoxyethane) sulfonic acid",           "chain": 6,  "group": "ether_sulfonate",     "category": "emerging"},
    "6+2 PFESA":     {"full_name": "Perfluoro(6-ethoxyethane) sulfonic acid",           "chain": 8,  "group": "ether_sulfonate",     "category": "emerging"},
    "PFMPA":         {"full_name": "Perfluoro-3-methoxypropanoic acid",                 "chain": 4,  "group": "ether_carboxylate",   "category": "emerging"},
    "PFMBA":         {"full_name": "Perfluoro-4-methoxybutanoic acid",                  "chain": 5,  "group": "ether_carboxylate",   "category": "emerging"},
    "11Cl-PF3OUdS":  {"full_name": "11-Chloro-1H,2H-perfluoro-1-undecanesulfonate",    "chain": 9,  "group": "chloro_sulfonate",    "category": "emerging"},
    "9Cl-PF3ONS":    {"full_name": "9-Chloro-1H-perfluoro-3,6-dioxanonane-1-sulfonate", "chain": 8, "group": "chloro_sulfonate",    "category": "emerging"},
}

# Human-readable category labels
CATEGORY_LABELS: Dict[str, str] = {
    "ultra_short": "Ultra-Short-Chain (≤C3)",
    "short_chain": "Short-Chain (C4–C7 carboxylate / C4–C5 sulfonate)",
    "long_chain":  "Long-Chain (C8+ carboxylate / C6+ sulfonate)",
    "precursor":   "Precursor / Transformation Product",
    "emerging":    "Emerging / Non-Standard",
    "unknown":     "Unclassified",
}

# ── Alias map: UPPER-CASE name / CAS / formula → canonical abbreviation ──────
PFAS_ALIASES: Dict[str, str] = {
    # Full names
    "PERFLUOROOCTANOIC ACID":                  "PFOA",
    "PERFLUOROOCTANOATE":                      "PFOA",
    "PFOA (TOTAL)":                            "PFOA",
    "PFOA, TOTAL":                             "PFOA",
    "PERFLUOROOCTANE SULFONIC ACID":           "PFOS",
    "PERFLUOROOCTANESULFONIC ACID":            "PFOS",
    "PFOS (TOTAL)":                            "PFOS",
    "PFOS, TOTAL":                             "PFOS",
    "LINEAR PFOS":                             "PFOS",
    "BRANCHED PFOS":                           "PFOS",
    "PERFLUOROHEXANE SULFONIC ACID":           "PFHxS",
    "PERFLUOROHEXANESULFONIC ACID":            "PFHxS",
    "PERFLUOROBUTANE SULFONIC ACID":           "PFBS",
    "PERFLUOROBUTANESULFONIC ACID":            "PFBS",
    "PERFLUOROBUTANOIC ACID":                  "PFBA",
    "PERFLUOROPENTANOIC ACID":                 "PFPeA",
    "PERFLUOROHEXANOIC ACID":                  "PFHxA",
    "PERFLUOROHEPTANOIC ACID":                 "PFHpA",
    "PERFLUORONONANOIC ACID":                  "PFNA",
    "PERFLUORODECANOIC ACID":                  "PFDA",
    "PERFLUOROUNDECANOIC ACID":                "PFUnDA",
    "PERFLUORODODECANOIC ACID":                "PFDoDA",
    "PERFLUOROTRIDECANOIC ACID":               "PFTrDA",
    "PERFLUOROTETRADECANOIC ACID":             "PFTeDA",
    "PERFLUOROHEPTANE SULFONIC ACID":          "PFHpS",
    "PERFLUORODECANE SULFONIC ACID":           "PFDS",
    "PERFLUOROOCTANE SULFONAMIDE":             "PFOSA",
    "GENX":                                    "HFPO-DA",
    "HEXAFLUOROPROPYLENE OXIDE DIMER ACID":    "HFPO-DA",
    "N-METHYL PFOSA":                          "N-MeFOSA",
    "N-ETHYL PFOSA":                           "N-EtFOSA",
    "PERFLUOROPENTANE SULFONIC ACID":          "PFPeS",

    # TFA aliases (spec keyword list)
    "TRIFLUOROACETIC ACID":                    "TFA",
    "TRIFLUOROACETATE":                        "TFA",
    "CF3COOH":                                 "TFA",
    "CF3COO-":                                 "TFA",
    "C2HF3O2":                                 "TFA",
    "76-05-1":                                 "TFA",   # CAS

    # TFMS aliases (spec keyword list)
    "TRIFLUOROMETHANESULFONIC ACID":           "TFMS",
    "TRIFLUOROMETHANESULFONATE":               "TFMS",
    "TRIFLATE":                                "TFMS",
    "CF3SO3H":                                 "TFMS",
    "CF3SO3-":                                 "TFMS",
    "CF3SO2OH":                                "TFMS",
    "1493-13-6":                               "TFMS",  # CAS (sodium salt)
    "358-23-6":                                "TFMS",  # CAS (acid)

    # 2+2 PFESA aliases (spec keyword list)
    "PERFLUORO(2-ETHOXYETHANE)SULFONIC ACID":  "2+2 PFESA",
    "PERFLUORO(2-ETHOXYETHANE)SULFONATE":      "2+2 PFESA",
    "2:2 PFESA":                               "2+2 PFESA",
    "113507-82-7":                             "2+2 PFESA",  # CAS

    # HFPO-DA / GenX aliases
    "13252-13-6":                              "HFPO-DA",  # CAS

    # ADONA aliases
    "919005-14-4":                             "ADONA",    # CAS

    # F-53B (must NOT match 2+2 PFESA per spec exclusion list)
    "73606-19-6":                              "F-53B",    # CAS

    # Fluorotelomer sulfonates
    "4:2 FLUOROTELOMER SULFONATE":             "4:2 FTS",
    "6:2 FLUOROTELOMER SULFONATE":             "6:2 FTS",
    "8:2 FLUOROTELOMER SULFONATE":             "8:2 FTS",
    "4:2 FLUOROTELOMER SULPHONATE":            "4:2 FTS",   # UK spelling
    "6:2 FLUOROTELOMER SULPHONATE":            "6:2 FTS",
    "8:2 FLUOROTELOMER SULPHONATE":            "8:2 FTS",
    "4:2 FLUOROTELOMER SULFONIC ACID":         "4:2 FTSA",
    "6:2 FLUOROTELOMER SULFONIC ACID":         "6:2 FTSA",
    "8:2 FLUOROTELOMER SULFONIC ACID":         "8:2 FTSA",
    "10:2 FLUOROTELOMER SULFONIC ACID":        "8:2 FTSA",  # map to nearest
    "6:2 FLUOROTELOMER SULPHONIC ACID":        "6:2 FTSA",  # UK
    "8:2 FLUOROTELOMER SULPHONIC ACID":        "8:2 FTSA",

    # UK sulphonate/sulphonic spelling for common PFAS
    "PERFLUOROOCTANE SULPHONIC ACID":          "PFOS",
    "PERFLUOROOCTANESULPHONIC ACID":           "PFOS",
    "PERFLUOROHEXANE SULPHONIC ACID":          "PFHxS",
    "PERFLUOROHEXANESULPHONIC ACID":           "PFHxS",
    "PERFLUOROBUTANE SULPHONIC ACID":          "PFBS",
    "PERFLUOROPENTANE SULPHONIC ACID":         "PFPeS",
    "PERFLUOROHEPTANE SULPHONIC ACID":         "PFHpS",
    "PERFLUORODECANE SULPHONIC ACID":          "PFDS",

    # Carboxylate (-ate) form variants
    "PERFLUOROOCTANOATE":                      "PFOA",
    "PERFLUORONONANOATE":                      "PFNA",
    "PERFLUORODECANOATE":                      "PFDA",
    "PERFLUOROUNDECANOATE":                    "PFUnDA",
    "PERFLUORODODECANOATE":                    "PFDoDA",
    "PERFLUOROBUTANOATE":                      "PFBA",
    "PERFLUOROPENTANOATE":                     "PFPeA",
    "PERFLUOROHEXANOATE":                      "PFHxA",
    "PERFLUOROHEPTANOATE":                     "PFHpA",

    # Sulfonate (-ate) form variants
    "PERFLUOROOCTANESULFONATE":                "PFOS",
    "PERFLUOROHEXANESULFONATE":                "PFHxS",
    "PERFLUOROBUTANESULFONATE":                "PFBS",
    "PERFLUOROPENTANESULFONATE":               "PFPeS",
    "PERFLUORODECANESULFONATE":                "PFDS",

    # Alternate carboxylate spellings sometimes used in EU/UK lab reports
    "PERFLUOROPROPIONIC ACID":                 "PFPrA",
    "PERFLUOROPROPANOATE":                     "PFPrA",
    "PERFLUOROTRIDECANOATE":                   "PFTrDA",
    "PERFLUOROTETRADECANOATE":                 "PFTeDA",

    # PFOSA / precursor variants
    "PERFLUOROOCTYLSULFONAMIDE":               "PFOSA",
    "PERFLUOROOCTYLSULPHONAMIDE":              "PFOSA",
    "N-METHYL PERFLUOROOCTYLSULFONAMIDE":      "N-MeFOSA",
    "N-ETHYL PERFLUOROOCTYLSULFONAMIDE":       "N-EtFOSA",

    # HFPO-DA / GenX alternate labels
    "HEXAFLUOROPROPYLENE OXIDE DIMER ACID":    "HFPO-DA",
    "HFPO DIMER ACID":                         "HFPO-DA",
    "AMMONIUM PFOA REPLACEMENT":               "HFPO-DA",

    # CAS numbers for remaining key species
    "335-67-1":   "PFOA",
    "1763-23-1":  "PFOS",
    "355-46-4":   "PFHxS",
    "375-22-4":   "PFBA",
    "2706-90-3":  "PFPeA",
    "307-24-4":   "PFHxA",
    "375-85-9":   "PFHpA",
    "375-95-1":   "PFNA",
    "335-76-2":   "PFDA",
    "2058-94-8":  "PFUnDA",
    "307-55-1":   "PFDoDA",
    "375-73-5":   "PFBS",
    "3871-99-6":  "PFHxS",
}

# Species that must NEVER be reclassified as 2+2 PFESA (spec explicit exclude list)
PFESA_2PLUS2_EXCLUDES: frozenset = frozenset(["F-53B", "73606-19-6"])


# ═══════════════════════════════════════════════════════════════════════════════
# UNIT CONVERSION
# ═══════════════════════════════════════════════════════════════════════════════

_UNIT_TO_MG_L: Dict[str, float] = {
    "ng/l": 1e-6,  "ng/L": 1e-6,  "ppt": 1e-6,
    "ug/l": 1e-3,  "ug/L": 1e-3,
    "µg/l": 1e-3,  "µg/L": 1e-3,
    "μg/l": 1e-3,  "μg/L": 1e-3,
    "ppb":  1e-3,
    "mg/l": 1.0,   "mg/L": 1.0,
    "ppm":  1.0,
}


def convert_to_mg_L(value: float, unit: str) -> Optional[float]:
    """Convert a concentration value to mg/L. Returns None for unknown units."""
    factor = _UNIT_TO_MG_L.get(unit) or _UNIT_TO_MG_L.get(unit.strip().lower())
    return value * factor if factor is not None else None


def detect_unit_from_text(text: str) -> Optional[str]:
    """Infer the concentration unit from a column header or free text."""
    t = text.lower().strip()
    if "ng/l" in t or "ppt" in t:
        return "ng/L"
    if any(u in t for u in ("ug/l", "µg/l", "μg/l", "ppb")):
        return "ug/L"
    if "mg/l" in t or "ppm" in t:
        return "mg/L"
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# PFAS NAME NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_pfas_name(raw: str) -> str:
    """
    Map a raw analyte string to its canonical PFAS abbreviation.

    Resolution order:
      1. Direct key match (exact abbreviation, case-insensitive)
      2. Alias lookup (full names, CAS numbers, formula strings)
      3. Alias lookup after stripping parenthetical suffixes
         e.g. "Perfluorooctanoic acid (PFOA)" → try "PERFLUOROOCTANOIC ACID"
      4. Partial key-in-name scan (abbreviation embedded in longer string)
      5. Return raw value unchanged (unknown species)

    Respects the PFESA exclude list (F-53B is never mapped to 2+2 PFESA).
    """
    if not raw:
        return raw
    s = raw.strip()
    upper = s.upper()

    # 1. Direct key match
    if s in PFAS_SPECIES_DB:
        return s
    if upper in PFAS_SPECIES_DB:
        return upper

    # 2. Full alias lookup (includes CAS numbers and formulas)
    if upper in PFAS_ALIASES:
        result = PFAS_ALIASES[upper]
        if result == "2+2 PFESA" and upper in {e.upper() for e in PFESA_2PLUS2_EXCLUDES}:
            return "F-53B"
        return result

    # 3. Strip parenthetical content and retry alias lookup
    #    e.g. "Perfluorooctanoic acid (PFOA)" → "Perfluorooctanoic acid"
    stripped = re.sub(r"\s*\(.*?\)\s*$", "", s).strip()
    if stripped and stripped != s:
        upper_stripped = stripped.upper()
        if upper_stripped in PFAS_SPECIES_DB:
            return upper_stripped
        if upper_stripped in PFAS_ALIASES:
            result = PFAS_ALIASES[upper_stripped]
            if result == "2+2 PFESA" and upper_stripped in {e.upper() for e in PFESA_2PLUS2_EXCLUDES}:
                return "F-53B"
            return result

    # 4. Partial key-in-name (only for keys ≥ 4 chars to avoid false positives)
    for key in sorted(PFAS_SPECIES_DB.keys(), key=len, reverse=True):
        if len(key) >= 4 and (key in s or key.upper() in upper):
            return key

    return s  # unknown — return raw stripped


def classify_pfas(name: str) -> str:
    """Return the category string for a (possibly unnormalized) PFAS name."""
    return PFAS_SPECIES_DB.get(normalize_pfas_name(name), {}).get("category", "unknown")


def get_pfas_info(name: str) -> Dict[str, Any]:
    """Return metadata dict for a PFAS species. Returns minimal stub for unknowns."""
    return PFAS_SPECIES_DB.get(
        normalize_pfas_name(name),
        {"full_name": name, "category": "unknown", "chain": None, "group": "unknown"},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SPECIES CLASSIFICATION HELPERS  (used by Module 2 rules)
# ═══════════════════════════════════════════════════════════════════════════════

def is_pfsa_sulfonate(name: str) -> bool:
    """
    Return True if the species is a PFSA / perfluoroalkyl sulfonic acid class.
    Includes straight-chain sulfonates and fluorotelomer sulfonates (FTS/FTSA).
    Used in Module 2 Rule R4 (PFSA kinetics).
    """
    info = get_pfas_info(name)
    return info.get("group") in ("sulfonate", "FTS", "FTSA")


def is_ether_carboxylate(name: str) -> bool:
    """
    Return True if the species belongs to the ether_carboxylate group
    (HFPO-DA/GenX, ADONA, F-53B, PFMPA, PFMBA).
    Used in Module 2 Rule R6.
    """
    return get_pfas_info(name).get("group") == "ether_carboxylate"


def is_short_telomer(name: str) -> Tuple[bool, Optional[int]]:
    """
    Check if a species name is a short-chain fluorotelomer with m-value < 4.
    Pattern: '<m>:<n> FTS|FTSA|FTCA|FTOH'
    Returns (is_short_telomer: bool, m_value: int | None).
    Used in Module 2 Rule R5.
    """
    m = re.match(r"^(\d+):(\d+)\s*(FTS[A]?|FTCA|FTOH)", name.strip(), re.IGNORECASE)
    if m:
        chain_m = int(m.group(1))
        return True, chain_m
    return False, None


def is_pfca_only(names: list) -> bool:
    """
    Return True if ALL species in the list are PFCA (perfluoroalkyl carboxylate).
    Used in Module 2 Rule R99.
    """
    return all(get_pfas_info(n).get("group") == "carboxylate" for n in names)


# ═══════════════════════════════════════════════════════════════════════════════
# NUMERIC VALUE PARSING
# ═══════════════════════════════════════════════════════════════════════════════

# Spec: treat_ND_as: unknown | treat_blank_as: unknown | do_not_assume_zero: true
_ND_TOKENS = frozenset([
    "nd", "bdl", "n/a", "na", "nr", "not detected", "not analysed",
    "not analyzed", "bl", "blk", "blank", "b", "u", "neg", "negative",
    "<dl", "<ql", "<rl", "<mdl", "<mrl",
])


def parse_numeric_value(raw: Any) -> Optional[float]:
    """
    Parse a concentration value from raw cell data.

    Spec policy (do_not_assume_zero = true):
    - Numeric types          → float
    - ND / blank / <RL       → None  (UNKNOWN — excluded from totals)
    - Blank / None / dash    → None  (missing)
    - Unparseable strings    → None
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        return None if v != v else v  # NaN guard

    s = str(raw).strip()
    if not s or s in ("-", "—", "–", "N/A", ""):
        return None

    # Non-detect / below detection limit → UNKNOWN (not zero per spec)
    if s.lower() in _ND_TOKENS:
        return None
    if s.startswith("<") or s.startswith("≤"):
        return None  # do_not_assume_zero per spec

    s_clean = s.replace(",", "").strip()
    try:
        return float(s_clean)
    except ValueError:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def format_conc_auto(value_mg_L: float) -> str:
    """Express a mg/L value in the most human-readable unit."""
    ng = value_mg_L * 1e6
    ug = value_mg_L * 1e3
    if ng < 1000:
        return f"{ng:.2f} ng/L"
    if ug < 1000:
        return f"{ug:.2f} µg/L"
    return f"{value_mg_L:.4f} mg/L"


def format_pct(value: float) -> str:
    return f"{value:.1f}%"


def status_badge_html(status: str) -> str:
    """Return an HTML <div> badge styled for the given evaluation status."""
    palette = {
        "PROCEED":     ("#1E8449", "#FFFFFF"),
        "CONDITIONAL": ("#D4AC0D", "#1C1C1C"),
        "CRITICAL":    ("#C0392B", "#FFFFFF"),
    }
    bg, fg = palette.get(status, ("#707B7C", "#FFFFFF"))
    return (
        f'<div style="display:inline-block; background:{bg}; color:{fg}; '
        f'padding:8px 24px; border-radius:6px; font-size:1.25rem; '
        f'font-weight:700; letter-spacing:2px; text-align:center; '
        f'box-shadow: 0 2px 4px rgba(0,0,0,0.2);">'
        f'{status}</div>'
    )


def severity_badge(severity: str) -> str:
    """Return a small inline HTML severity label for all classification types."""
    palette = {
        # Core statuses
        "critical":       ("#C0392B", "#fff",     "CRITICAL"),
        "warning":        ("#D4AC0D", "#111",      "WARNING"),
        "info":           ("#2980B9", "#fff",      "INFO"),
        "ok":             ("#1E8449", "#fff",      "OK / PROCEED"),
        # Spec M2 classifications
        "commercial":     ("#6C3483", "#fff",      "COMMERCIAL"),
        "technical":      ("#BA4A00", "#fff",      "TECHNICAL"),
        "pathway":        ("#117A65", "#fff",      "PATHWAY"),
        "special_handling": ("#5D6D7E", "#fff",   "SPECIAL HANDLING"),
    }
    bg, fg, label = palette.get(severity, ("#707B7C", "#fff", severity.upper()))
    return (
        f'<span style="background:{bg}; color:{fg}; '
        f'padding:2px 8px; border-radius:3px; font-size:0.75rem; '
        f'font-weight:600; letter-spacing:1px;">{label}</span>'
    )
