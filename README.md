# PFAS Material Evaluation Engine

**Claros R&D Team** | Framework Architecture by Zack Liu | v1.0

A deterministic, rule-based expert system for preliminary PFAS treatment feasibility screening.
This is an internal R&D decision tool — not a chatbot or generic AI demo.

---

## What It Does

Accepts customer-provided PFAS data (Excel tables, PDF lab reports, pasted text) and applies
a three-module rule engine to produce:

1. **Technical Full Output** — structured evaluation with per-species composition, reactivity
   flags, water matrix assessment, and treatment technology guidance
2. **Business Email Draft** — concise internal email summarising conclusions and action items
3. **Overall Status** — `PROCEED` / `CONDITIONAL` / `CRITICAL` with colour-coded badge

---

## Project Structure

```
pfas_eval_engine/
├── app.py            Streamlit UI — layout, rendering, session state
├── parser.py         Data ingestion — Excel, PDF, pasted text
├── engine.py         Rule engine — Module 1, 2, 3, status logic, email generation
├── utils.py          PFAS species DB, unit conversions, formatting helpers
├── requirements.txt  Python dependencies
└── README.md         This file
```

---

## Installation

```bash
# 1. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate          # macOS / Linux
venv\Scripts\activate             # Windows

# 2. Install dependencies
pip install -r requirements.txt
```

**Python version:** 3.9 or higher recommended.

---

## Running Locally

```bash
cd pfas_eval_engine
streamlit run app.py
```

The app opens in your browser at `http://localhost:8501`.

---

## How to Use

### Input options (at least one required)

| Source | Format | Notes |
|--------|--------|-------|
| Excel PFAS table | `.xlsx`, `.xls`, `.csv` | Column A = analyte names, Columns B+ = sample concentrations. Unit detected automatically. |
| PDF lab report | `.pdf` | Structured table or text-based. pdfplumber extracts data. |
| Pasted text / email | Text area | Inline concentrations (e.g. `PFOA: 250 ng/L`) and matrix params (e.g. `DOC = 5 mg/L`) parsed by regex. |
| Treatment goals | Text area | Context used in email draft; not parsed for concentrations. |

### Running an evaluation

1. Upload / paste at least one data source in the **left panel**
2. Click **▶ Run Evaluation**
3. Review output in the three tabs on the right:
   - `📋 Technical Output` — full structured report
   - `✉ Business Email Draft` — ready-to-edit internal email
   - `🔍 Debug / Logs` — parser and engine trace for troubleshooting

### Downloading results

Both the Technical Report and Email Draft have **Download** buttons.

---

## Engine Logic Summary

### Module 1 — PFAS Composition Analysis
- Computes total PFAS concentration per sample
- Ranks species by concentration; calculates percentage contribution
- Defines **Primary Set**: species contributing ≥ 5 % of total
- Classifies species by chain length and functional group

### Module 2 — Species-Based Reactivity Screening
Applies deterministic rules based on composition:

| Rule | Condition | Status |
|------|-----------|--------|
| R1 | Ultra-short-chain (TFA, TFMS, PFPrA) in Primary Set | **CRITICAL** |
| R1b | Ultra-short-chain trace (< 5 %) | CONDITIONAL |
| R2a | Short-chain dominant ≥ 50 % | CONDITIONAL |
| R2b | Short-chain significant 30–50 % | CONDITIONAL |
| R3 | Precursors > 5 % of total | CONDITIONAL |
| R4 | Emerging species (GenX, PFESA, ADONA) in Primary Set | CONDITIONAL |
| R5 | Long-chain dominant ≥ 70 % | PROCEED signal |
| R6 | Ultra-short / emerging keyword in text (no conc.) | CRITICAL / CONDITIONAL |

### Module 3 — Water Matrix Screening
Applies thresholds to extracted matrix parameters:

| Parameter | Threshold | Flag |
|-----------|-----------|------|
| DOC / TOC | > 10 mg/L | WARNING — reduced GAC capacity |
| Sulfate | > 250 mg/L | WARNING — reduced IX capacity |
| Nitrate | > 10 mg/L | WARNING — IX competition |
| Hardness | > 500 mg/L | WARNING — RO scaling risk |
| TDS | > 1000 mg/L | WARNING — RO energy / IX impact |
| pH | < 6 or > 9 | WARNING — speciation effects |
| Turbidity / TSS | > 5 NTU/mg/L | INFO — prefiltration required |

### Overall Status

```
CRITICAL     → any ultra-short-chain in Primary Set, OR any critical flag
CONDITIONAL  → short-chain ≥ 30 %, precursors present, matrix warnings,
               emerging species, or missing required data
PROCEED      → long-chain dominant ≥ 70 %, no critical/warning flags
```

---

## Unit Handling

All concentrations are normalised to **mg/L** internally.

| Input unit | Factor to mg/L |
|------------|---------------|
| ng/L (ppt) | × 1 × 10⁻⁶ |
| µg/L (ppb) | × 1 × 10⁻³ |
| mg/L (ppm) | × 1 |

Display uses auto-scaling (ng/L / µg/L / mg/L) based on magnitude.

---

## Deploying to Streamlit Cloud

1. Push the `pfas_eval_engine/` folder to a GitHub repository
2. Go to [streamlit.io/cloud](https://streamlit.io/cloud) and connect the repo
3. Set the main file path to `pfas_eval_engine/app.py` (or `app.py` if at root)
4. `requirements.txt` is picked up automatically — no additional configuration needed

---

## Known Limitations (v1.0)

- PDF parsing works best on text-based (not scanned) PDFs
- Matrix parameter extraction from text uses regex — complex tables in email text may not parse fully
- No authentication, database, or multi-user workflow (by design for this MVP)
- Species classification defaults to "Unclassified" for non-standard analyte names not in the database

---

## Version

`v1.0` — Initial internal release
Claros R&D Team | Framework Architecture by Zack Liu
