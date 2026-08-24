# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A deterministic, rule-based expert system (Streamlit app) for preliminary PFAS treatment feasibility screening — an internal Claros R&D decision tool, not a chatbot. It ingests customer PFAS lab data (Excel / PDF / pasted text), runs a fixed rule engine, and produces a technical report, a business email draft, and bench-scale test plans.

The core evaluation logic is **deterministic** (no ML, no generative inference). An LLM is used only as an optional *pre-parser* to extract structured data from messy uploads and to draft the outbound email — never to make treatment decisions.

## Commands

```bash
pip install -r requirements.txt          # Python 3.9+ (devcontainer uses 3.11)
streamlit run app.py                      # opens http://localhost:8501
```

There is no test suite, linter config, or build step in this repo. The devcontainer's `postAttachCommand` auto-launches the app with `--server.enableCORS false --server.enableXsrfProtection false`.

The optional LLM features require an Anthropic API key, read via `_get_api_key()` in `app.py` from `st.secrets["ANTHROPIC_API_KEY"]` (i.e. `.streamlit/secrets.toml`, gitignored). Without a key the app runs fully on the rule-based path.

## Architecture — the pipeline

Data flows one direction through five modules. `app.py` is the only stateful layer (Streamlit `session_state`); everything else is pure functions operating on dataclasses.

```
uploads ──> parser.py / llm_parser.py ──> ParsedData
                                              │
                                          engine.py evaluate() ──> EvaluationResult
                                              │                        │
                                          module4.py generate_test_plans()
                                              │                        │
                                          app.py renders 4 tabs (report / email / test plans / debug)
```

### `parser.py` — rule-based ingestion → `ParsedData`
- `parse_all(...)` is the entry point. Merges Excel + PDF + text into one `ParsedData`. **Conflict priority: Excel > PDF > Text.** Text PFAS concentrations are only used if no structured file data exists.
- Handles both normal and **transposed** Excel layouts (`_detect_transposed_layout`), matrix-param extraction (DOC, sulfate, nitrate, hardness, TDS, pH, etc.), and PDF tables vs. free text.
- `ParsedData` (defined here) carries `pfas_samples` (dict of sample → {analyte: mg/L}), `matrix_params`, `keyword_species`, `aof_tof_data`, `sample_metadata`, plus `logs`/`warnings`/`errors` threaded through the whole pipeline.

### `llm_parser.py` — optional LLM pre-parser (same `ParsedData` output)
- `parse_with_llm(...)` converts files to text, sends to Claude (`model="claude-sonnet-4-5"`, `max_tokens=8192`) for structured JSON extraction, then maps JSON → `ParsedData` via `parse_from_llm_json`.
- **Always falls back to `parse_all()` on any failure** so the app never crashes when the LLM is unavailable. It is a drop-in substitute for the parser stage, toggled by a UI switch in `app.py`.

### `engine.py` — the deterministic rule engine → `EvaluationResult`
`evaluate(parsed)` orchestrates, per sample:
- **Module 1** (`run_module1`) — composition: total PFAS, ranking, percentage contribution, "Primary Set" (species ≥5% of total), chain-length/functional-group classification.
- **Module 2** (`run_module2`) — species reactivity rules R1–R6 producing per-sample status (CRITICAL / CONDITIONAL / PROCEED).
- **Module 3** (`run_module3`) — water-matrix threshold screening (`M3_R1`…`M3_R7`).
- **TOF/AOF analysis** (`run_tof_analysis`) — compares theoretical organofluorine vs. reported bulk TOF/AOF for mass-balance coverage.
- Overall status via `_determine_status`; per-sample via `_per_sample_status`.
- Before M1/M2, `evaluate()` **merges statistical Average+Maximum summary rows** into a single worst-case dataset (max preferred, avg as fallback for blank-max analytes) — see the `[M1-PRE]` block. Watch this when touching sample handling.
- All flags are `FlagItem(severity, code, message, detail)`; rule codes (e.g. `M3_R6`) are load-bearing identifiers referenced across modules and commit history — keep them stable.

### `module4.py` — bench test-plan generator → `List[TestPlan]`
- `generate_test_plans(result, parsed)` builds a deterministic bench-scale plan per sample from the M1–M3 results. Standard system: 750 mL Amazon Reactor.
- Two conditions per plan: (A) fixed reference control (10 mM sulfite + 2 mM iodide), (B) water-adjusted dose scaled by PFAS load + nitrate/nitrite electron demand.
- Domain constants live at the top of the file (`SULFITE_BASE_*` tiers, nitrate/nitrite electron stoichiometry, `NOVEM_TOF_DL_MG_L`, telomer kinetics threshold, sampling schedules). These are **confirmed with Claros R&D** — treat as chemistry, not arbitrary tunables; several align with engine `M3_*` rule thresholds.
- Outputs are *recommendations*: `app.py` renders every field as editable.

### `utils.py` — shared domain layer
- `PFAS_SPECIES_DB` — master dict of species → `{full_name, chain, group, category}`. Categories (`ultra_short`, `short_chain`, `long_chain`, `precursor`, etc.) drive Module 1/2 classification. Add new analytes here.
- Unit conversion (everything normalized to **mg/L** internally: ng/L ×1e-6, µg/L ×1e-3), name normalization (`normalize_pfas_name`), classification helpers (`classify_pfas`, `is_ftoh`, `is_short_telomer`, `get_pfas_f_fraction`), and display formatting (`format_conc_auto` auto-scales for display, `status_badge_html`).

### `app.py` — Streamlit UI (~1800 lines)
- Single-file UI: input panel, `▶ Run Evaluation`, and four output tabs (Technical Output, Business Email Draft, Test Plans, Debug/Logs).
- `session_state` keys: `eval_result`, `parsed_data`, `llm_email`, `test_plans`. Email and test plans are generated lazily/on-demand; new evaluation runs reset them to `None`.
- `_generate_llm_email(...)` uses the Anthropic client directly (separate from the parser LLM call). LLM email and LLM parsing are independently gated on the API key + UI toggle.

## Conventions

- **The engine stays deterministic.** Rule logic, thresholds, and status decisions belong in `engine.py` / `module4.py` as explicit rules — do not push evaluation decisions into the LLM. The LLM is confined to parsing messy input and drafting prose.
- **Preserve rule/flag codes** (`M3_R6`, `M2_R4`, etc.). Commit messages reference them and they tie engine thresholds to module4 pretreatment logic.
- Chemistry constants and thresholds are commented with their rationale/source — keep those comments when editing values, and cross-check the paired engine/module4 thresholds noted in-file.
- Concentrations are mg/L everywhere internally; only display formatting rescales.
- README.md documents Modules 1–3 and unit handling but predates `module4.py` and `llm_parser.py`; trust the code for those two.
