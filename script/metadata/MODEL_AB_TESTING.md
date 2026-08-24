# Model A/B testing for column classification (DENG-10944)

Choosing which LLM classifies BigQuery columns against the Mozilla data taxonomy.
This documents the evaluation: candidates, method, how to run it, what is measured,
and where the logs live.

## Why

The classifier (`field_classifier.py`) sends each column's name, type, DLP-sanitized
samples, existing description, and the full ~8.7K-token taxonomy to an LLM, which
returns a taxonomy label + confidence + `needs_review` as structured JSON. At scale
this is ~114K columns across the 158 mozilla-confidential datasets.

The task is ~97% easy with a hard ~3% tail (opaque columns, over-precise identifier
labels, `needs_review` calibration). The quality bar that matters:

1. **Never under-classify a sensitive column** (email / IP / phone / address / financial).
2. **Don't over-assign specific identifier leaves** (e.g. `user.account.fxid` to a
   timestamp - the bug we found and fixed with a prompt nudge; see git history).
3. **Well-calibrated `needs_review`.**

We want the cheapest, fastest model that clears that bar - measured, not assumed.

## Candidates (5)

| Model | Tier | Plumbing |
|---|---|---|
| `gemini-3.5-flash-lite` | cheap Gemini (GA) | Vertex ADC |
| `gemini-3.6-flash` | GA upgrade | Vertex ADC |
| `claude-haiku-4-5` | cheap Claude | `ANTHROPIC_API_KEY` |
| `claude-sonnet-5` | strong Claude | `ANTHROPIC_API_KEY` |
| `claude-opus-5` | quality ceiling | `ANTHROPIC_API_KEY` |

> The original baseline `gemini-3.1-flash-lite-preview` is **retired (404)** as of
> 2026-07-31 - the preview ID no longer resolves. This is also the pipeline's
> current *default* model, so the productionized default must be updated to the
> chosen GA model. `gemini-3.1-flash-lite` (GA, $0.25/$1.50) also exists if a
> cheaper option than 3.5-flash-lite ($0.30/$2.50) is wanted.

## Test set

The vetted **FxA `accounts_db_external`** dataset (207 columns). We've already
hand-checked its labels - the fxid cases, the 4 `needs_review` rows, and every
sensitive column - so it doubles as ground truth. The A/B **reuses existing
profiles** (`akomar_column_profiles_v1`), so there is **no re-profiling / re-scan**
(~231 GB avoided per model); each run is pure LLM cost, a few dollars per model.

Each model's rows coexist in `akomar_field_classifications_v1`, distinguished by the
`model` column - nothing is overwritten.

## How to run

One command runs all five:

```bash
cd /Users/akomar/mozilla/dev-worktrees/bigquery-etl.data_classification_based_on_DENG-10944

DLP_PROJECT=akomar-sandbox-438914 DLP_QUOTA_PROJECT=akomar-sandbox-438914 \
CLASSIFICATION_PROJECT=moz-fx-data-shared-prod \
CLASSIFICATION_DATASET=data_governance_metadata_derived \
  script/metadata/ab_test_models.sh
```

Prereqs: `ANTHROPIC_API_KEY` set (for the three `claude-*` arms) and Vertex ADC as
yourself (for the `gemini-*` arms + read access to the restricted FxA data). Run as
your own account, not the sandbox SA.

- Subset: `MODELS="gemini-3.6-flash claude-sonnet-5" script/metadata/ab_test_models.sh`
- Different dataset: `DATASET=stripe_external ...`
- Re-measure from scratch (a model already run is otherwise skipped): `REFRESH=1 ...`

**Parallelism (default).** All models run concurrently (`PARALLEL=0` for sequential).
Each model writes its own `<model>.log` and times itself independently, so
per-model runtime stays accurate; total wall-clock is ~the slowest model (~30-40 min)
instead of the sequential ~2 h. Under `REFRESH=1` the harness clears all A/B rows in
a **single** up-front `DELETE` and then lets the parallel children append only -
per-model `--refresh` would issue concurrent DML on one table and hit BigQuery's
mutation-concurrency limits.

**Don't let the laptop sleep** - wrap in `caffeinate` (idle sleep suspends the
in-flight Vertex/Anthropic socket and the client wedges on the dead connection):

```bash
caffeinate -dims env REFRESH=1 \
  DLP_PROJECT=akomar-sandbox-438914 DLP_QUOTA_PROJECT=akomar-sandbox-438914 \
  CLASSIFICATION_PROJECT=moz-fx-data-shared-prod \
  CLASSIFICATION_DATASET=data_governance_metadata_derived \
  script/metadata/ab_test_models.sh
```

## What is measured

Cost is computed from **real token counts**, not estimates. `field_classifier.py`
now logs a `TOKENS model=... llm_calls=... input_tokens=... output_tokens=...` line
per run (from each provider's usage metadata); the harness sums them per model and
applies a price table.

Per-model row in the summary: columns classified, wall-clock, seconds/column, input
& output tokens, actual cost, cost/1k columns, and cost extrapolated to the full
114,187-column corpus. Transient errors and non-zero exit codes are flagged.

Prices (per 1M tokens, standard / global endpoint, confirmed 2026-07-31):

| Model | Input | Output |
|---|---|---|
| gemini-3.1-flash-lite | $0.25 | $1.50 |
| gemini-3.6-flash | $1.50 | $7.50 |
| claude-haiku-4-5 | $1.00 | $5.00 |
| claude-sonnet-5 | $3.00 (intro $2 to 2026-08-31) | $15.00 (intro $10) |
| claude-opus-5 | $5.00 | $25.00 |

Note: `gemini-3.6-flash` is *pricier* than Claude Haiku 4.5; `flash-lite` is the
cheapest arm. [Gemini Vertex pricing](https://cloud.google.com/gemini-enterprise-agent-platform/generative-ai/pricing)
(non-global endpoints ~10% higher from 2026-07-01). Batch/Flex is ~50% off and
context caching cuts cached input further - both are prod levers, applied to the
winner, not this A/B.

## Where the logs are (reviewable later)

Under `classification_runs/model_ab/` (gitignored):

| File | Contents |
|---|---|
| `<model>.log` | full stdout+stderr for that model's run |
| `ab_metrics.jsonl` | one JSON line per model run (raw numbers) |
| `ab_summary.txt` | the comparison table |
| `ab_run.log` | everything, tee'd - the top-level review log |

## Scorecard (the qualitative half - separate step)

The harness produces cost/runtime/tokens. The **quality** comparison against the
vetted FxA labels is a separate analysis (in progress): for each model, diff
`primary_label` vs the vetted baseline and check the known-hard cases -

- sensitive-column under-classification (hard gate),
- `*.fxid` over-assignment count,
- `needs_review` calibration vs the 4 known-ambiguous,
- JSON-validity / parse-failure rate.

## Results (2026-07-31, FxA accounts_db_external, 207 columns)

Cost/runtime from `ab_summary.txt`; quality from `ab_scorecard.py` (see
`classification_runs/model_ab/`). Full-corpus $ extrapolated to 114,187 columns,
standard rates, no caching/batch.

`fxid_over` / `needs_rev` for gemini-3.5-flash-lite are shown post-`id`-nudge (see
below); the raw first-pass numbers were 6 / 6.

| model | s/col | ~full$ | sens_viol | fxid_over | needs_rev | vs_consensus |
|---|---|---|---|---|---|---|
| gemini-3.5-flash-lite | 6.4 | ~$222 | 0 | 2 | 4 | 75% |
| claude-haiku-4-5 | 6.5 | ~$763 | 0 | 2 | 25 | 78% |
| gemini-3.6-flash | 8.5 | ~$1,068 | 0 | 1 | 0 | 81% |
| claude-sonnet-5 | 9.9 | ~$3,371 | 0 | 1 | 29 | 79% |
| claude-opus-5 | 8.7 | ~$5,713 | 0 | 0 | 9 | 82% |

Findings:
- **All five pass the sensitive-column gate** (0 under-classifications).
- The timestamp/bool -> `fxid` prompt nudge **generalized to all models**; residual
  `fxid_over` cases are all `id` (row-PK vs account-id) columns.
- A follow-up **`id`-vs-account-id nudge** cut flash-lite's `fxid_over` 6 -> 2 (now
  tied with Haiku) with no regression: all 25 `uid` + 5 `userId` stayed
  `user.account.fxid`, sensitive gate still 0, consensus unchanged.
- `needs_review`: 3.6-flash flags nothing (no triage signal); Sonnet/Haiku over-flag
  (25-29); Opus (9) and 3.5-flash-lite (4) are best-calibrated.

**Decision: `gemini-3.5-flash-lite` + the `id` nudge.** Cheapest (~$222 full-corpus,
3.4x under Haiku), fastest, best-calibrated `needs_review`, ties Haiku on `fxid`
precision, passes the safety gate. **Fallback: `claude-haiku-4-5`.** Drop 3.6-flash
(dominated); Sonnet/Opus not worth 5-25x cost on this task.

Done: the retired default (`gemini-3.1-flash-lite-preview`, 404) was replaced with
`gemini-3.5-flash-lite` across `field_classifier.py`, `classify_table.sh`,
`classify_datasets.sh`, and `export_to_sheet.py`.

## Prod optimizations to apply to the winner (not in the A/B)

Bigger levers than the model choice; deferred until a model is chosen:

- **Prompt-cache the taxonomy** (identical every call): Claude `cache_control` on a
  system block (~90% off cached input), or Gemini context caching. Requires moving
  the taxonomy out of the user prompt into a cached prefix.
- **Batch API** (Anthropic Batch / Vertex batch): classification isn't
  latency-sensitive, so ~50% off *and* it collapses the ~258 h sequential wall-time
  into a single async batch (~1 h).
- **Tiered escalation**: cheap model for the ~97%, strong model only for
  low-confidence / `needs_review`.

## Future directions (parked, not evaluated)

Ideas for later; the A/B harness + `ab_scorecard.py` can test them cheaply (we
already demonstrated the pairwise-disagreement diff on Sonnet vs 3.6-flash).

- **Ensemble as calibrated confidence (most promising).** Run two *cheap* models
  (e.g. `gemini-3.5-flash-lite` + `claude-haiku-4-5`); auto-accept where they agree,
  route disagreements to review/escalation. Inter-model disagreement is a
  better-grounded uncertainty signal than any single model's self-reported
  `needs_review` (which we found poorly calibrated - 3.6-flash flags nothing, Sonnet
  over-flags), and two cheap models still cost a fraction of one strong model.
- **Higher thinking/effort on the hard tail (lower expected payoff).** Worth a test,
  but tempered: classification against a fixed taxonomy is recognition more than
  reasoning, and thinking-on didn't help in the A/B (Opus was not better than
  3.6-flash; thinking also caused a truncation failure). Adding *signal* (column
  description, table semantics, more sample values) will likely help ambiguous
  columns more than adding thinking.
