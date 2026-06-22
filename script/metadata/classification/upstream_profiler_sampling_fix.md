# TODO: upstream the profiler sample_id fix to main

The column profiler (`sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_v1/query.py`,
productionized in PR #9503) silently profiles some tables to **zero columns**.
This branch (`data_classification_based_on_DENG-10944`) carries a fix that should
be upstreamed to main as its own PR, decoupled from the classification PoC.

## The bug

The profiler samples 1% via `WHERE sample_id = <bucket>` whenever the table
merely *has* a `sample_id` column. Glean `events_stream` derivatives (and likely
other tables) carry the column but leave it **entirely NULL**, so `sample_id =
<bucket>` matches no rows. The scan returns 0 rows, the existing empty-slice
guard fires, and the table profiles to zero columns with only an info log
(`Scanned slice empty for <table>; no column stats emitted.`). No error, no
output - easy to miss in a batch run.

Confirmed against `moz-fx-data-shared-prod.accounts_backend_derived.events_stream_v1`
on 2026-06-20: 292B rows total, ~347M in the scanned (run_date - 7d) partition,
but `sample_id` is NULL for 100% of rows, so the 1% sample scanned 0 rows.
`users_services_first_seen_v1` in the same dataset hit it too.

This bug exists in the **deployed production profiler**, so any all-NULL
`sample_id` table on the `bqetl_data_governance_metadata` DAG is currently
profiling to nothing.

## The fix (already implemented on this branch)

Detect whether `sample_id` is actually populated; fall back to `TABLESAMPLE
SYSTEM (1 PERCENT)` when it is not. The sampling decision moved out of the SQL
builders and into the caller.

Functions changed in `query.py`:
- **New `sample_id_is_populated(client, table, partition_filter)`** - cheap
  `SELECT EXISTS(SELECT 1 ... WHERE sample_id IS NOT NULL [AND <partition>] LIMIT 1)`.
  Short-circuits via `LIMIT 1` so the common (populated) case stays cheap; only
  the rare all-NULL case scans the column over the partition.
- **`build_profile_query` / `build_nested_profile_queries`** - new
  `tablesample: bool` param. They no longer detect `has_sample_id` themselves or
  fall back to `random.randint`; they render exactly what the caller decides -
  `WHERE sample_id = <bucket>` when `sample_id is not None`, or `TABLESAMPLE
  SYSTEM (1 PERCENT)` in the FROM clause when `tablesample` is True (mutually
  exclusive).
- **`_run_profile_query`** - resolves the sampling mode: if the table has a
  `sample_id` column and it is populated, use the deterministic bucket (current
  behavior, reproducible per run date); otherwise set `tablesample=True` and log
  the fallback.
- **`profile_bq_table` docstring** - updated to mention the fallback.

Verified: builders render both paths correctly; the TABLESAMPLE query is valid
SQL against a partitioned require-partition-filter table and returns ~1% of rows
(3.4M of 347M) with real stats. `black`/`flake8`/`isort`/`pydocstyle`/`mypy` all
pass.

## Upstreaming steps

1. Cherry-pick / re-apply only the `query.py` changes onto a fresh branch off
   `main` (do **not** drag in the classification PoC files under `script/metadata/`).
   The relevant commit(s) on this branch touch only that one file for this fix.
2. Open a PR against `mozilla/bigquery-etl`, ref the original profiler ticket
   (DENG-11204 / PR #9503) and tag the profiler owner (gkatre).
3. Consider adding a unit test - there are currently **no tests** for these
   builder functions, which is why the signature refactor was safe but also why
   the bug shipped. A test asserting:
   - `build_profile_query(..., sample_id=N)` emits `sample_id = N` and no TABLESAMPLE
   - `build_profile_query(..., tablesample=True)` emits TABLESAMPLE and no `sample_id =`
   would lock in the fix.
4. Decide the reproducibility tradeoff with the owner: TABLESAMPLE is **not**
   deterministic across runs (unlike the run-date-seeded `sample_id` bucket). For
   approximate profiling stats this is acceptable, but call it out in the PR so
   it's a conscious decision. If strict reproducibility is required for the
   fallback too, an alternative is a deterministic hash sample
   (`MOD(ABS(FARM_FINGERPRINT(...)), 100) = bucket`) - but that needs a stable
   per-row column, which the generic profiler cannot assume exists.

## Related

The classifier-side fix on this branch (`field_classifier.py`
`load_ping_mapping`/`load_probes_by_ping` swallowing `NotFound` for non-Glean
datasets) is **PoC-only** and does **not** need upstreaming - those phase-2
tables and that loader live in the PoC, not in #9503.
