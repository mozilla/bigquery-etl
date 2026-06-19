# Plan: classify all FxA (Mozilla Accounts) data

**Goal.** Run the classification pipeline over *all* Firefox Accounts / Mozilla
Accounts data as a test, once the productionized profiler is integrated (see
[`profiler_productionization_plan.md`](profiler_productionization_plan.md)).

**Prerequisite.** This depends on the profiler-integration work being done first
(in-tree production profiler -> `akomar_column_profiles_v1`, classifier reading
raw stats with optional descriptions). Do that first; this doc is the
FxA-specific layer on top.

## Why FxA is a different (and harder) test than the ads tables

The ads test was mostly Glean-sourced tables with probe definitions. FxA is
**mixed-provenance and PII-dense**, which stresses exactly the parts of the
classifier that the Glean path papers over.

## Scope: what "all FxA data" actually is

Discovered from `sql/moz-fx-data-shared-prod/` (verify table lists at runtime via
`INFORMATION_SCHEMA` - counts below are repo dir entries, approximate):

| Dataset | ~Tables | Provenance | Phase-2 probes? | Notes |
|---|---|---|---|---|
| `accounts_db_external` | ~42 | **Fivetran MySQL mirror** of the FxA backend DB | **No** (not telemetry) | **ACL-restricted** to `accounts-confidential`. The PII core: accounts, emails, tokens, devices, oauth, carts. |
| `accounts_db_nonprod_external` | ~42 | nonprod mirror | No | Likely synthetic/test data - probably **exclude** (confirm it isn't real). |
| `firefox_accounts_derived` | ~51 | Legacy server-side FxA events (auth/content/delete events, amplitude exports, sanitized docker logs) | **Sparse/none** (legacy, not Glean pings) | The legacy events core. |
| `firefox_accounts` | ~24 | Views over `firefox_accounts_derived` | n/a | Views - the profiler skips VIEW/MATERIALIZED VIEW, so these are auto-excluded. |
| `accounts_backend` + `_derived` + `_external` | ~15 | **Glean** (server-side Glean app) | **Yes** | Probe matching + `data_sensitivity` available here. |
| `accounts_frontend` + `_derived` | ~6 | **Glean** (JS Glean on accounts.firefox.com) | **Yes** | Probe matching available. |

There are also `accounts_db`, `accounts_db_derived` (small) - include if they hold
real data.

**Action:** before running, enumerate the live datasets/tables with
`INFORMATION_SCHEMA.SCHEMATA` / `.TABLES` rather than trusting this list - dataset
membership drifts. Decide explicitly whether to include the `*_nonprod_*` mirrors.

## The central challenge: partial probe coverage

The classifier was built around Glean: it fuzzy-matches columns to source-ping
probes and is told to defer to a declared `data_sensitivity`. For the
**non-Glean majority of FxA** (`accounts_db_external`, `firefox_accounts_derived`),
Phase 2 will resolve **no source ping**, so there are **no probes and no
`data_sensitivity`** to lean on. Those columns get classified from **column name
+ data type + profiled stats + the `pii_suppressed` tier** alone.

Implications:
- This is actually the most valuable signal-quality test we can run - it shows how
  the classifier does when the Glean crutch is gone.
- The raw-stats-into-prompt change from the profiler-integration plan is **load
  bearing** here, not optional. Without it, non-Glean FxA columns would reach the
  model as name + type only.
- Expect more `needs_review=true` / `low` confidence on the DB-mirror tables. That
  is correct behavior, not a bug - track the rate as a result.

## The governance blocker: restricted PII must not leak

`accounts_db_external` is `dataset_base_acl: restricted` (accounts-confidential
workgroup) because it contains raw PII. Two concrete risks:

1. **Access.** The profiler/classifier credentials must be in (or granted to) the
   accounts-confidential workgroup to read `accounts_db_external` at all.
   Verify before running, or these tables silently fail/skip.
2. **Example-value leakage.** The productionized profiler stores `example_value`
   and top `values` for every *non-suppressed* column. Its PII suppression list
   (`_PII_LEAF_NAMES`: account, email, fxa, ip, password, dob, ... + `_email`
   suffix) is **narrow** and misses many FxA-sensitive columns (`uid`,
   `auth_salt`, `verify_hash`, `kA`/`wrap_wrap_kb`, `recovery_data`, `flow_id`,
   session tokens, etc.). So raw PII would land in the profiling table - and if
   that table lives in `mozdata-nonprod.analysis`, **restricted PII has leaked
   into a less-restricted dataset.**

Mitigations (pick before running, do not skip):
- **Restrict the destination.** Write FxA profiling + classification output to a
  table whose ACL matches accounts-confidential, not the open
  `mozdata-nonprod.analysis`. (Possibly the whole FxA test should target a
  restricted dataset.)
- **And/or suppress values for the DB mirror.** Run the profiler with
  value/example capture disabled for `accounts_db_external`, or extend
  `_PII_LEAF_NAMES` with the FxA column vocabulary. Classification mostly needs
  names + null_rate + cardinality, not literal example values - dropping example
  values costs little classification signal and removes the leak.
- Confirm with data stewards (FxA / data-review) that classifying these tables
  into the chosen destination is acceptable.

## Run approach

The profiler is **dataset-scoped**, which fits "all FxA" better than the ads
per-table approach: run it **once per FxA dataset with no `--tables`** (profiles
every base table in the dataset, one `WRITE_TRUNCATE` partition, no clobber).
Give each dataset its own partition date (as in the ads plan) so several FxA
datasets coexist in one destination table. Views are auto-skipped.

Then per table: `lineage_probe_fetcher.py --table` (will no-op probes for the
non-Glean ones) + `field_classifier.py --table`.

## Eval angle (worth adding for FxA specifically)

Unlike the open-ended PoC, FxA has **knowable ground truth**: the FxA DB schema is
documented (ecosystem-platform DB reference) and many columns are unambiguously
PII. Cheap, high-value validation:
- Hand-label a set of known-sensitive FxA columns (email, uid, ip_address,
  tokens, recovery keys) and check the classifier tags them `highly_sensitive` /
  the right `user.unique_id.*` labels.
- Report precision on the `pii_suppressed` columns (these *should* all come out
  highly_sensitive) and the `needs_review` rate on the DB-mirror tables.

This would be the first real accuracy signal for the classifier - the PoC's
explicit non-goal was "no ground-truth eval," and FxA is the natural place to
start one.

## Open questions

- Destination ACL: restricted dataset vs. nonprod analysis with values suppressed?
- Include `accounts_db_nonprod_external` and other `*_nonprod_*`? (Probably no.)
- Do we have accounts-confidential read access under the credentials we'll run as?
- Is there value in classifying the legacy `firefox_accounts_derived` sanitized /
  amplitude-export tables, or scope to the live DB mirror + Glean apps?
