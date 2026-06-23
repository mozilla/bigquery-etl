# Development Workflows

This guide explains how to try out a query against real data in a private workspace before it goes to production.

When a query in bigquery-etl is new or changed, it is usually worth checking that it actually works (that it runs without errors and returns sensible numbers) before opening a pull request. Instead of writing to the production tables that power live dashboards and metrics, `bqetl` can run the query and save the results to a personal **development environment** that no one else sees. The query can be re-run and adjusted there until it looks right, and only then submitted for review.

This is useful for:

- **Testing a new derived table** before it becomes part of the scheduled pipeline.
- **Checking a change to an existing query** — confirming the results still look correct and nothing downstream breaks.
- **Exploring results** without the risk of overwriting or affecting production data.
- **Sharing work-in-progress** results with a teammate for feedback.

No deep familiarity with the command line is required. The examples below can be copied, with the table name and date adjusted to fit the task.

## Prerequisites

- Authenticated to GCP: `gcloud auth application-default login`
- Access to a personal sandbox project (e.g., `my-sandbox-project`) or the shared dev project `moz-fx-data-proto`

## Impersonating the shared sandbox service account

A shared service account, `bq-dev-sandbox@moz-fx-data-proto.iam.gserviceaccount.com`, can be
impersonated to run development work without holding broad production access directly. It lives
in the shared dev project `moz-fx-data-proto`, is granted write access there and **read-only**
access to production (so dev queries can still read prod sources), but has **no** production
write or deploy permissions — so nothing run through it can modify prod.

Members of the `dataplatform/developers` workgroup can impersonate it (ask a data platform admin
to be granted `roles/iam.serviceAccountTokenCreator` on it if you aren't already in that
workgroup).

The easiest way to use it is to set `impersonate_service_account` on your target in
`bqetl_targets.yaml` — `bqetl` then impersonates it automatically for every command run against
that target, with no manual `export` needed:

```yaml
dev:
  project_id: moz-fx-data-proto
  dataset: '{{ account.username }}_{{ git.branch }}'
  impersonate_service_account: bq-dev-sandbox@moz-fx-data-proto.iam.gserviceaccount.com
```

Alternatively, point application-default credentials at it directly via the environment (an
explicit env var takes precedence over the target config):

```bash
export CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=bq-dev-sandbox@moz-fx-data-proto.iam.gserviceaccount.com
```

To run a single command with your own credentials instead — e.g. when the sandbox SA lacks access
you personally have — pass `--no-impersonate`:

```bash
./bqetl --target dev --no-impersonate query run <dataset>.<table> --write
```

Either way `bqetl` uses application-default credentials, so no extra flags are needed. This is also
what makes it safe for coding agents to run, deploy, and backfill: for an agent those commands
are permitted **only** when scoped to an allow-listed non-prod `--target`
(`coding_agents.dev_project_allowlist` in `bqetl_project.yaml`) **and** impersonating the sandbox
service account — so `--no-impersonate` (or a missing SA) disables them for agents. The service
account's IAM is the backstop that prevents any production writes regardless.

The shared service account is only needed for the shared dev project. If you work in a **personal
sandbox project** you own, you can skip impersonation and instead grant the needed permissions
(e.g. `roles/bigquery.user`, `roles/aiplatform.user`) directly to your own account — or to a
service account in that project — as needed. Run without `impersonate_service_account` set (or
with `--no-impersonate`) to use your own credentials.

### Writing datasets while impersonating

Datasets `bqetl` creates are owned by you, not the service account — so when impersonating, the SA
can't write results into a new dataset unless it's also granted access. When this happens `bqetl`
asks whether to grant the SA write access, with one caveat: **granting makes the dataset readable
by everyone who can impersonate the SA**. Answer `no` (the default) for anything sensitive and use a
personal sandbox instead; answer `yes` only for non-sensitive shared dev work.

To decide without a prompt — e.g. in CI or for a human running non-interactively — set it on the
target (or override per-run with `BQETL_GRANT_IMPERSONATION_DATASET_ACCESS=1`):

```yaml
dev:
  project_id: moz-fx-data-proto
  impersonate_service_account: bq-dev-sandbox@moz-fx-data-proto.iam.gserviceaccount.com
  grant_impersonation_access: true   # SA gets WRITER on datasets it creates
```

Non-interactive runs default to **not** granting.

**Coding agents** can't give meaningful consent (they can't be prompted and can set env vars
themselves), so they may widen access **only** via an explicit `grant_impersonation_access: true`
on the target — the env var and prompt are ignored for them. Without it, an agent creates datasets
the SA can't write to (writes fail). Note this is a cooperative guardrail, not a security boundary:
`bqetl_targets.yaml` is local and agent-editable. The hard boundary is the service account's IAM —
it should not be able to grant itself dataset access.

## Target-Based Development

Configure a target in `./bqetl_targets.yaml`. When running commands from `private-bigquery-etl`, targets are still read from `bigquery-etl/bqetl_targets.yaml`.

**Per-source-dataset deployment** (one target dataset per source dataset, keeps datasets separate):
```yaml
dev:
  project_id: my-sandbox-project
  dataset_prefix: user_{{ git.branch }}_{{ git.commit }}_{{ artifact.project_id }}
```

**Single-dataset deployment** (all artifacts land in one dataset, use `artifact_prefix` to avoid name collisions):
```yaml
dev:
  project_id: my-sandbox-project
  dataset: anna_dev
  artifact_prefix: "{{ git.branch }}_{{ git.commit }}_"
```

`dataset` and `dataset_prefix` are mutually exclusive. `artifact_prefix` can be used with either.

**Shared dev project** (when a personal sandbox is not available, deploy to `moz-fx-data-proto`): since this project is shared across users, scope your artifacts with your username so they don't collide with other people's:
```yaml
dev:
  project_id: moz-fx-data-proto
  dataset_prefix: '{{ account.username }}_{{ git.branch }}_{{ artifact.project_id }}'
```

Clean up after yourself on the shared project (`./bqetl --target dev target clean --older-than 7d`), since others rely on it too.

To avoid passing `--target` on every invocation, set a default using one of these (listed in priority order):

1. `--target` flag (explicit, highest priority)
2. `BQETL_TARGET` environment variable — add to your shell profile for a per-session default:
   ```bash
   export BQETL_TARGET=dev
   ```
3. `default_target` key in `bqetl_targets.yaml`:
   ```yaml
   default_target: dev

   dev:
     project_id: my-sandbox-project
     dataset: anna_dev
   ```

Use `--target dev` to run and deploy artifacts to the dev environment:

```bash
./bqetl --target dev query run telemetry_derived.clients_daily_v6 \
  --parameter=submission_date:DATE:2026-02-22 --write
```

What happens automatically:

1. Copies to `sql/my-sandbox-project/user_<branch>_<commit>_moz_fx_data_shared_prod_telemetry_derived/clients_daily_v6/`
2. Rewrites references if `--defer-to-target` is specified (deployed tables → dev, others → prod)
3. Deploys schema to `my-sandbox-project.user_<branch>_<commit>_..._telemetry_derived.clients_daily_v6`
4. Runs query and populates data

Generated target directories are already covered by `.gitignore`. If your dev project path isn't excluded by the standard patterns, add it to `.git/info/exclude` (a local, untracked gitignore) rather than modifying `.gitignore`.

## Common Workflows

### 1. Test a single query

```bash
./bqetl --target dev query run \
  sql/moz-fx-data-shared-prod/telemetry_derived/firefox_desktop_exact_mau28_by_dimensions_v2/query.sql \
  --parameter=submission_date:DATE:2026-02-22 --write
```

`--write` is needed to persist results to the target table; without it, results are printed to the console. If the target dataset doesn't exist yet, it is created with access restricted to the user who created it.

### 2. Test multiple related queries

```bash
# Run upstream first
./bqetl --target dev query run telemetry_derived.clients_last_seen_v1 \
  --parameter=submission_date:DATE:2026-02-22 --write

# Run downstream using the dev version of upstream
./bqetl --target dev query run --defer-to-target \
  --parameter=submission_date:DATE:2026-02-22 --write \
  telemetry_derived.clients_daily_v6
```

With `--defer-to-target`:
- Rewrites references to tables already deployed in dev → dev project
- References to everything else → prod

Without `--defer-to-target` (default): no rewrites — all references stay pointed at prod.

### 3. Deploy artifacts without running

```bash
# Deploy table schema only (no data)
./bqetl --target dev deploy --tables telemetry_derived.clients_daily_v6

# Deploy a view definition
./bqetl --target dev deploy --views telemetry.clients_daily

# Deploy a UDF
./bqetl --target dev deploy --routines udf.normalize_metadata

# Deploy tables and views together, rewriting references to target
./bqetl --target dev deploy --tables --views --defer-to-target \
  telemetry_derived.clients_daily_v6
```

Supports `--defer-to-target` (same as `query run`) and `--dry-run` to preview
without making changes.

### 4. Backfill testing

Both the lower-level `query backfill` and the managed `backfill initiate`/`complete`
commands support `--target`, running into the target-deployed table and (for managed
backfills) staging into the target project so you can rehearse a backfill without
production write access. See
[Testing a backfill in a dev environment](backfilling_a_table.md#testing-a-backfill-in-a-dev-environment).

```bash
./bqetl --target dev query backfill --defer-to-target \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  telemetry_derived.clients_daily_v6
```
Managed backfill:
```bash
./bqetl --target dev backfill initiate moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6
./bqetl --target dev backfill complete moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6
```

### 5. Share with teammates

```bash
# Grant read access to all datasets for the current branch
./bqetl --target dev target share --email teammate@mozilla.com

# Grant write access
./bqetl --target dev target share --email teammate@mozilla.com --role WRITER

# Share only datasets for a specific branch
./bqetl --target dev target share --branch feature-xyz --email teammate@mozilla.com

# Narrow to datasets containing "telemetry_derived"
./bqetl --target dev target share --dataset telemetry_derived --email teammate@mozilla.com

# Share specific tables within matched datasets
./bqetl --target dev target share --table tbl1 --table tbl2 --email teammate@mozilla.com

# Combine: specific table within specific dataset
./bqetl --target dev target share --dataset telemetry_derived --table clients_daily_v6 --email teammate@mozilla.com

# Preview without sharing
./bqetl --target dev target share --dry-run --email teammate@mozilla.com
```

Datasets are discovered automatically from the target's naming pattern in
`bqetl_targets.yaml` (same as `target clean`). Use `--branch` to narrow by branch,
`--dataset` to filter by dataset name substring. Both are repeatable.

Without `--table`, grants dataset-level access to matched datasets.
With `--table`, grants table-level IAM access to specific tables within matched
datasets (repeatable). `--dataset` and `--table` can be combined.
Table-level sharing is persisted in the local manifest so it is re-applied
automatically on re-deploy.

Supports `READER` (default), `WRITER`, and `OWNER` roles.

### 6. Clean up dev deployments

```bash
# Delete tables not updated in 7 days
./bqetl --target dev target clean --older-than 7d

# Delete artifacts for a specific branch
./bqetl --target dev target clean --branch feature-xyz

# Delete all datasets for this target
./bqetl --target dev target clean --all

# Combine: delete stale tables for a specific branch
./bqetl --target dev target clean --branch feature-xyz --older-than 7d

# Dry run first
./bqetl --target dev target clean --older-than 7d --dry-run
```

Cleanup granularity is determined automatically from the target config in `bqetl_targets.yaml`:

- `--branch` checks where `git.branch` appears in the target templates. If it's in
  `dataset`/`dataset_prefix`, entire matching datasets are deleted. If it's in
  `artifact_prefix`, only matching tables within datasets are deleted.
- `--older-than` always performs table-level cleanup based on last modified time.
- `--all` deletes all matching datasets entirely.

Empty datasets are removed automatically after table-level cleanup.

Local copied files are cleaned up in both cases.

### 7. Migrate a previous branch's deployment to the current branch

```bash
# Check out the destination branch first
git checkout new-feature-name

./bqetl --target dev target migrate-branch old-feature-name

# Preview first
./bqetl --target dev target migrate-branch old-feature-name --dry-run
```

The destination branch is always the currently checked-out git branch —
the deploy step that recreates views/MVs/routines renders target names
from the current branch, so this command must be run from the branch
you're migrating to.

Replaces the sanitized old-branch string with the current-branch string
in dataset and/or artifact names, depending on where `git.branch` appears
in the target's templates. Tables are copied to their new location and
the originals deleted. Views, materialized views, and routines are
skipped during the migration; the deploy step prompted at the end of the
command recreates them at the new location from local SQL templates and
cleans up the originals.

References (FROM clauses, routine calls, manifests) in local SQL/YAML
files under the target dir are also rewritten to the new names, so
downstream queries generated under the target path keep pointing at the
migrated artifacts.

If the target template includes `git.commit`, only the most recently
modified commit's datasets are migrated — older commit deployments are
left behind (clean those up with `./bqetl target clean` if no longer
needed). The commit hash in dataset and artifact names is rewritten to
the current `HEAD`, so the next deploy lands at the matching location
instead of creating a parallel set of artifacts.

Migrated tables reflect the BigQuery state at time of migration, not the
current local SQL templates. If templates have drifted since the last
deploy, migrated tables won't pick those changes up. Accept the deploy
prompt at the end of the command — or run `./bqetl deploy` manually on
the migrated paths — to re-render everything from templates.

**Caveat: substring replacement.** Renames and local reference rewrites
are implemented as plain substring replacement of the sanitized branch
(and commit) string. A very short or generic branch name (e.g. `dev`,
`test`, `a`) can incorrectly match unrelated substrings inside identifiers
or SQL/YAML content, producing bad names or content edits. In practice
ticket-prefixed branch names (`DENG-1234-*`, `feature-xyz`) are unique
enough that this doesn't bite, and the rewrite is scoped to the target
project's dev directory — but if you use generic branch names, preview
with `--dry-run` and spot-check the migration plan before accepting.
A more robust implementation would match qualified table IDs via regex
rather than using raw substring replace.

### 8. Isolated deploys (e.g. for staging)

Use `--isolated` to deploy a fully self-contained mirror into the target —
every reference is rewritten so the deployed artifacts touch only target
project data:

```bash
./bqetl --target stage deploy --tables --views --isolated \
  telemetry_derived.clients_daily_v6
```

What `--isolated` does:

1. **Walks dependencies** of each input artifact. Managed deps (under `sql/`)
   are added as full artifacts. Unmanaged deps (live/stable tables, syndicated
   tables, etc.) get a stub written directly into the target tree
   (`sql/<target_project>/<target_dataset>/<target_artifact>/`) with a
   `schema.yaml` fetched via `client.get_table()`.
2. **Auto-discovers UDF dependencies** of every query/view/MV in the input
   set, including transitive deps. These are added to the routine publish
   step automatically; you don't need to list them in `--routines` paths.
3. **Rewrites all 3-part references** in every artifact to point at target —
   project, dataset, and artifact names rendered through the target's
   templates. Same source of truth as the stub placement, so refs land where
   the stubs are. 2-part UDF refs within UDF bodies (e.g.
   `json.extract_int_map(...)` inside `mozfun.json.extract`) are also
   rewritten.
4. **Forces these flags together**: `--table-force`, `--table-skip-external-data`,
   `--table-skip-existing-schemas`, `--view-force`, `--routines`. Schema
   updates via dry-run are skipped — the schema in the target dir is
   authoritative.
5. **Strips `CREATE MATERIALIZED VIEW … AS`** for materialized-view artifacts
   and renames them to `query.sql`, deploying as schema-only tables. (The
   target project usually can't refresh MVs because it lacks source data.)

`--isolated` is mutually exclusive with `--defer-to-target`.

#### Schema resolution

For each table without a `schema.yaml` in the target dir,
`--isolated` falls back through:

1. The `schema.yaml` copied from source (kept as-is, with `!include`
   directives flattened).
2. `client.get_table()` against the source project — fastest, works on
   partition-required tables.
3. Dry-running the rewritten query against the **target** project. UDFs and
   stubs are already published at this point, so this picks up local UDF
   changes.
4. Fail loudly with the errors from steps 2 and 3 in the message.

Tables marked `allow_field_addition` always re-derive (skip step 1) so
schema drift is captured.

#### CI parity flags

For the `stage` target (or any CI-shared target), set these on the target
in `bqetl_targets.yaml` so you don't have to remember to pass them every
time:

```yaml
stage:
  project_id: moz-fx-data-integration-tests
  dataset: "{{ artifact.dataset_id }}_{{ artifact.project_id }}_{{ git.commit }}"
  grant_dryrun_access: true     # READER for dry_run.function_accounts
  expire_after_hours: 12         # auto-GC by `target clean --delete-expired`
  rewrite_tests: true            # mirror tests/sql/<src>/ → tests/sql/<tgt>/
```

CLI flags `--rewrite-tests/--no-rewrite-tests` and `--expire-after-hours=N`
override per-run if needed. For personal `dev` targets, leave these unset —
your dev datasets stay private (no service-account access), persist across
iterations, and don't touch your `tests/sql/` tree.

## Future Enhancements

- `--changed` flag to automatically run all git-modified queries
- Copy sample/limited data from prod for realistic testing
- Automatic downstream testing with `--with-downstream`
- Unify `stage deploy` via `--target stage`
- Add `--target` support to SQL generators
- `./bqetl routine test` / `./bqetl query test` wrappers around pytest
