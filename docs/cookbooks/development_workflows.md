# Development Workflows

This guide covers testing SQL changes in development environments before deploying to production.

## Prerequisites

- Authenticated to GCP: `gcloud auth application-default login`
- Access to a personal sandbox project (e.g., `dev-sandbox-user`) or shared dev project (e.g., `moz-fx-data-shared-dev`)

## Target-Based Development

Configure a target in `./bqetl_targets.yaml`. When running commands from `private-bigquery-etl`, targets are still read from `bigquery-etl/bqetl_targets.yaml`.

**Per-source-dataset deployment** (one target dataset per source dataset, keeps datasets separate):
```yaml
dev:
  project_id: dev-sandbox-user
  dataset_prefix: user_{{ git.branch }}_{{ git.commit }}_{{ artifact.project_id }}
```

**Single-dataset deployment** (all artifacts land in one dataset, use `artifact_prefix` to avoid name collisions):
```yaml
dev:
  project_id: dev-sandbox-user
  dataset: anna_dev
  artifact_prefix: "{{ git.branch }}_{{ git.commit }}_"
```

`dataset` and `dataset_prefix` are mutually exclusive. `artifact_prefix` can be used with either.

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
     project_id: dev-sandbox-user
     dataset: anna_dev
   ```

Use `--target dev` to run and deploy artifacts to the dev environment:

```bash
./bqetl --target dev query run telemetry_derived.clients_daily_v6 \
  --parameter=submission_date:DATE:2026-02-22 --write
```

What happens automatically:

1. Copies to `sql/dev-sandbox-user/user_<branch>_<commit>_moz_fx_data_shared_prod_telemetry_derived/clients_daily_v6/`
2. Rewrites references if `--defer-to-target` is specified (deployed tables → dev, others → prod)
3. Deploys schema to `dev-sandbox-user.user_<branch>_<commit>_..._telemetry_derived.clients_daily_v6`
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

### 4. Backfill testing  _(future)_

```bash
./bqetl --target dev query backfill --defer-to-target \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  telemetry_derived.clients_daily_v6
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

Migrated tables reflect the BigQuery state at time of migration, not the
current local SQL templates. If templates have drifted since the last
deploy, migrated tables won't pick those changes up. Accept the deploy
prompt at the end of the command — or run `./bqetl deploy` manually on
the migrated paths — to re-render everything from templates.

### 8. Isolated deploys (e.g. for staging) _(future)_

Use `--isolated` to rewrite **all** references to the target environment, ensuring complete isolation:

```bash
./bqetl --target stage query run --isolated telemetry_derived.clients_daily_v6
```

This creates stubs in the target environment for all referenced artifacts.

## Future Enhancements

- `--changed` flag to automatically run all git-modified queries
- Copy sample/limited data from prod for realistic testing
- Automatic downstream testing with `--with-downstream`
- Unify `stage deploy` via `--target stage`
- Add `--target` support to SQL generators
- `./bqetl routine test` / `./bqetl query test` wrappers around pytest
