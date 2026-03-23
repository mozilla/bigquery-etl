# Development Workflows

This guide covers testing SQL changes in development environments before deploying to production.

## Prerequisites

- Authenticated to GCP: `gcloud auth application-default login`
- Access to a personal sandbox project (e.g., `dev-sandbox-user`) or shared dev project (e.g., `moz-fx-data-shared-dev`)

## Target-Based Development

Configure a target in `./bqetl_targets.yaml`:

```yaml
dev:
  project_id: dev-sandbox-user  # or moz-fx-data-dev
  dataset_prefix: user_{{ git.branch }}_{{ git.commit }}_{{ artifact.project_id }}
```

Use `--target dev` to run and deploy artifacts to the dev environment:

```bash
./bqetl --target dev query run telemetry_derived.clients_daily_v6 \
  --parameter=submission_date:DATE:2026-02-22 --write
```

What happens automatically:
1. Copies to `sql/dev-sandbox-user/user_<branch>_<commit>_moz_fx_data_shared_prod_telemetry_derived/clients_daily_v6/`
2. Rewrites references (deployed tables → dev, others → prod)
3. Deploys schema to `dev-sandbox-user.user_<branch>_<commit>_..._telemetry_derived.clients_daily_v6`
4. Runs query and populates data

Add generated directories to `.gitignore`:
```gitignore
sql/*-sandbox/
sql/*/dev_*/
sql/moz-fx-data-dev/
```

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
./bqetl --target dev query run --defer \
  --parameter=submission_date:DATE:2026-02-22 --write \
  telemetry_derived.clients_daily_v6
```

With `--defer`:
- References to tables already deployed in dev → dev project
- References to everything else → prod

Without `--defer` (default): all references stay pointed at prod.

### 3. Deploy artifacts without running

```bash
# Deploy table schema only (no data)
./bqetl --target dev deploy --tables telemetry_derived.clients_daily_v6

# Deploy a view definition
./bqetl --target dev deploy --views telemetry.clients_daily

# Deploy a UDF
./bqetl --target dev deploy --routines udf.normalize_metadata
```

### 4. Backfill testing

```bash
./bqetl --target dev query backfill --defer \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  telemetry_derived.clients_daily_v6
```

### 5. Share with teammates _(future)_

```bash
./bqetl --target dev share telemetry_derived.clients_daily_v6 \
  --email teammate@mozilla.com \
  --role READER
```

### 6. Clean up dev deployments _(future)_

```bash
# Clean deployments older than 7 days
./bqetl --target dev clean --older-than 7d

# Clean a specific branch
./bqetl --target dev clean --branch feature-xyz

# Clean everything for this target
./bqetl --target dev clean --all

# Dry run first
./bqetl --target dev clean --older-than 7d --dry-run
```

Deletes datasets and tables in the target BigQuery project as well as local copied files.

### 7. Handle branch renames _(future)_

```bash
./bqetl --target dev rename-branch old-feature-name new-feature-name
```

### 8. Isolated deploys (e.g. for staging) _(future)_

Use `--isolated` to rewrite **all** references to the target environment, ensuring complete isolation:

```bash
./bqetl --target stage query run --isolated telemetry_derived.clients_daily_v6
```

This creates stubs in the target environment for all referenced artifacts.

## Future Enhancements

- `--changed` flag to automatically run all git-modified queries
- `./bqetl --target dev clean` for managed cleanup
- Copy sample/limited data from prod for realistic testing
- Automatic downstream testing with `--with-downstream`
- Unify `stage deploy` via `--target stage`
- Add `--target` support to SQL generators
- `./bqetl routine test` / `./bqetl query test` wrappers around pytest
