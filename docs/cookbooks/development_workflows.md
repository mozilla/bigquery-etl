# Development and Testing Workflows

This guide covers different approaches to testing SQL changes in development environments before deploying to production.

## Overview

When developing queries, views, or UDFs, you typically need to:
1. Test your changes in isolation
2. Test with upstream and downstream dependencies
3. Validate with historical data (backfills)
4. Share results with teammates for review

This guide covers workflows for each scenario.

## Prerequisites

- Authenticated to GCP: `gcloud auth application-default login`
- Access to either:
  - Personal sandbox project (e.g., `dev-sandbox-user`)
  - Shared dev project (e.g., `moz-fx-data-shared-dev`)

## Target-Based Development

Configure a target for your development environment in `~/.bigquery_etl/targets.yaml`:

```yaml
targets:
  dev:
    project_id: dev-sandbox-user  # or moz-fx-data-dev
    dataset_prefix: user_{{ git.branch }}_{{ git.commit }}
```

Then use `--target dev` with commands to work in your dev environment:

```bash
# Run query - automatically deploys if needed
./bqetl --target dev query run telemetry_derived.clients_daily_v6

# What happens automatically:
# 1. Copies to sql/dev-sandbox-user/user_main_abc123_telemetry_derived/clients_daily_v6/
# 2. Rewrites references (smart: deployed tables → dev, others → prod)
# 3. Deploys schema to dev-sandbox-user.user_main_abc123_telemetry_derived.clients_daily_v6
# 4. Runs query and populates data
```

The target tells bqetl where to find/deploy queries:
- Without `--target`: uses source at `sql/moz-fx-data-shared-prod/...`
- With `--target dev`: looks for `sql/{target.project_id}/...` first, auto-deploys if missing

## Common Workflows

### 1. Testing a Single Query Change

**Scenario:** You fixed a bug in a query and want to test it.

```bash
# Edit the query locally
vim sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/query.sql

# Run in dev - single command does everything
./bqetl --target dev query run telemetry_derived.clients_daily_v6
```

**What happens automatically:**

First run:
```
ℹ️  Deploying to dev (first time)...
✅ Copied to sql/dev-sandbox-user/user_main_abc123_telemetry_derived/clients_daily_v6/
✅ Rewritten references
✅ Deployed schema to dev-sandbox-user.user_main_abc123_telemetry_derived.clients_daily_v6
✅ Created dataset with user-only access
⏳ Running query...
✅ Query completed
```

Subsequent runs (if source unchanged):
```
ℹ️  Using existing dev deployment
⏳ Running query...
✅ Query completed
```

After editing source:
```
ℹ️  Source changed, updating dev deployment...
✅ Updated deployment
⏳ Running query...
✅ Query completed
```

**Key behaviors:**
- Creates `sql/{target.project_id}/{prefix}{dataset}/{table}/` directory locally
- References rewriting: opt-in via `--defer` flag (default: use prod tables)
- Dataset access: user-only (private) by default

### 2. Testing Multiple Related Queries

**Scenario:** You modified both `clients_last_seen_v1` (upstream) and `clients_daily_v6` (downstream).

**Option A: Run each query with reference rewriting**
```bash
# Run upstream first
./bqetl --target dev query run telemetry_derived.clients_last_seen_v1

# Run downstream - use dev version of upstream with --defer
./bqetl --target dev query run --defer \
  telemetry_derived.clients_daily_v6
```

With `--defer`:
- References to deployed dev tables → `dev-sandbox-user.dev_dataset.table`
- References to non-deployed tables → `moz-fx-data-shared-prod.dataset.table` (prod)
- Uses dev version of `clients_last_seen_v1` if it exists in dev project

**Without `--defer` (default):**
```bash
./bqetl --target dev query run telemetry_derived.clients_daily_v6
```
- All references remain pointing to prod tables
- Useful for testing query logic without dependencies

Each `query run` automatically deploys and populates data.

**Option B: Pre-deploy multiple, then run**
```bash
# Deploy schemas for both (no data yet)
./bqetl --target dev deploy --defer \
  telemetry_derived.clients_last_seen_v1 \
  telemetry_derived.clients_daily_v6

# Then run them
./bqetl --target dev query run telemetry_derived.clients_last_seen_v1
./bqetl --target dev query run --defer \
  telemetry_derived.clients_daily_v6
```

Useful when you want to set up multiple tables before running any queries.

**Note:** The `--defer` flag on `deploy` creates dev copies with rewritten SQL. When running queries, use the same flag to ensure they use the dev versions of dependencies.

**Option C: Deploy multiple artifacts with dependencies (future)**

Deploy several related tables, views, and routines with automatic dependency resolution:

```bash
./bqetl --target dev deploy --defer \
  telemetry_derived.clients_last_seen_v1 \
  telemetry_derived.clients_daily_v6 \
  telemetry.clients_daily \
  udf.normalize_metadata
```

**What this does automatically:**
1. Resolves paths from names (tables, views, routines)
2. Detects dependencies between all artifacts
3. Copies to `sql/dev-sandbox-user/dev_*/`
4. Rewrites references with `--defer` (deployed artifacts → dev, others → prod)
5. Deploys in dependency order (UDFs → tables → views)
6. Includes all dependent artifacts

**Without `--defer`:**
- Deploys schemas to dev project but keeps all references pointing to prod
- Useful for testing schema changes without changing query logic


### 3. When to Use `deploy` vs `query run`

**Use `query run --target dev` for tables/queries (most common):**
- Iterative development: edit → run → see results → repeat
- Automatically deploys if needed
- Single command for the full cycle
- Populates table with data

**Use `deploy --target dev` when you need to:**
- Set up table schemas without running queries yet
- Deploy views (views don't "run", they're just definitions)
- Deploy routines/UDFs (same - just definitions)
- Deploy multiple artifacts before running any queries
- Separate deployment from execution (e.g., CI pipelines)

```bash
# deploy creates schema/definition only
./bqetl --target dev deploy telemetry_derived.clients_daily_v6
# Table exists but is empty

./bqetl --target dev deploy telemetry.clients_daily
# View exists (definition deployed)

./bqetl --target dev deploy udf.normalize_metadata
# UDF exists (definition deployed)

# query run automatically deploys + runs + populates data
./bqetl --target dev query run telemetry_derived.clients_daily_v6
# Table exists and has data
```

**The unified `deploy` command supports:**
- Tables/queries (via `--tables`, default on)
- Views (via `--views`, default on)
- Routines/UDFs (via `--routines`, default on)

### 4. Backfill Testing

Test historical data processing - automatically deploys if needed:

```bash
# Backfill using dev dependencies (if deployed)
./bqetl --target dev query backfill --defer \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  telemetry_derived.clients_daily_v6

# Or backfill using prod dependencies
./bqetl --target dev query backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  telemetry_derived.clients_daily_v6

# On first run:
# ℹ️  Deploying to dev...
# ⏳ Backfilling 2024-01-01...
# ⏳ Backfilling 2024-01-02...
# ...
```

### 5. View and UDF Testing

**Using unified deploy command (future):**
```bash
# Deploy everything (tables, views, routines)
./bqetl --target dev deploy \
  telemetry_derived.clients_daily_v6 \
  telemetry.clients_daily \
  udf.normalize_metadata

# Deploy only views
./bqetl --target dev deploy --views --no-tables --no-routines \
  telemetry.clients_daily

# Deploy only routines
./bqetl --target dev deploy --routines --no-tables --no-views \
  udf.normalize_metadata
```

**Current commands:**
```bash
# Deploy views
./bqetl --target dev view publish telemetry.clients_daily

# Deploy UDFs/routines
./bqetl --target dev routine publish udf.normalize_metadata
```

Views and routines are automatically copied to dev directory and references are rewritten just like queries.

### 6. Development Environment Management

**Sharing with teammates:**

```bash
# Share access to a specific table (future)
./bqetl --target dev share telemetry_derived.clients_daily_v6 \
  --email teammate@mozilla.com \
  --role READER

# Share access to entire dataset (future)
./bqetl --target dev share --dataset dev_telemetry_derived \
  --email teammate@mozilla.com \
  --role READER

# Share multiple tables for review
./bqetl --target dev share \
  telemetry_derived.clients_daily_v6 \
  telemetry.clients_daily \
  --email teammate@mozilla.com \
  --with-link  # also prints BigQuery console link
```

**Cleanup old deployments:**

```bash
# List target deployments (future)
./bqetl --target dev list
# Output:
# dev-sandbox-user.dev_main_abc123_telemetry_derived (created 2 days ago, 5.2 GB)
# dev-sandbox-user.dev_feature_xyz789_telemetry (created 14 days ago, 8.1 GB)
# Total: 13.3 GB across 2 datasets

# Clean deployments older than 7 days
./bqetl --target dev clean --older-than 7d

# Clean specific branch
./bqetl --target dev clean --branch feature-xyz

# Clean all deployments for this target
./bqetl --target dev clean --all

# Dry run to see what would be deleted
./bqetl --target dev clean --older-than 7d --dry-run
```

What gets cleaned:
- BigQuery datasets matching target prefix in target project
- Local `sql/{target-project}/` directories
- Optionally: schema cache files

**Handle branch renames:**

```bash
# When you rename a git branch, datasets need updating (future)
./bqetl --target dev rename-branch old-feature-name new-feature-name

# What this does:
# 1. Finds datasets with old branch name in prefix
# 2. Renames datasets: dev_old_feature_abc123 → dev_new_feature_abc123
# 3. Updates local sql/{project}/dev_new_feature_*/ directories
# 4. Preserves data and schema

# Or detect automatically from git
./bqetl --target dev migrate-branch
# Detects: Previous branch was "old-feature", current is "new-feature"
# Migrate datasets? [Y/n]
```

**Inspect what's deployed:**

```bash
# Show deployed artifacts for target (future)
./bqetl --target dev status

# Output:
# Using target: dev (dev-sandbox-user, prefix: dev_main_abc123_)
#
# Deployed tables (3):
#   ✓ telemetry_derived.clients_daily_v6 (deployed 2h ago, 1.2 GB)
#   ✓ telemetry_derived.clients_last_seen_v1 (deployed 1h ago, 500 MB)
#   ✗ telemetry_derived.ssl_ratios_v1 (source modified, needs update)
#
# Deployed views (1):
#   ✓ telemetry.clients_daily (deployed 2h ago)
#
# Total: 1.7 GB

# Check if local changes need deployment
./bqetl --target dev diff
# Shows which files have changed since last deployment
```


## Best Practices

### 1. Use Personal Dataset Prefixes

In shared dev projects, use your name as prefix to avoid conflicts:

```yaml
targets:
  dev:
    project_id: moz-fx-data-shared-dev
    dataset_prefix: dev_user_  # includes your name
```

### 2. Default to Private Datasets

Dev datasets should be private (user-only access) by default. This is automatic when using `--target dev` or stage deploy.

### 3. Only Deploy What You Changed

Don't deploy entire dependency trees. Deploy only:
- Queries you modified
- Direct dependencies you also modified

Unchanged upstream tables should reference prod data for realistic testing.

### 4. Clean Up Old Deployments

```bash
# List dev datasets
bq ls --project_id=dev-sandbox-user

# Delete old dataset
bq rm -r -f dev-sandbox-user:dev_telemetry_derived

# Or clean up rewritten SQL files
rm -rf sql/dev-sandbox-user/
```

### 5. Inspect Rewritten SQL

When using stage deploy, always check the rewritten SQL to understand what references changed:

```bash
# View rewritten query
cat sql/dev-sandbox-user/dev_telemetry_derived/clients_daily_v6/query.sql

# Compare to original
diff sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/query.sql \
     sql/dev-sandbox-user/dev_telemetry_derived/clients_daily_v6/query.sql
```

### 6. Don't Commit Generated SQL

Add dev deployments to `.gitignore`:

```gitignore
# Dev deployments - don't commit
sql/*-sandbox/
sql/*/dev_*/
sql/moz-fx-data-dev/
```

## Reference Rewriting Behavior

Two flags control how query references are rewritten:

### `--defer` (for dev testing)

**Smart rewriting**: Only rewrites references to tables you deployed to dev.

**Query references are rewritten as:**
- Tables deployed to dev → `dev-sandbox-user.dev_dataset.table`
- Tables not deployed → `moz-fx-data-shared-prod.dataset.table` (prod)

**Example:**

```sql
-- Original query
SELECT * FROM `moz-fx-data-shared-prod`.telemetry_derived.clients_last_seen_v1
JOIN `moz-fx-data-shared-prod`.telemetry.main

-- With --defer, if clients_last_seen_v1 is deployed to dev but main is not:
SELECT * FROM `dev-sandbox-user`.dev_telemetry_derived.clients_last_seen_v1
JOIN `moz-fx-data-shared-prod`.telemetry.main  -- still prod
```

This "smart rewriting" allows you to test your changes while using prod data for unchanged dependencies.

**Use case:** Development and testing where you want to use real prod data for dependencies.

### `--isolated` (for stage deploys)

**Complete rewriting**: Rewrites ALL references to point to the target environment.

**Query references are rewritten as:**
- All `moz-fx-data-shared-prod` references → target project and dataset
- Applies dataset prefix to all datasets

**Example:**

```sql
-- Original query
SELECT * FROM `moz-fx-data-shared-prod`.telemetry_derived.clients_last_seen_v1
JOIN `moz-fx-data-shared-prod`.telemetry.main

-- With --isolated and --target stage (project: stage-project, prefix: stage_):
SELECT * FROM `stage-project`.stage_telemetry_derived.clients_last_seen_v1
JOIN `stage-project`.stage_telemetry.main  -- also rewritten, even if not deployed
```

This ensures complete isolation - all queries run against the stage environment only.

**Use case:** CI/CD stage deployments where you want complete environment isolation.

### Without `--defer` (default)

All references remain pointing to production tables, regardless of what's deployed in dev:

```bash
# Deploy to dev but keep references to prod
./bqetl --target dev deploy telemetry_derived.clients_daily_v6

# Run in dev, but still queries prod tables
./bqetl --target dev query run telemetry_derived.clients_daily_v6
```

**Use cases:**
- Testing schema changes without affecting query logic
- Validating query syntax in dev environment
- Testing queries in isolation from dev dependencies

### When to use each approach

**No rewriting flags (default):**
- Testing a single query in isolation
- You want to use prod data for all dependencies
- Testing schema-only changes
- Quick validation of query logic

**Use `--defer` when:**
- Testing changes across multiple related queries in dev
- You've deployed upstream dependencies to dev
- You want to validate data pipeline logic with some dev tables
- Development workflow where most dependencies use prod data

**Use `--isolated` when:**
- CI/CD stage deployments
- Complete environment isolation required
- Integration testing in a separate environment
- Want all queries to run against stage/test data only
- Preventing any accidental reads from prod


## Future Enhancements

Additional planned improvements:

1. **Auto-detection of changed files:**
   ```bash
   ./bqetl --target dev query run --changed  # uses git diff
   # Automatically runs all queries you've modified
   ```

4. **Copy sample/limited data from prod:**
   ```bash
   # Copy a single partition for testing
   ./bqetl --target dev copy-sample telemetry.main \
     --partition 2024-01-01 \
     --limit 10000

   # Copy last N days
   ./bqetl --target dev copy-sample telemetry_derived.clients_daily_v6 \
     --last-n-days 7

   # Copy random sample
   ./bqetl --target dev copy-sample telemetry.main \
     --sample-percent 0.1  # 0.1% of data

   # Use case:
   # - Test query logic with realistic prod data
   # - Faster iteration (smaller data = faster queries)
   # - Lower costs (less data scanned)
   # - Create test fixtures automatically
   ```

7. **Automatic downstream testing:**
   ```bash
   # Run changed queries + immediate downstream dependencies
   ./bqetl --target dev query run --changed --with-downstream

   # Or for specific query
   ./bqetl --target dev query run clients_last_seen_v1+

   # Detects if your changes break downstream tables/queries
   # Runs in dependency order: upstream → changed → downstream
   # Useful for catching schema changes, logic errors, etc.
   ```

8. **Unify stage deploy with targets:**
   Replace the separate `stage deploy` command with `--target stage`:
   ```bash
   # Instead of:
   ./bqetl stage deploy --dataset-suffix=pr123 paths...

   # Use:
   ./bqetl --target stage deploy --isolated paths...
   # Or
   ./bqetl --target stage query run --isolated --changed

   # Benefits:
   # - Consistent workflow across dev, stage, and prod
   # - Reduces special-case code
   # - stage becomes just another target configuration
   # - CI can use same commands as local dev
   # - --isolated provides complete environment isolation
   ```

   **What needs to change:**
   - `deploy` command accepts multiple query names (not just paths)
   - Automatic dependency detection and topological sorting
   - Remove stage-specific flags (now in target config)
   - `--isolated` flag for complete reference rewriting (stage use case)
   - `--defer` flag for smart reference rewriting (dev use case)
   - See comparison in [Testing Multiple Related Queries](#2-testing-multiple-related-queries) Option C

9. **Better handling of generated SQL:**
   ```bash
   # Regenerate SQL for specific app in dev
   ./bqetl generate glean_app --target dev --app-id fenix

   # Deploy generated queries to dev
   ./bqetl --target dev deploy --generated org_mozilla_firefox.*

   # Test template changes by regenerating in dev
   ./bqetl generate glean_app --target dev --all
   ./bqetl --target dev query run --changed --generated-only
   ```

   **Challenges to address:**
   - Generated SQL files shouldn't be committed to dev directories
   - Template changes should trigger regeneration in dev
   - Need to distinguish template changes from generated output changes
   - Testing generated SQL requires regeneration in target environment
   - Generators need to support target-aware output paths

   **Potential solutions:**
   - Add `--target` support to all generators
   - Generators write to `sql/{target-project}/` when target specified
   - Add `.generated` metadata to track generated files
   - `deploy` command auto-regenerates if template changed
   - `clean` command removes generated files in target directories
