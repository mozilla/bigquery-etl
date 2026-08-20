-- Most columns come from the repo checkout, but `table_type` is resolved from
-- INFORMATION_SCHEMA and the `last_changed_*` columns from `git log`. Both can
-- degrade without failing the run: the query nulls the affected columns and
-- records why in a status column, rather than publishing misleading values or
-- discarding an otherwise-correct snapshot. That keeps a single bad day cheap,
-- but it means a permanent regression -- git history missing from the image, or
-- the service account losing a permission -- would otherwise surface only as a
-- log line in a task that still succeeded. These checks make it visible.
--
-- Change tracking depends on `.git` surviving the Docker image build with full
-- history, which nothing in this repo controls. A shallow clone or missing git
-- sets every row to `shallow_history` or `git_unavailable` and nulls all six
-- `last_changed_*` columns, so "which PR last touched this?" stops working.
-- Warn rather than fail: ownership and tier are unaffected and still worth
-- publishing.

#warn
ASSERT (
  SELECT
    COUNTIF(last_changed_status != 'found') / COUNT(*)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
) < 0.05
AS
  "More than 5% of rows have no resolved git history; check that the image ships a full .git (last_changed_status shows why)";

-- `table_type` needs INFORMATION_SCHEMA read access on every project under
-- sql/. A project the service account cannot query marks its rows
-- `project_unavailable`; entities that are genuinely undeployed are
-- `not_in_bigquery`, which is a finding rather than a fault. Leave room for the
-- known-inaccessible glam-fenix-dev and fail only on a broad loss of access.

#warn
ASSERT (
  SELECT
    COUNTIF(table_type_status = 'project_unavailable') / COUNT(*)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
) < 0.10
AS
  "More than 10% of rows could not resolve table_type because a project's INFORMATION_SCHEMA was unreadable";

-- A snapshot far smaller than the repo means the glob or the checkout is wrong.
-- The query already refuses to write zero rows; this catches a partial write.

#fail
{{ min_row_count(1000, "submission_date = @submission_date") }}
-- One row per entity per partition. A duplicate would double-count every
-- ownership-coverage denominator built on this table.

#fail
{{ is_unique(["full_table_id"], "submission_date = @submission_date") }}
-- Columns the query sets on every code path, including the metadata-parse-error
-- path. A null here means a row was built by a path that does not exist today.

#fail
{{ not_null(["full_table_id", "owner_count", "table_type_status", "last_changed_status", "run_timestamp"], "submission_date = @submission_date") }}
