{#
   We use raw here b/c the first pass is rendered to create the checks.sql
   files, and the second pass is rendering of the checks themselves.
   Without/outside the {% raw %} the macros would be rendered for every
   check file when we create the checks file, when `bqetl generate active_users`
   is called.
   Inside the {% raw %} the checks get rendered when we _run_ the check,
   during `bqetl query backfill`.
   (you can also run them locally with `bqetl check run`).
#}
{% raw -%}
#warn
WITH
dau_sum AS (
    SELECT
        SUM(dau),
    FROM
        `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
    WHERE
        submission_date = @submission_date

),
distinct_client_count_base AS (
    -- release channel
    SELECT
        COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_focus_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
    -- beta channel
    UNION ALL
    SELECT
        COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_focus_beta_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
    -- nigthly channel
    UNION ALL
    SELECT
        COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_focus_nightly_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
),
distinct_client_count AS (
    SELECT
        SUM(distinct_client_count)
    FROM
        distinct_client_count_base
)
SELECT
    IF(
        (SELECT * FROM dau_sum) <> (SELECT * FROM distinct_client_count),
        ERROR("DAU mismatch between aggregates table and live table"),
        NULL
    );
{% endraw %}
