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
distinct_client_count_nightly_base AS (
    SELECT
        client_info.client_id,
        "nightly" AS channel,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel = "nightly"
    -- NOTE: the below two tables are marked as depricated inside the GLEAN dictionary
    -- however, they are still considered when generating active_users_aggregates metrics
    -- this is why they are being considered here.
    UNION ALL
    SELECT
        client_info.client_id,
        "nightly" AS channel,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND mozfun.norm.fenix_app_info("org_mozilla_fenix_nightly", client_info.app_build).channel = "nightly"
    UNION ALL
    SELECT
        client_info.client_id,
        "nightly" AS channel,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND mozfun.norm.fenix_app_info("org_mozilla_fennec_aurora", client_info.app_build).channel = "nightly"
),
distinct_client_count_base AS (
    -- release channel
    SELECT
        client_info.client_id,
        "release" AS channel,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND mozfun.norm.fenix_app_info("org_mozilla_firefox", client_info.app_build).channel = "release"
    -- beta channel
    UNION ALL
    SELECT
        client_info.client_id,
        "beta" AS channel,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND mozfun.norm.fenix_app_info("org_mozilla_firefox_beta", client_info.app_build).channel = "beta"
    -- NOTE: nightly table also contains some entries considered to be "beta" channel by our ETL
    -- this is why the below entries are included here.
    UNION ALL
    SELECT
        client_info.client_id,
        "beta" AS channel,
    FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_live.baseline_v1`
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel = "beta"
    -- nightly channel
    UNION ALL
    SELECT
        client_id,
        channel
    FROM
        distinct_client_count_nightly_base
    LEFT JOIN
        `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen` AS baseline_clients_last_seen
    USING
        (client_id)
    WHERE
        baseline_clients_last_seen.submission_date = @submission_date
        AND baseline_clients_last_seen.days_since_seen = 0
),
distinct_client_counts_per_channel AS (
    SELECT
        channel,
        COUNT(DISTINCT client_id) AS distinct_client_count,
    FROM
        distinct_client_count_base
    GROUP BY
        channel
),
distinct_client_count AS (
    SELECT
        SUM(distinct_client_count),
    FROM
        distinct_client_counts_per_channel
)
SELECT
    IF(
        ABS((SELECT * FROM dau_sum) - (SELECT * FROM distinct_client_count)) > 1,
        ERROR("DAU mismatch between aggregates table and live table"),
        NULL
    );

{#
# kept this config file in case we decided to run a check for each channel independently.

#warn
WITH

release_dau_sum AS (
  SELECT
    SUM(dau) AS dau_sum,
  FROM
    `{{ project_id }}.{{ dataset }}.{{ table }}`
  WHERE
    submission_date = @submission_date
    AND channel = "release"
),

release_distinct_client_count AS (
  SELECT
    COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info("org_mozilla_firefox", client_info.app_build).channel = "release"
)
SELECT
  IF(
    (SELECT * FROM release_dau_sum) <> (SELECT * FROM release_distinct_client_count),
    ERROR("DAU mismatch between aggregates table and live table"),
    NULL
  );

#warn
beta_dau_sum AS (
  SELECT
    SUM(dau) AS dau_sum,
  FROM
    `{{ project_id }}.{{ dataset }}.{{ table }}`
  WHERE
    submission_date = @submission_date
  AND channel = "beta"
),
base AS (
  SELECT
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info("org_mozilla_firefox_beta", client_info.app_build).channel = "beta"
  -- NOTE: nightly table also contains some entries considered to be "beta" channel by our ETL
  -- this is why the below entries are included here.
  UNION ALL
  SELECT
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel = "beta"
),
beta_distinct_client_count AS (
  SELECT
    COUNT(DISTINCT client_id) AS distinct_client_count,
  FROM
    base
)
SELECT
  IF(
    (SELECT * FROM beta_dau_sum) <> (SELECT * FROM beta_distinct_client_count),
    ERROR("DAU mismatch between aggregates table and live table"),
    NULL
  );

#warn
WITH
nightly_dau_sum AS (
  SELECT
    SUM(dau) AS dau_sum,
  FROM
    `{{ project_id }}.{{ dataset }}.{{ table }}`
  WHERE
    submission_date = @submission_date
    AND channel = "nightly"
),
base AS (
  SELECT
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel = "nightly"
  -- NOTE: the below two tables are marked as depricated inside the GLEAN dictionary
  -- however, they are still considered when generating active_users_aggregates metrics
  -- this is why they are being considered here.
  UNION ALL
  SELECT
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info("org_mozilla_fenix_nightly", client_info.app_build).channel = "nightly"
  UNION ALL
  SELECT
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info("org_mozilla_fennec_aurora", client_info.app_build).channel = "nightly"
),
nightly_distinct_client_count AS (
  SELECT
    COUNT(DISTINCT client_id) AS distinct_client_count,
  FROM
    base
)
SELECT
  IF(
    (SELECT * FROM nightly_dau_sum) <> (SELECT * FROM nightly_distinct_client_count),
    ERROR("DAU mismatch between aggregates table and live table"),
    NULL
  );
#}
{% endraw %}

