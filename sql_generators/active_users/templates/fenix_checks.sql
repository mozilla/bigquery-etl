{#
   We use raw here b/c the first pass is rendered to create the checks.sql
   files, and the second pass is the rendering of the checks themselves.
   Without/outside the {% raw %} the macros would be rendered for every
   check file when we create the checks file, when `bqetl generate active_users`
   is called.
   Inside the {% raw %} the checks get rendered when we _run_ the check,
   during `bqetl query backfill`.
   (you can also run them locally with `bqetl check run`).
#}
{% raw -%}
#warn
WITH daily_users_sum AS (
  SELECT
    SUM(daily_users),
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
    -- NOTE: The next two tables `org_mozilla_fenix_nightly_live.baseline_v1` and `org_mozilla_fennec_aurora_live.baseline_v1`
    -- are not used as application IDs in Glean, but are also not yet marked as deprecated because they still count for KPIs:
    -- Related PR https://github.com/mozilla/probe-scraper/pull/640.
  UNION ALL
  SELECT
    client_info.client_id,
    "nightly" AS channel,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info(
      "org_mozilla_fenix_nightly",
      client_info.app_build
    ).channel = "nightly"
  UNION ALL
  SELECT
    client_info.client_id,
    "nightly" AS channel,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.norm.fenix_app_info(
      "org_mozilla_fennec_aurora",
      client_info.app_build
    ).channel = "nightly"
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
    AND mozfun.norm.fenix_app_info(
      "org_mozilla_firefox_beta",
      client_info.app_build
    ).channel = "beta"
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
    USING (client_id)
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
    ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count)) > 10,
    ERROR(
      CONCAT(
        "Daily_users mismatch between the firefox_ios live (`org_mozilla_firefox_live`, `org_mozilla_fenix_live.baseline_v1`,`org_mozilla_firefox_beta_live.baseline_v1`,`org_mozilla_fenix_nightly_live.baseline_v1`, `org_mozilla_fennec_aurora_live.baseline_v1`) and active_users_aggregates (`{{ dataset_id }}.{{ table_name }}`) tables is greater than 10.",
        " Live table count: ",
        (SELECT * FROM distinct_client_count),
        " | active_users_aggregates (DAU): ",
        (SELECT * FROM daily_users_sum),
        " | Delta detected: ",
        ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count))
      )
    ),
    NULL
  );

{% endraw %}
