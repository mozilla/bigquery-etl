CREATE TEMP FUNCTION get_fields(m ANY TYPE) AS (
  STRUCT(
    m.submission_timestamp,
    m.client_info.client_id,
    m.sample_id,
    m.metrics.string_list.addons_enabled_addons,
    m.normalized_country_code,
    m.client_info.locale,
    m.normalized_os
  )
);

WITH unioned AS (
  SELECT
    get_fields(release).*,
    client_info.app_display_version AS app_version,
  FROM
    org_mozilla_firefox.metrics AS release
  UNION ALL
  SELECT
    get_fields(beta).*,
    -- Bug 1669516 We choose to show beta versions as 80.0.0b1, etc.
    REPLACE(client_info.app_display_version, '-beta.', 'b') AS app_version,
  FROM
    org_mozilla_firefox_beta.metrics AS beta
  UNION ALL
  SELECT
    get_fields(nightly).*,
    -- Bug 1669516 Nightly versions have app_display_version like "Nightly <timestamp>",
    -- so we take the geckoview version instead.
    nightly.metrics.string.geckoview_version AS app_version,
  FROM
    org_mozilla_fenix.metrics AS nightly
  UNION ALL
  SELECT
    get_fields(preview_nightly).*,
    preview_nightly.metrics.string.geckoview_version AS app_version,
  FROM
    org_mozilla_fenix_nightly.metrics AS preview_nightly
  UNION ALL
  SELECT
    get_fields(old_fenix_nightly).*,
    old_fenix_nightly.metrics.string.geckoview_version AS app_version,
  FROM
    org_mozilla_fennec_aurora.metrics AS old_fenix_nightly
),
cleaned AS (
  SELECT
    * REPLACE (
      IF(
        -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
        REGEXP_CONTAINS(app_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
        app_version,
        NULL
      ) AS app_version
    )
  FROM
    unioned
),
per_client AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    ARRAY_CONCAT_AGG(addons_enabled_addons ORDER BY submission_timestamp) AS addons,
    -- We always want to take the most recent seen version per
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1693308
    ARRAY_AGG(app_version ORDER BY mozfun.norm.truncate_version(app_version, "minor") DESC)[
      SAFE_OFFSET(0)
    ] AS app_version,
    udf.mode_last(ARRAY_AGG(normalized_country_code)) AS country,
    udf.mode_last(ARRAY_AGG(locale)) AS locale,
    udf.mode_last(ARRAY_AGG(normalized_os)) AS app_os,
  FROM
    cleaned
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
  GROUP BY
    submission_date,
    sample_id,
    client_id
)
SELECT
  * EXCEPT (addons),
  ARRAY(
    SELECT AS STRUCT
      addon,
      -- As of 2020-07-01, the metrics ping from Fenix contains no data about
      -- the version of installed addons, so we inject null and replace with
      -- an appropriate placeholder value when we get to the app-facing view.
      CAST(NULL AS STRING) AS version,
    FROM
      UNNEST(addons) AS addon
    GROUP BY
      addon
  ) AS addons
FROM
  per_client
