-- See https://stackoverflow.com/a/44885334/1260237
-- and https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/decodeURIComponent
CREATE TEMPORARY FUNCTION decode_uri_component(path STRING)
RETURNS STRING DETERMINISTIC
LANGUAGE js
AS
  """
if (path == null) {
  return null;
}
try {
  return decodeURIComponent(path);
} catch (e) {
  return path;
}
""";

WITH filtered_main AS (
  SELECT
    submission_timestamp,
    client_id,
    sample_id,
    payload.info.addons,
    application.version,
    normalized_country_code,
    environment.settings.locale,
    normalized_os
  FROM
    telemetry.main
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
),
per_clients_without_addons AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    -- We always want to take the most recent seen version per
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1693308
    ARRAY_AGG(
      application.version
      ORDER BY
        mozfun.norm.truncate_version(application.version, "minor") DESC
    )[SAFE_OFFSET(0)] AS app_version,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp)
    ) AS country,
    mozfun.stats.mode_last(
      ARRAY_AGG(environment.settings.locale ORDER BY submission_timestamp)
    ) AS locale,
    mozfun.stats.mode_last(ARRAY_AGG(normalized_os ORDER BY submission_timestamp)) AS app_os,
  FROM
    filtered_main
  GROUP BY
    submission_date,
    sample_id,
    client_id
),
per_clients_just_addons_base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    payload.info.addons
  FROM
    filtered_main
  GROUP BY
    submission_date,
    client_id,
    sample_id,
    addons
),
per_clients_just_addons AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    ARRAY_CONCAT_AGG(
      -- As of 2020-07-17, we parse addon info from the payload.info.addons string
      -- because it provides more complete information compared to environment.addons.active_addons
      -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1653499
      -- Previous logic in https://github.com/mozilla/bigquery-etl/blob/55a429924beee3c31aca4fee0063d655f1d527f2/sql/amo_prod/desktop_addons_by_client_v1/query.sql
      ARRAY(
        SELECT AS STRUCT
          decode_uri_component(REGEXP_EXTRACT(TRIM(addon), "(.+):")) AS id,
          REGEXP_EXTRACT(TRIM(addon), ":(.+)") AS version
        FROM
          UNNEST(SPLIT(addons, ",")) AS addon
      )
    ) AS addons
  FROM
    per_clients_just_addons_base
  GROUP BY
    submission_date,
    sample_id,
    client_id
),
per_client AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    addons,
    app_version,
    country,
    locale,
    app_os,
  FROM
    per_clients_without_addons
  INNER JOIN
    per_clients_just_addons
  USING
    (submission_date, client_id, sample_id)
)
SELECT
  * EXCEPT (addons),
  ARRAY(
    SELECT AS STRUCT
      addon.id,
      -- Same methodology as for app_version above.
      ARRAY_AGG(addon.version ORDER BY mozfun.norm.truncate_version(addon.version, "minor") DESC)[
        SAFE_OFFSET(0)
      ] AS version,
    FROM
      UNNEST(addons) AS addon
    GROUP BY
      addon.id
  ) AS addons
FROM
  per_client
