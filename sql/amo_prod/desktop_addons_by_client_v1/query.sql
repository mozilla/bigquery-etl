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

WITH per_client AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    ARRAY_CONCAT_AGG(
      -- As of 2020-07-17, we parse addon info from the payload.info.addons string
      -- because it provides more complete information compared to environment.addons.active_addons
      -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1653499
      -- Previous logic in https://github.com/mozilla/bigquery-etl/blob/55a429924beee3c31aca4fee0063d655f1d527f2/sql/amo_prod/desktop_addons_by_client_v1/query.sql
      ARRAY(
        SELECT AS STRUCT
          decode_uri_component(REGEXP_EXTRACT(addon, "(.+):")) AS id,
          REGEXP_EXTRACT(addon, ":(.+)") AS version
        FROM
          UNNEST(SPLIT(payload.info.addons)) AS addon
      )
    ) AS addons,
    udf.mode_last(array_agg(application.version)) AS app_version,
    udf.mode_last(array_agg(normalized_country_code)) AS country,
    udf.mode_last(array_agg(environment.settings.locale)) AS locale,
    udf.mode_last(array_agg(normalized_os)) AS app_os,
  FROM
    telemetry.main
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
      addon.id,
      udf.mode_last(array_agg(addon.version)) AS version,
    FROM
      UNNEST(addons) AS addon
    GROUP BY
      addon.id
  ) AS addons
FROM
  per_client
