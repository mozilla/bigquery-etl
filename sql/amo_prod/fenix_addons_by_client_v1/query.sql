CREATE TEMP function get_fields(m ANY TYPE) AS (
  STRUCT(
    m.submission_timestamp,
    m.client_info.client_id,
    m.sample_id,
    m.metrics.string_list.addons_enabled_addons,
    m.client_info.app_build,
    m.normalized_country_code,
    m.client_info.locale,
    m.normalized_os
  )
);

CREATE TABLE
  amo_prod.fenix_addons_by_client_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
AS
WITH unioned AS (
  SELECT
    get_fields(m1).*
  FROM
    org_mozilla_fenix.metrics AS m1
  UNION ALL
  SELECT
    get_fields(m2).*
  FROM
    org_mozilla_fenix_nightly.metrics AS m2
  UNION ALL
  SELECT
    get_fields(m3).*
  FROM
    org_mozilla_fennec_aurora.metrics AS m3
  UNION ALL
  SELECT
    get_fields(m4).*
  FROM
    org_mozilla_firefox.metrics AS m4
  UNION ALL
  SELECT
    get_fields(m5).*
  FROM
    org_mozilla_firefox_beta.metrics AS m5
),
per_client AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    array_concat_agg(addons_enabled_addons) AS addons,
    udf.mode_last(array_agg(app_build)) AS app_version,
    udf.mode_last(array_agg(normalized_country_code)) AS country,
    udf.mode_last(array_agg(locale)) AS locale,
    udf.mode_last(array_agg(normalized_os)) AS app_os,
  FROM
    unioned
  WHERE
    DATE(submission_timestamp) = @submission_timestamp
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
