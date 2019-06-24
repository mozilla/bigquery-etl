CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_desktop_v1` AS
WITH
  base AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_desktop_raw_v1`),
  --
  daily AS (
  SELECT
    submission_date AS `date`,
    metrics_daily.*,
    * EXCEPT (new_profiles)
  FROM
    base,
    UNNEST(metrics) ),
  --
  new_profile_week1 AS (
  SELECT
    DATE_SUB(submission_date, INTERVAL 6 day) AS `date`,
    metrics_1_week_post_new_profile.*,
    *
  FROM
    base,
    UNNEST(metrics) ),
  --
  new_profile_week2 AS (
  SELECT
    DATE_SUB(submission_date, INTERVAL 13 day) AS `date`,
    metrics_2_week_post_new_profile.*,
    * EXCEPT (new_profiles)
  FROM
    base,
    UNNEST(metrics) ),
  --
  joined AS (
  SELECT
  * EXCEPT (submission_date,
    app_name,
    metrics,
    metrics_daily,
    metrics_1_week_post_new_profile,
    metrics_2_week_post_new_profile)
  FROM
    daily
  FULL JOIN
    new_profile_week1
  USING
    (`date`,
      usage,
      id_bucket,
      app_name,
      app_version,
      country,
      os,
      channel)
  FULL JOIN
    new_profile_week2
  USING
    (`date`,
      usage,
      id_bucket,
      app_name,
      app_version,
      country,
      os,
      channel) )
  --
SELECT
  *
FROM
  joined
