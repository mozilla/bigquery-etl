CREATE OR REPLACE VIEW
  `{{ project }}`.glam_etl.org_mozilla_fenix_glam_nightly__view_clients_daily_histogram_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `{{ project }}`.glam_etl.org_mozilla_fenix__view_clients_daily_histogram_aggregates_v1
  WHERE
    mozfun.norm.fenix_app_info('org_mozilla_fenix', app_build_id).channel = 'nightly'
  UNION ALL
  SELECT
    *
  FROM
    `{{ project }}`.glam_etl.org_mozilla_fenix_nightly__view_clients_daily_histogram_aggregates_v1
  UNION ALL
  SELECT
    *
  FROM
    `{{ project }}`.glam_etl.org_mozilla_fennec_aurora__view_clients_daily_histogram_aggregates_v1
),
with_app_build_id AS (
  SELECT
    * EXCEPT (app_build_id, channel, app_version),
    mozfun.glam.fenix_build_to_build_hour(app_build_id) AS app_build_id,
    "*" AS channel,
  FROM
    extracted
),
with_build_hour AS (
  SELECT
    *,
    mozfun.glam.build_hour_to_datetime(app_build_id) AS build_hour
  FROM
    with_app_build_id
),
with_geckoview_version_renamed AS (
  SELECT
    build_hour,
    geckoview_major_version AS app_version
  FROM
    `moz-fx-data-shared-prod`.org_mozilla_fenix.geckoview_version
)
SELECT
  * EXCEPT (build_hour)
FROM
  with_build_hour
JOIN
  with_geckoview_version_renamed
  USING (build_hour)
