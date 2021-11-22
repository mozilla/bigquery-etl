CREATE OR REPLACE VIEW
  `{{ project }}`.glam_etl.firefox_desktop_glam_release__view_clients_daily_scalar_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `{{ project }}`.glam_etl.firefox_desktop__view_clients_daily_scalar_aggregates_v1
  WHERE
    channel = 'release'
)
SELECT
  * EXCEPT (app_build_id, channel),
  `mozfun.glam.build_seconds_to_hour`(app_build_id) AS app_build_id,
  "*" AS channel
FROM
  extracted
