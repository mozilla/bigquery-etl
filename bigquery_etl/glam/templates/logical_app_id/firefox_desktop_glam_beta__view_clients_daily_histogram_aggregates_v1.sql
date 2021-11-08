CREATE OR REPLACE VIEW
  `{{ project }}`.glam_etl.firefox_desktop_glam_beta__view_clients_daily_histogram_aggregates_v1
AS
WITH extracted AS (
  SELECT
    *
  FROM
    `{{ project }}`.glam_etl.firefox_desktop__view_clients_daily_histogram_aggregates_v1
  WHERE
    channel = 'beta'
)
SELECT
  * EXCEPT (app_build_id, channel),
  `alekhya-test-1-322715.udf.build_seconds_to_hour`(app_build_id) AS app_build_id,
  "*" AS channel
FROM
  extracted
