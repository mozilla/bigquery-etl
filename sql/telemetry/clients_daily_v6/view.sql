CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_daily_v6`
AS
SELECT
  submission_date AS submission_date_s3,
  * EXCEPT (
    active_experiment_id,
    active_experiment_branch,
    total_hours_sum,
    scalar_parent_dom_contentprocess_troubled_due_to_memory_sum,
    histogram_parent_devtools_developertoolbar_opened_count_sum
  ) REPLACE(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    IFNULL(geo_subdivision1, '??') AS geo_subdivision1,
    IFNULL(geo_subdivision2, '??') AS geo_subdivision2
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
