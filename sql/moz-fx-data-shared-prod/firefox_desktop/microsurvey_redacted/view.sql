-- Generated via ./bqetl generate stable_views_redacted
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.microsurvey_redacted`
AS
SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        metrics.* REPLACE (
          (SELECT AS STRUCT metrics.text2.* EXCEPT (microsurvey_event_input_value)) AS text2
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.firefox_desktop.microsurvey`
