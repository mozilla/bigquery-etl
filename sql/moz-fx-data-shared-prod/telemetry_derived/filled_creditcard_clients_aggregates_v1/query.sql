SELECT
  submission_date,
  CASE
    WHEN country IN ('US', 'CA', 'FR', 'DE', 'GB')
      THEN country
    ELSE 'OTHER'
  END AS country,
  100 * COUNT(DISTINCT client_id) AS clients,
  100 * COUNT(*) AS fill_counts
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  submission_date = @submission_date
  AND event_category = 'creditcard'
  AND event_object = 'cc_form_v2'
  AND event_method = 'filled'
  AND normalized_channel = "release"
  AND browser_version_info.major_version > 100
  AND sample_id < 10
GROUP BY
  submission_date,
  country
