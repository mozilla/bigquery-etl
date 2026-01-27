SELECT
  submission_date,
  CASE
    WHEN country IN ('US', 'CA', 'FR', 'DE', 'GB')
      THEN country
    ELSE 'OTHER'
  END AS country,
  10 * COUNT(DISTINCT client_id) AS clients,
  10 * COUNT(*) AS fill_counts
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  submission_date = @submission_date
  AND event_category = 'address'
  AND event_object = 'address_form'
  AND event_method = 'filled'
  AND normalized_channel = "release"
  AND browser_version_info.major_version > 110
  AND sample_id = 0
GROUP BY
  submission_date,
  country
