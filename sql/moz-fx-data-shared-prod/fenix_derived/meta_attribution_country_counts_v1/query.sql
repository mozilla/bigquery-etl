SELECT
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN (metrics.string.meta_attribution_app = '382348575493443')
      THEN 'Firefox Focus for Android'
    WHEN (metrics.string.meta_attribution_app = '979253712091854')
      THEN 'Firefox Browser for Android'
    WHEN (metrics.string.meta_attribution_app = '697946762208244')
      THEN 'Firefox Nightly for Android'
    ELSE NULL
  END AS meta_attribution_app,
  normalized_channel,
  metadata.geo.country AS country,
  COUNT(*) AS ping_count
FROM
  `moz-fx-data-shared-prod.fenix.first_session`
WHERE
  {% if is_init() %}
    DATE(submission_timestamp) >= '2023-11-01'
  {% else %}
    DATE(submission_timestamp) = @submission_date
  {% endif %}
  AND client_info.client_id IS NOT NULL
GROUP BY
  submission_date,
  meta_attribution_app,
  normalized_channel,
  country;
