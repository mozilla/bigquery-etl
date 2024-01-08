-- Aggregate meta attribution counts by country and channel.
SELECT
  DATE(submission_timestamp) AS date_attributed,

  CASE
    WHEN (metrics.string.meta_attribution_app = '382348575493443')
      THEN 'Firefox Focus for Android'
    WHEN (metrics.string.meta_attribution_app = '979253712091854')
      THEN 'Firefox Browser for Android'
    WHEN (metrics.string.meta_attribution_app = '697946762208244')
      THEN 'Firefox Nightly for Android'
    WHEN (metrics.string.meta_attribution_app IS NULL)
      THEN NULL
  END AS meta_attribution_app,

  normalized_channel,
  metadata.geo.country AS country,
  COUNT(*) AS client_count
FROM
  fenix.first_session
WHERE
  DATE(submission_timestamp) = @submission_date
  AND client_info.client_id IS NOT NULL
GROUP BY
  1,
  2,
  3,
  4;
