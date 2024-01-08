CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod`.fenix_derived.meta_attribution_country_counts_v1
AS
-- Initialization query first observations for Meta Attribution Country Counts

SELECT
  DATE(submission_timestamp) as date_attributed,

  CASE
    WHEN (
      metrics.string.meta_attribution_app = '382348575493443'
    ) THEN 'Firefox Focus for Android'
    WHEN (
      metrics.string.meta_attribution_app = '979253712091854'
    )THEN 'Firefox Browser for Android'
    WHEN (
      metrics.string.meta_attribution_app = '697946762208244'
    ) THEN 'Firefox Nightly for Android'
    WHEN (metrics.string.meta_attribution_app IS NULL)
    THEN NULL
  END as meta_attribution_app,

  normalized_channel,
  metadata.geo.country as country,
  count(*) as client_count
FROM
  fenix.first_session
WHERE
  DATE(submission_timestamp) >= '2020-06-01'
  AND client_info.client_id IS NOT NULL
GROUP BY 1, 2, 3, 4;
