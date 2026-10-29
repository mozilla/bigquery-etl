-- Query for plausible_derived.download_metrics_v1
-- Daily rollup of Plausible product_download events on firefox.com, by page
-- path, session entry page, country, and the download's
-- product/platform/release channel/language.
--
-- Joins to sessions_v1 to attribute each download back to the session's
-- entry page (e.g. a campaign landing page), since
-- product_download typically fires on a later page in the funnel, not the
-- page that actually drove the download. This join is scoped to only the
-- product_download subset of events_v1 (a small fraction of all events), not
-- a general events-to-sessions join, so it's a deliberate, bounded exception
-- to the "no joins" rule the other plausible_derived tables follow.
--
-- Uses the same midnight window-split caveat as plausible_external/README.md
-- documents for events-to-sessions joins: a session that started the
-- previous day can still have events landing in today's events_v1 partition,
-- so the sessions side is widened by one day. LEFT JOIN so a download whose
-- session falls outside that one-day lookback (or otherwise fails to match)
-- is still counted, with entry_page NULL, rather than silently dropped.
SELECT
  @submission_date AS `date`,
  e.pathname,
  s.entry_page,
  e.country_code,
  e.acquisition_channel,
  e.utm_campaign,
  JSON_VALUE(e.props, '$.product') AS product,
  JSON_VALUE(e.props, '$.platform') AS platform,
  JSON_VALUE(e.props, '$.release_channel') AS release_channel,
  JSON_VALUE(e.props, '$.download_language') AS download_language,
  JSON_VALUE(e.props, '$.method') AS method,
  COUNT(*) AS download_count
FROM
  `moz-fx-data-shared-prod.plausible_external.events_v1` AS e
LEFT JOIN
  `moz-fx-data-shared-prod.plausible_external.sessions_v1` AS s
  ON e.session_id = s.session_id
  AND DATE(s.start)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
  AND @submission_date
WHERE
  DATE(e.`timestamp`) = @submission_date
  AND e.name = 'product_download'
GROUP BY
  `date`,
  e.pathname,
  s.entry_page,
  e.country_code,
  e.acquisition_channel,
  e.utm_campaign,
  product,
  platform,
  release_channel,
  download_language,
  method
