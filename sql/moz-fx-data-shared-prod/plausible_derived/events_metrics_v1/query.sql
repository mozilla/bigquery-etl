-- Query for plausible_derived.events_metrics_v1
-- Daily rollup of Plausible events on firefox.com, by page path, country,
-- and event name. Single scan of events_v1, no joins. Covers every event
-- name Plausible emits (currently pageview, engagement, product_download);
-- a future new event name requires no changes here, it just appears as a
-- new value in event_name.
SELECT
  @submission_date AS `date`,
  pathname,
  country_code,
  acquisition_channel,
  utm_campaign,
  name AS event_name,
  COUNT(*) AS event_count,
  -- Only meaningful when event_name = 'engagement'; Plausible delivers
  -- engagement_time = 0 (not NULL) on other event types, so this is
  -- harmlessly 0 for non-engagement rows. Capped per-event at 30 minutes
  -- before summing since Plausible does not bound this value itself (an
  -- observed maximum exceeded 13 days for a single event). Exposed as a sum
  -- rather than a pre-computed average so averaging across further grouping
  -- can be done correctly by summing both this and event_count first.
  SUM(LEAST(engagement_time, 1800000)) AS total_engagement_time_ms
FROM
  `moz-fx-data-shared-prod.plausible_external.events_v1`
WHERE
  DATE(`timestamp`) = @submission_date
GROUP BY
  `date`,
  pathname,
  country_code,
  acquisition_channel,
  utm_campaign,
  event_name
