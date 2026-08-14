-- Query for firefox_desktop_derived.safe_browsing_interstitials_v1
--
-- The Safe Browsing interstitial funnel: how often Firefox renders an
-- about:blocked warning page, and what users then do with it.
--
-- Two metrics, both in the metrics ping, joined on threat type and scope.
--
-- DISPLAYS come from page.load_error, a dual labeled counter recorded in
-- nsDocShell::DisplayLoadError
-- It is a general page-load-error counter; Safe Browsing is four of its ~45
-- categories, filtered below.
--
-- ACTIONS come from urlclassifier.ui_events, accumulated in
-- browser/actors/BlockedSiteParent.sys.mjs on exactly two button presses:
-- goBackButton -> *_GET_ME_OUT_OF_HERE, ignore_warning_link ->
-- *_IGNORE_WARNING. Event codes are defined in
-- toolkit/components/url-classifier/IUrlClassifierUITelemetry.idl (1-32). The
-- *_PAGE_TOP and *_WHY_BLOCKED codes exist there but nothing writes them.
--
-- KNOWN BIAS, read before using this table. Displays count renders, not unique
-- encounters. nsChannelClassifier::MarkEntryClassified returns early for
-- blocking error codes, so a blocked URL is never cache-tagged, and so every
-- reload or session restore re-classifies and re-renders.
-- Observed displays per client run 1.7-2.1 where a single warning would be 1.0.
-- The actions are per click. So any action/displays ratio is a LOWER BOUND on
-- the real per-encounter rate, and the inflation cannot be divided out --
-- neither metric carries a navigation or session key.
-- Do not present these ratios as point estimates.
WITH base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel,
    normalized_os,
    IFNULL(normalized_country_code, '??') AS country,
    client_info.client_id AS client_id,
    metrics.dual_labeled_counter.page_load_error AS page_load_error,
    metrics.custom_distribution.urlclassifier_ui_events.values AS ui_events
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
displays AS (
  SELECT
    submission_date,
    normalized_channel,
    normalized_os,
    country,
    CASE
      scope_label.key
      WHEN 'top'
        THEN 'Top-level page'
      WHEN 'frame'
        THEN 'Iframe'
    END AS scope,
    CASE
      threat_label.key
      WHEN 'PHISHING_URI'
        THEN 'Phishing'
      WHEN 'MALWARE_URI'
        THEN 'Malware'
      WHEN 'UNWANTED_URI'
        THEN 'Unwanted'
      WHEN 'HARMFUL_URI'
        THEN 'Harmful'
    END AS threat_type,
    SUM(threat_label.value) AS displays,
    COUNT(DISTINCT client_id) AS display_clients
  FROM
    base
  CROSS JOIN
    UNNEST(page_load_error) AS scope_label
  CROSS JOIN
    UNNEST(scope_label.value) AS threat_label
  WHERE
    threat_label.key IN ('PHISHING_URI', 'MALWARE_URI', 'UNWANTED_URI', 'HARMFUL_URI')
    AND threat_label.value > 0
  GROUP BY
    submission_date,
    normalized_channel,
    normalized_os,
    country,
    scope,
    threat_type
),
actions AS (
  SELECT
    submission_date,
    normalized_channel,
    normalized_os,
    country,
    CASE
      WHEN event_code
        BETWEEN 1
        AND 8
        THEN 'Malware'
      WHEN event_code
        BETWEEN 9
        AND 16
        THEN 'Phishing'
      WHEN event_code
        BETWEEN 17
        AND 24
        THEN 'Unwanted'
      WHEN event_code
        BETWEEN 25
        AND 32
        THEN 'Harmful'
    END AS threat_type,
    CASE
      WHEN event_code IN (1, 2, 3, 4, 9, 10, 11, 12, 17, 18, 19, 20, 25, 26, 27, 28)
        THEN 'Top-level page'
      WHEN event_code IN (5, 6, 7, 8, 13, 14, 15, 16, 21, 22, 23, 24, 29, 30, 31, 32)
        THEN 'Iframe'
    END AS scope,
    -- Event codes transcribed from IUrlClassifierUITelemetry.idl.
    -- *_GET_ME_OUT_OF_HERE and *_IGNORE_WARNING respectively.
    SUM(IF(event_code IN (3, 7, 11, 15, 19, 23, 27, 31), event_count, 0)) AS left_site,
    SUM(IF(event_code IN (4, 8, 12, 16, 20, 24, 28, 32), event_count, 0)) AS proceeded_anyway,
    COUNT(
      DISTINCT IF(event_code IN (3, 7, 11, 15, 19, 23, 27, 31), client_id, NULL)
    ) AS left_site_clients,
    COUNT(
      DISTINCT IF(event_code IN (4, 8, 12, 16, 20, 24, 28, 32), client_id, NULL)
    ) AS proceeded_anyway_clients
  FROM
    (
      SELECT
        submission_date,
        normalized_channel,
        normalized_os,
        country,
        client_id,
        SAFE_CAST(ui_event.key AS INT64) AS event_code,
        ui_event.value AS event_count
      FROM
        base
      CROSS JOIN
        UNNEST(ui_events) AS ui_event
      WHERE
        -- Zero-count entries are bucket padding, not observations.
        ui_event.value > 0
    )
  GROUP BY
    submission_date,
    normalized_channel,
    normalized_os,
    country,
    threat_type,
    scope
)
-- FULL OUTER JOIN so a row survives when only one side has data: a display with
-- no button press (the common case), or an action whose display landed in a
-- different ping.
SELECT
  submission_date,
  normalized_channel,
  normalized_os,
  country,
  threat_type,
  scope,
  IFNULL(displays.displays, 0) AS displays,
  IFNULL(displays.display_clients, 0) AS display_clients,
  IFNULL(actions.left_site, 0) AS left_site,
  IFNULL(actions.proceeded_anyway, 0) AS proceeded_anyway,
  IFNULL(actions.left_site_clients, 0) AS left_site_clients,
  IFNULL(actions.proceeded_anyway_clients, 0) AS proceeded_anyway_clients
FROM
  displays
FULL OUTER JOIN
  actions
  USING (submission_date, normalized_channel, normalized_os, country, threat_type, scope)
