-- KB content quality metrics at the article x month grain, combining GA4
-- engagement signals with content freshness and helpfulness votes.
--
-- Grain: month x locale x slug. One row per article slug + page locale + month.
-- Freshness is measured from the en-US source article (last approved revision)
-- and applied to every page locale of the same slug; helpfulness and the GA4
-- engagement signals are measured per (locale, slug).
--
-- Full recompute over GA4 history (from 2024-07-01, the baseline start) each
-- run; no date partition.
WITH
-- Per-session bounce flag. session_engaged can appear on any event type, so it
-- is aggregated across every event in the session.
session_flags AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH) AS month,
    IF(
      MAX(
        (
          SELECT
            MAX(IF(ep.key = 'session_engaged', ep.value.int_value, 0))
          FROM
            UNNEST(event_params) ep
        )
      ) = 0,
      1,
      0
    ) AS is_bounce
  FROM
    `moz-fx-data-shared-prod.sumo_ga.ga4_events`
  WHERE
    -- Filter on the submission_date partition column to prune partitions; the
    -- monthly grain itself is still derived from event_date below.
    submission_date >= '2024-07-01'
  GROUP BY
    user_pseudo_id,
    session_id,
    MONTH
),
-- page_view and user_engagement events with the params we need flattened out.
base_events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_timestamp,
    DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH) AS month,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'content_group'
    ) AS content_group,
    (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'default_slug'
    ) AS default_slug,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'locale') AS locale,
    (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'engagement_time_msec'
    ) AS engagement_time_msec
  FROM
    `moz-fx-data-shared-prod.sumo_ga.ga4_events`
  WHERE
    -- Prune on the submission_date partition column (month is derived from
    -- event_date below).
    submission_date >= '2024-07-01'
    AND event_name IN ('page_view', 'user_engagement')
),
page_views AS (
  SELECT
    user_pseudo_id,
    session_id,
    MONTH,
    event_timestamp,
    content_group,
    default_slug,
    locale
  FROM
    base_events
  WHERE
    event_name = 'page_view'
),
-- Landing / exit / bounce flags for KB-article page views. Landing and exit are
-- ranked across ALL page views in the session, then restricted to KB articles,
-- so a KB article only counts as a landing if it is the session's first page.
landing_and_exit AS (
  SELECT
    MONTH,
    locale,
    default_slug,
    is_bounce,
    landing,
    exit_page
  FROM
    (
      SELECT
        p.month,
        p.locale,
        p.default_slug,
        p.content_group,
        sf.is_bounce,
        IF(
          ROW_NUMBER() OVER (
            PARTITION BY
              p.user_pseudo_id,
              p.session_id
            ORDER BY
              p.event_timestamp ASC
          ) = 1,
          1,
          0
        ) AS landing,
        IF(
          ROW_NUMBER() OVER (
            PARTITION BY
              p.user_pseudo_id,
              p.session_id
            ORDER BY
              p.event_timestamp DESC
          ) = 1,
          1,
          0
        ) AS exit_page
      FROM
        page_views p
      JOIN
        session_flags sf
        USING (user_pseudo_id, session_id, MONTH)
    )
  WHERE
    content_group = 'kb-article'
),
-- Total engagement time and session count per (month, locale, slug).
engagement_by_article AS (
  SELECT
    MONTH,
    locale,
    default_slug,
    SUM(engagement_time_msec) AS total_engagement_time_msec,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(session_id AS STRING))) AS session_count
  FROM
    base_events
  WHERE
    event_name = 'user_engagement'
    AND content_group = 'kb-article'
  GROUP BY
    MONTH,
    locale,
    default_slug
),
-- Distinct (month, locale, slug) combinations observed in GA4 page views.
article_month_combinations AS (
  SELECT
    MONTH,
    locale,
    default_slug AS slug,
    COUNT(*) AS page_views
  FROM
    page_views
  GROUP BY
    MONTH,
    locale,
    default_slug
),
-- Cumulative helpful-vote share per (month, locale, slug), using all votes cast
-- from 2023-01-01 through the end of the month.
articles_helpfulness_monthly AS (
  SELECT
    amc.month,
    amc.locale,
    amc.slug,
    SAFE_DIVIDE(SUM(IF(v.helpful, 1, 0)), COUNT(v.slug)) AS helpfulness
  FROM
    article_month_combinations amc
  LEFT JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.metrics_kb_votes_details` v
    ON v.locale = amc.locale
    AND v.slug = amc.slug
    AND DATE(v.vote_created_date) >= '2023-01-01'
    AND DATE(v.vote_created_date) <= LAST_DAY(amc.month)
  GROUP BY
    amc.month,
    amc.locale,
    amc.slug
),
-- Freshness per (month, locale, slug): months since the en-US article's last
-- approved revision before the month. Only articles with at least one approved
-- revision before the start of the month are included.
articles_freshness_monthly AS (
  SELECT
    amc.month,
    amc.slug,
    amc.locale,
    amc.page_views,
    MAX(d.title) AS title,
    DATE_DIFF(LAST_DAY(amc.month), DATE(MAX(r.created)), MONTH) AS freshness_months,
    -- Canonical CX product name from the article's slash-delimited `products`
    -- path. The 'GA4' rule set matches path-style inputs; unmapped paths return
    -- 'Other'.
    mozfun.customer_experience.normalize_product(MAX(d.products), 'GA4') AS product
  FROM
    article_month_combinations amc
  INNER JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus` d
    ON d.locale = 'en-US' -- freshness based on the English article version
    AND d.slug = amc.slug
  INNER JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` r
    ON d.id = r.document_id
    AND r.is_approved
    AND DATE(r.created) < amc.month
  GROUP BY
    amc.month,
    amc.slug,
    amc.locale,
    amc.page_views
)
SELECT
  le.month,
  le.locale,
  le.default_slug AS slug,
  MAX(a.title) AS title,
  MAX(a.product) AS product,
  MIN(a.freshness_months) AS freshness_months,
  MAX(h.helpfulness) AS helpfulness,
  SAFE_DIVIDE(MAX(e.total_engagement_time_msec), MAX(e.session_count)) AS avg_engagement_time,
  SAFE_DIVIDE(SUM(le.landing), COUNT(*)) AS landing_rate,
  SAFE_DIVIDE(SUM(le.exit_page), COUNT(*)) AS exit_rate,
  SAFE_DIVIDE(SUM(le.is_bounce), COUNT(*)) AS bounce_pageview_share,
  COUNT(*) AS page_views
FROM
  landing_and_exit le
INNER JOIN
  articles_freshness_monthly a
  ON le.month = a.month
  AND le.locale = a.locale
  AND le.default_slug = a.slug
LEFT JOIN
  engagement_by_article e
  ON le.month = e.month
  AND le.locale = e.locale
  AND le.default_slug = e.default_slug
LEFT JOIN
  articles_helpfulness_monthly h
  ON le.month = h.month
  AND le.locale = h.locale
  AND le.default_slug = h.slug
GROUP BY
  le.month,
  le.locale,
  le.default_slug
