-- Session-level KB search success signals for SUMO, derived from GA4 events.
--
-- Search success = a KB-article view that happens AFTER a `search` event within
-- the same GA4 session (user_pseudo_id x ga_session_id). The `_1min` / `_2min`
-- flags additionally require the KB view to land within 60 / 120 seconds of the
-- closest preceding search event.
--
-- Grain: search session (user_pseudo_id x ga_session_id) x day.
-- Incremental: one submission_date partition per run.
WITH search_events AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    (
      SELECT
        ep.value.int_value
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'ga_session_id'
    ) AS ga_session_id,
    (
      SELECT
        ep.value.string_value
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'locale'
    ) AS search_locale
  FROM
    `mozdata.sumo_ga.ga4_events`
  WHERE
    submission_date = @submission_date
    AND event_name = 'search'
),
-- One row per search session, with the locale of its first search event.
search_sessions AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    ARRAY_AGG(search_locale ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS search_locale
  FROM
    search_events
  WHERE
    ga_session_id IS NOT NULL
  GROUP BY
    user_pseudo_id,
    ga_session_id
),
-- KB-article engagement events, carrying their session id.
kb_article_events AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    (
      SELECT
        ep.value.int_value
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'ga_session_id'
    ) AS ga_session_id
  FROM
    `mozdata.sumo_ga.ga4_events`
  WHERE
    submission_date = @submission_date
    AND event_name = 'user_engagement'
    AND EXISTS (
      SELECT
        1
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'content_group'
        AND ep.value.string_value = 'kb-article'
    )
),
-- For each session, seconds from the closest preceding search to a KB view.
kb_events_after_search AS (
  SELECT
    kb.user_pseudo_id,
    kb.ga_session_id,
    MIN(
      TIMESTAMP_DIFF(
        TIMESTAMP_MICROS(kb.event_timestamp),
        TIMESTAMP_MICROS(se.event_timestamp),
        SECOND
      )
    ) AS min_diff_in_seconds
  FROM
    kb_article_events kb
  INNER JOIN
    search_events se
    ON kb.ga_session_id = se.ga_session_id
    AND kb.user_pseudo_id = se.user_pseudo_id
    AND kb.event_timestamp > se.event_timestamp
  GROUP BY
    kb.user_pseudo_id,
    kb.ga_session_id
)
SELECT
  @submission_date AS session_date,
  s.search_locale,
  s.user_pseudo_id,
  s.ga_session_id,
  IF(kb.ga_session_id IS NOT NULL, 1, 0) AS is_successful_search,
  IF(kb.min_diff_in_seconds <= 60, 1, 0) AS is_successful_search_1min,
  IF(kb.min_diff_in_seconds <= 120, 1, 0) AS is_successful_search_2min
FROM
  search_sessions s
LEFT JOIN
  kb_events_after_search kb
  ON s.user_pseudo_id = kb.user_pseudo_id
  AND s.ga_session_id = kb.ga_session_id
