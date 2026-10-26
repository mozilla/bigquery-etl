-- One row per Glean client per day. Every urlbar result type the client saw or picked is
-- nested in `results`, one element per (window_mode, sap, interaction, result_type).
-- No KPI logic, no version or channel filter: downstream tables decide what counts.
-- Click logic follows firefox_desktop_derived.urlbar_events_daily_v2, but matches on the
-- raw result_type rather than the product type, so raw types sharing a product name
-- aren't double-credited.
WITH events AS (
  SELECT
    glean_client_id AS client_id,
    legacy_telemetry_client_id,
    sample_id,
    event_id,
    event_name,
    event_action,
    event_timestamp,
    is_terminal,
    selected_result,
    product_selected_result,
    window_mode,
    sap,
    interaction,
    pref_fx_suggestions,
    pref_sponsored_suggestions,
    pref_ohttp_available,
    pref_ohttp_enabled,
    pref_data_collection,
    results
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.urlbar_events_v2`
  WHERE
    submission_date = @submission_date
    AND event_name IN ('abandonment', 'engagement', 'bounce', 'disable')
    AND glean_client_id IS NOT NULL
),
-- One row per event per raw result type shown in it.
session_types AS (
  SELECT
    e.client_id,
    e.event_id,
    r.result_type,
    ANY_VALUE(r.product_result_type) AS product_result_type,
    ANY_VALUE(r.result_group) AS result_group,
    LOGICAL_OR(e.is_terminal) AS shown_in_terminal,
    LOGICAL_OR(
      e.is_terminal
      AND e.event_action = 'engaged'
      AND e.selected_result = r.result_type
    ) AS clicked,
    LOGICAL_OR(e.is_terminal AND e.event_name = 'abandonment') AS abandoned,
    COUNTIF(e.event_action = 'annoyance' AND e.selected_result = r.result_type) AS annoyances
  FROM
    events AS e
  CROSS JOIN
    UNNEST(e.results) AS r
  GROUP BY
    e.client_id,
    e.event_id,
    r.result_type
  UNION ALL
  -- Picks of something that wasn't in the results list: paste/drop-and-go (input_field) and
  -- quick actions (action_*). Kept as their own elements so clicks sum to sessions_engaged.
  SELECT
    client_id,
    event_id,
    selected_result AS result_type,
    product_selected_result AS product_result_type,
    CAST(NULL AS STRING) AS result_group,
    FALSE AS shown_in_terminal,
    TRUE AS clicked,
    FALSE AS abandoned,
    0 AS annoyances
  FROM
    events
  WHERE
    is_terminal
    AND event_action = 'engaged'
    AND NOT EXISTS (SELECT 1 FROM UNNEST(results) AS r WHERE r.result_type = selected_result)
),
-- event_id is unique per event, not per session. A completed session has exactly one
-- terminal event, so counting terminal events counts sessions; non-terminal events
-- (annoyances, bounce, disable) only contribute shown-less elements and annoyances.
sessions AS (
  SELECT
    client_id,
    event_id,
    window_mode,
    sap,
    interaction,
    is_terminal AS completed,
    is_terminal
    AND event_action = 'engaged' AS engaged,
    is_terminal
    AND event_name = 'abandonment' AS abandoned
  FROM
    events
),
elements AS (
  SELECT
    s.client_id,
    s.window_mode,
    s.sap,
    s.interaction,
    t.result_type,
    ANY_VALUE(t.product_result_type) AS product_result_type,
    ANY_VALUE(t.result_group) AS result_group,
    COUNTIF(t.clicked) AS clicks,
    COUNTIF(t.shown_in_terminal) AS sessions_shown,
    COUNTIF(t.shown_in_terminal AND t.abandoned) AS sessions_shown_abandoned,
    SUM(t.annoyances) AS annoyances
  FROM
    session_types AS t
  JOIN
    sessions AS s
    USING (client_id, event_id)
  GROUP BY
    s.client_id,
    s.window_mode,
    s.sap,
    s.interaction,
    t.result_type
),
client_totals AS (
  SELECT
    client_id,
    COUNTIF(completed) AS sessions_total,
    COUNTIF(engaged) AS sessions_engaged,
    COUNTIF(abandoned) AS sessions_abandoned
  FROM
    sessions
  GROUP BY
    client_id
),
-- Identifiers and the latest pref values that day.
client_attributes AS (
  SELECT
    client_id,
    ANY_VALUE(sample_id) AS sample_id,
    ARRAY_AGG(legacy_telemetry_client_id IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS legacy_telemetry_client_id,
    ARRAY_AGG(
      STRUCT(
        pref_fx_suggestions AS pref_suggest_enabled,
        pref_sponsored_suggestions AS pref_sponsored_enabled,
        pref_ohttp_available AS pref_online_suggest_available,
        pref_ohttp_enabled AS pref_online_suggest_enabled,
        pref_data_collection
      )
      ORDER BY
        event_timestamp DESC
      LIMIT
        1
    )[OFFSET(0)] AS prefs
  FROM
    events
  GROUP BY
    client_id
),
client_results AS (
  SELECT
    client_id,
    ARRAY_AGG(
      STRUCT(
        window_mode,
        sap,
        interaction,
        result_type,
        product_result_type,
        result_group,
        clicks,
        sessions_shown,
        sessions_shown_abandoned,
        annoyances
      )
      ORDER BY
        window_mode,
        sap,
        interaction,
        result_type
    ) AS results
  FROM
    elements
  GROUP BY
    client_id
)
SELECT
  @submission_date AS submission_date,
  c.client_id,
  a.legacy_telemetry_client_id,
  a.sample_id,
  a.prefs.pref_suggest_enabled,
  a.prefs.pref_sponsored_enabled,
  a.prefs.pref_online_suggest_available,
  a.prefs.pref_online_suggest_enabled,
  a.prefs.pref_data_collection,
  -- No shared bot list exists yet.
  FALSE AS is_bot,
  c.sessions_total,
  c.sessions_engaged,
  c.sessions_abandoned,
  r.results
FROM
  client_totals AS c
JOIN
  client_attributes AS a
  USING (client_id)
LEFT JOIN
  client_results AS r
  USING (client_id)
