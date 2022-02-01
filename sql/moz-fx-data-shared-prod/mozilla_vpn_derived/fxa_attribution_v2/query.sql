WITH new_flows AS (
  SELECT
    flow_id,
    MIN(`timestamp`) AS flow_started,
    ARRAY_AGG(DISTINCT user_id IGNORE NULLS) AS fxa_uids,
    -- record service across whole flow because it may be missing from some events
    ARRAY_AGG(DISTINCT NULLIF(service, "undefined_oauth") IGNORE NULLS) AS services,
    ARRAY_AGG(
      IF(
        entrypoint_experiment IS NOT NULL
        OR entrypoint_variation IS NOT NULL
        OR utm_campaign IS NOT NULL
        OR utm_content IS NOT NULL
        OR utm_medium IS NOT NULL
        OR utm_source IS NOT NULL
        OR utm_term IS NOT NULL,
        STRUCT(
          `timestamp`,
          entrypoint_experiment,
          entrypoint_variation,
          utm_campaign,
          utm_content,
          utm_medium,
          utm_source,
          utm_term
        ),
        NULL
      ) IGNORE NULLS
    ) AS attributions,
  FROM
    mozdata.firefox_accounts.fxa_content_auth_stdout_events
  WHERE
    IF(@date IS NULL, partition_date < CURRENT_DATE, partition_date = @date)
    AND flow_id IS NOT NULL
  GROUP BY
    flow_id
)
SELECT
  flow_id,
  LEAST(new_flows.flow_started, old_flows.flow_started) AS flow_started,
  ARRAY(
    SELECT DISTINCT
      *
    FROM
      UNNEST(ARRAY_CONCAT(IFNULL(old_flows.fxa_uids, []), IFNULL(new_flows.fxa_uids, [])))
  ) AS fxa_uids,
  ARRAY(
    SELECT DISTINCT
      *
    FROM
      UNNEST(ARRAY_CONCAT(IFNULL(old_flows.fxa_uids, []), IFNULL(new_flows.fxa_uids, [])))
  ) AS services,
  -- preserve each attribution with the earliest timestamp and updated count
  ARRAY(
    SELECT
      MIN(`timestamp`) AS min_timestamp,
      MAX(`timestamp`) AS max_timestamp,
      entrypoint_experiment,
      entrypoint_variation,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
    FROM
      UNNEST(ARRAY_CONCAT(IFNULL(old_flows.attributions, []), IFNULL(new_flows.attributions, [])))
    GROUP BY
      entrypoint_experiment,
      entrypoint_variation,
      utm_campaign,
      utm_content,
      utm_medium,
      utm_source,
      utm_term
  ) AS attributions,
FROM
  fxa_attribution_v2 AS old_flows
FULL JOIN
  new_flows
USING
  (flow_id)
HAVING
  IF(
    @date IS NOT NULL,
    -- remove flows that ended and lack either fxa uids or attributions
    (
      -- flow ids expire in less than a day, so it's safe to assume flows that started on the previous
      -- date have ended and will not have any more events. Use `= @date - 1` instead of `< @date` to
      -- safely handle out of order backfilling.
      NOT (DATE(flow_started) = @date - 1)
      -- TODO maybe also remove flows that have no service, or don't have service = guardian-vpn
      OR (ARRAY_LENGTH(fxa_uids) > 0 AND ARRAY_LENGTH(attributions) > 0)
    ),
    TRUE
  )
