CREATE TEMP FUNCTION contains_phase_2_experiment(map ANY TYPE) AS (
  (
    SELECT
      LOGICAL_OR(
        key_value.key IN UNNEST(
          [
            'bug-1655185-pref-topsites-redirect-launch-us-v5-release-80-80',
            'bug-1678672-pref-topsites-redirect-launch-us-v5-v2-release-80-82',
            'bug-1657450-pref-topsites-url-redirect-germany-launch-release-80-80',
            'bug-1678671-pref-topsites-url-redirect-germany-launch-v2-release-80-82',
            'bug-1657447-pref-topsites-url-redirect-uk-launch-v5-release-80-80',
            'bug-1678673-pref-topsites-url-redirect-uk-launch-v5-v2-release-80-82'
          ]
        )
      )
    FROM
      UNNEST(map) AS key_value
  )
);

CREATE TEMP FUNCTION contains_phase_3_experiment(map ANY TYPE) AS (
  (
    SELECT
      LOGICAL_OR(
        key_value.key IN UNNEST(
          [
            'bug-1665061-pref-topsites-launch-phase-3-us-release-83-85',
            'bug-1678683-pref-topsites-launch-phase-3-us-v2-release-83-85',
            'bug-1676316-pref-topsites-launch-phase-3-de-release-83-85',
            'bug-1676315-pref-topsites-launch-phase-3-gb-release-83-85',
            'bug-1682646-pref-topsites-launch-phase3-group2-fr-release-84-86',
            'bug-1682644-pref-topsites-launch-phase3-group2-ca-release-84-86',
            'bug-1682645-pref-topsites-launch-phase3-group2-au-release-84-86'
          ]
        )
      )
    FROM
      UNNEST(map) AS key_value
  )
);

CREATE TEMP FUNCTION contains_rollout_experiment(map ANY TYPE) AS (
  (
    SELECT
      LOGICAL_OR(
        key_value.key IN UNNEST(['bug-1693420-rollout-sponsored-top-sites-rollout-release-84-100'])
      )
    FROM
      UNNEST(map) AS key_value
  )
);

WITH topsites_temp AS (
  SELECT
    submission_date,
    -- might be a problem if the experiment targeting and the geo we detect are the different
    country,
    -- for phase 2, the events were structured differently. info contained in map
    CASE
    WHEN
      contains_phase_2_experiment(experiments)
    THEN
      udf.get_key(event_map_values, 'source')
    ELSE
      event_object
    END
    AS placement,
    CASE
    WHEN
      contains_phase_2_experiment(experiments)
    THEN
      udf.get_key(event_map_values, 'partner')
    ELSE
      event_string_value
    END
    AS partner,
    CASE
    WHEN
      contains_phase_2_experiment(experiments)
    THEN
      event_object
    ELSE
      event_method
    END
    AS interaction,
    SPLIT(app_version, '.')[OFFSET(0)] AS version,
    normalized_channel,
    CASE
    WHEN
      contains_phase_2_experiment(experiments)
    THEN
      'phase2'
    WHEN
      contains_phase_3_experiment(experiments)
    THEN
      'phase3'
    WHEN
      contains_rollout_experiment(experiments)
    THEN
      'rollout'
    ELSE
      NULL
    END
    AS phase
  FROM
    telemetry.events
  WHERE
    submission_date = @submission_date
    AND event_category = 'partner_link'
    AND country IN ('US', 'DE', 'GB', 'FR', 'CA', 'AU')
),
searchmode_temp AS (
  SELECT
    submission_date,
    -- might be a problem if the experiment targeting and the geo we detect are the different.
    country,
    source AS placement,
    -- looks as though search engine replacement is only for amazon
    'amazon' AS partner,
    sap,
    SPLIT(app_version, '.')[OFFSET(0)] AS version,
    channel AS normalized_channel,
    CASE
    WHEN
      contains_phase_2_experiment(experiments)
    THEN
      'phase2'
    WHEN
      contains_phase_3_experiment(experiments)
    THEN
      'phase3'
    WHEN
      contains_rollout_experiment(experiments)
    THEN
      'rollout'
    ELSE
      NULL
    END
    AS phase
  FROM
    search.search_clients_daily
  WHERE
    submission_date = @submission_date
    -- looks as though search engine replacement is only for amazon
    AND engine IN ('amazondotcom-adm', 'amazon-de-adm', 'amazon-en-GB-adm')
    AND country IN ('US', 'DE', 'GB', 'FR', 'CA', 'AU')
)
SELECT
  submission_date,
  country,
  placement,
  partner,
  version,
  normalized_channel,
  SUM(sap) AS n_engagements,
  'redirect' AS interaction,
  phase
FROM
  searchmode_temp
GROUP BY
  submission_date,
  phase,
  country,
  placement,
  partner,
  version,
  normalized_channel
UNION ALL
  (
    SELECT
      submission_date,
      country,
      placement,
      partner,
      version,
      normalized_channel,
      COUNTIF(interaction = 'click') AS n_engagements,
      'topsite' AS interaction,
      phase
    FROM
      topsites_temp
    GROUP BY
      submission_date,
      phase,
      country,
      placement,
      partner,
      version,
      normalized_channel
  )
ORDER BY
  submission_date DESC,
  country ASC,
  n_engagements DESC
