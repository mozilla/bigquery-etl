CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_acquisition_funnel`
AS
SELECT
  first_seen_date,
  country_code,
  channel,
  os,
  os_version,
  distribution_id,
  attribution_ua,
  -- TODO: There's a prettier way to decode these but for now...
  CASE
    WHEN attribution_source = "%2528not%2Bset%2529"
      THEN "(not set)"
    ELSE attribution_source
  END AS attribution_source,
  CASE
    WHEN attribution_medium = "%2528not%2Bset%2529"
      THEN "(not set)"
    ELSE attribution_medium
  END AS attribution_medium,
  CASE
    WHEN attribution_campaign = "%2528not%2Bset%2529"
      THEN "(not set)"
    ELSE attribution_campaign
  END AS attribution_campaign,
  CASE
    WHEN attribution_content = "%2528not%2Bset%2529"
      THEN "(not set)"
    ELSE attribution_content
  END AS attribution_content,
  CASE
    WHEN attribution_experiment = "%2528not%2Bset%2529"
      THEN "(not set)"
    ELSE attribution_experiment
  END AS attribution_experiment,
  CASE
    WHEN attribution_dlsource = "%2528not%2Bset%2529"
      THEN "(not set)"
    ELSE attribution_dlsource
  END AS attribution_dlsource,
  startup_profile_selection_reason,
  COUNT(client_id) AS cohort,
  COUNTIF(activated) AS activated,
  -- TODO: naming here should probably change to repeat first month user
  COUNTIF(returned_second_day) AS returned_second_day,
  -- TODO: naming here should probably change to qualified repeat first month user
  COUNTIF(qualified_second_day) AS qualified_second_day,
  COUNTIF(retained_week4) AS retained_week4,
  COUNTIF(qualified_week4) AS qualified_week4
FROM
  -- TODO: Let's discuss the name of this table
  `mozdata.telemetry.clients_first_seen_28_days_later`
WHERE
  first_seen_date >= "2022-01-01"
GROUP BY
  first_seen_date,
  country_code,
  channel,
  os,
  os_version,
  distribution_id,
  attribution_source,
  attribution_ua,
  attribution_medium,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_dlsource,
  startup_profile_selection_reason
