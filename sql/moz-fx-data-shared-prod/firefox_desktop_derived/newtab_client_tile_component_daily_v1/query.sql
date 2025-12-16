SELECT
  submission_date,
  client_id,
  profile_group_id,
  app_version,
  os,
  channel,
  country,
  is_default_ui,
  organic_topsites_enabled,
  sponsored_topsites_enabled,
  t.position,
  t.is_sponsored,
  ANY_VALUE(experiments) AS experiments,
  SUM(t.impression_count) AS impression_count,
  SUM(t.click_count) AS click_count,
  SUM(t.dismissal_count) AS dismissal_count,
  SUM(t.pin_count) AS pin_count,
  SUM(t.unpin_count) AS unpin_count,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2` AS v,
  UNNEST(topsite_tile_components) AS t
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  client_id,
  profile_group_id,
  app_version,
  os,
  channel,
  country,
  is_default_ui,
  organic_topsites_enabled,
  sponsored_topsites_enabled,
  t.position,
  t.is_sponsored
