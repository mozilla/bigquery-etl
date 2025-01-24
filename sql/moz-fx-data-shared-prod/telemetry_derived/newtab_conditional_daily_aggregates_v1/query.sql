SELECT
  submission_date,
  country_code,
  pocket_enabled,
  pocket_sponsored_stories_enabled,
  topsites_enabled,
  COUNT(DISTINCT client_id) AS total_client_count,
  COUNT(DISTINCT newtab_visit_id) AS total_visit_count,
  -- conditional
  COUNT(DISTINCT IF(channel = 'release', client_id, NULL)) AS client_count,
  COUNT(DISTINCT IF(channel = 'release', newtab_visit_id, NULL)) AS visit_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:home'
      AND newtab_homepage_category = 'enabled',
      client_id,
      NULL
    )
  ) AS home_default_client_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:home'
      AND newtab_homepage_category = 'enabled',
      newtab_visit_id,
      NULL
    )
  ) AS home_default_visit_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:newtab'
      AND newtab_newtab_category = 'enabled',
      client_id,
      NULL
    )
  ) AS newtab_default_client_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:newtab'
      AND newtab_newtab_category = 'enabled',
      newtab_visit_id,
      NULL
    )
  ) AS newtab_default_visit_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:home'
      AND newtab_homepage_category != 'enabled',
      client_id,
      NULL
    )
  ) AS home_nondefault_client_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:home'
      AND newtab_homepage_category != 'enabled',
      newtab_visit_id,
      NULL
    )
  ) AS home_nondefault_visit_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:newtab'
      AND newtab_newtab_category != 'enabled',
      client_id,
      NULL
    )
  ) AS newtab_nondefault_client_count,
  COUNT(
    DISTINCT IF(
      channel = 'release'
      AND newtab_open_source = 'about:newtab'
      AND newtab_newtab_category != 'enabled',
      newtab_visit_id,
      NULL
    )
  ) AS newtab_nondefault_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND newtab_open_source = 'about:welcome', client_id, NULL)
  ) AS welcome_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND newtab_open_source = 'about:welcome', newtab_visit_id, NULL)
  ) AS welcome_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND searches > 0, client_id, NULL)
  ) AS searched_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND searches > 0, newtab_visit_id, NULL)
  ) AS searched_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_topsite_impressions > 0, client_id, NULL)
  ) AS sponsored_topsite_impressed_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_topsite_impressions > 0, newtab_visit_id, NULL)
  ) AS sponsored_topsite_impressed_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_topsite_clicks > 0, client_id, NULL)
  ) AS sponsored_topsite_clicked_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_topsite_clicks > 0, newtab_visit_id, NULL)
  ) AS sponsored_topsite_clicked_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND topsite_impressions > 0, client_id, NULL)
  ) AS topsite_impressed_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND topsite_impressions > 0, newtab_visit_id, NULL)
  ) AS topsite_impressed_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND topsite_clicks > 0, client_id, NULL)
  ) AS topsite_clicked_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND topsite_clicks > 0, newtab_visit_id, NULL)
  ) AS topsite_clicked_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_pocket_impressions > 0, client_id, NULL)
  ) AS sponsored_pocket_impressed_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_pocket_impressions > 0, newtab_visit_id, NULL)
  ) AS sponsored_pocket_impressed_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_pocket_clicks > 0, client_id, NULL)
  ) AS sponsored_pocket_clicked_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND sponsored_pocket_clicks > 0, newtab_visit_id, NULL)
  ) AS sponsored_pocket_clicked_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND organic_pocket_impressions > 0, client_id, NULL)
  ) AS organic_pocket_impressed_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND organic_pocket_impressions > 0, newtab_visit_id, NULL)
  ) AS organic_pocket_impressed_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND organic_pocket_clicks > 0, client_id, NULL)
  ) AS organic_pocket_clicked_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND organic_pocket_clicks > 0, newtab_visit_id, NULL)
  ) AS organic_pocket_clicked_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND pocket_impressions > 0, client_id, NULL)
  ) AS pocket_impressed_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND pocket_impressions > 0, newtab_visit_id, NULL)
  ) AS pocket_impressed_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND pocket_clicks > 0, client_id, NULL)
  ) AS pocket_clicked_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND pocket_clicks > 0, newtab_visit_id, NULL)
  ) AS pocket_clicked_visit_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND pocket_is_signed_in, client_id, NULL)
  ) AS pocket_signed_in_client_count,
  COUNT(
    DISTINCT IF(channel = 'release' AND pocket_is_signed_in, newtab_visit_id, NULL)
  ) AS pocket_signed_in_visit_count,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.newtab_interactions_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  country_code,
  pocket_enabled,
  pocket_sponsored_stories_enabled,
  topsites_enabled
