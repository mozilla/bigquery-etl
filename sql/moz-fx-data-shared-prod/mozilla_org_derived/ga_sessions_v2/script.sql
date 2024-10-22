MERGE INTO
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v2` T
  USING (
    --get all the unique "GA Client ID", "GA Session ID" combinations with events between 3 days prior to the submission date and the submission date
    WITH all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days AS (
      SELECT DISTINCT
        user_pseudo_id AS ga_client_id,
        CAST(e.value.int_value AS string) AS ga_session_id
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
      JOIN
        UNNEST(event_params) e
      WHERE
        e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
        AND _TABLE_SUFFIX
        BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@submission_date, INTERVAL 3 DAY))
        AND FORMAT_DATE('%Y%m%d', @submission_date)
    ),
    --get the unique client IDs from the prior table
    distinct_ga_client_ids AS (
      SELECT DISTINCT
        ga_client_id
      FROM
        all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days
    ),
    device_properties_at_session_start_event AS (
      --get all session starts, from any date, associated with these client ID / session IDs from the last 3 days
      SELECT
        user_pseudo_id AS ga_client_id,
        CAST(e.value.int_value AS string) AS ga_session_id,
        (
          SELECT
            `value`
          FROM
            UNNEST(event_params)
          WHERE
            key = 'ga_session_number'
          LIMIT
            1
        ).int_value AS ga_session_number,
        geo.country AS country,
        geo.region AS region,
        geo.city AS city,
        collected_traffic_source.manual_campaign_id AS campaign_id,
        collected_traffic_source.manual_campaign_name AS campaign,
        collected_traffic_source.manual_source AS source,
        collected_traffic_source.manual_medium AS medium,
        collected_traffic_source.manual_term AS term,
        collected_traffic_source.manual_content AS content,
        device.category AS device_category,
        device.mobile_model_name AS mobile_device_model,
        device.mobile_marketing_name AS mobile_device_string,
        device.operating_system AS os,
        device.operating_system_version AS os_version,
        device.language AS `language`,
        device.web_info.browser AS browser,
        device.web_info.browser_version AS browser_version,
        PARSE_DATE('%Y%m%d', event_date) AS session_date
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        UNNEST(event_params) AS e
      JOIN
        all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days c
        ON a.user_pseudo_id = c.ga_client_id
        AND CAST(e.value.int_value AS string) = c.ga_session_id
      WHERE
        e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
        AND event_name = 'session_start'
      QUALIFY
        ROW_NUMBER() OVER (
          PARTITION BY
            user_pseudo_id,
            e.value.int_value
          ORDER BY
            event_timestamp ASC
        ) = 1
    ),
    --search all time for all the client IDs, and get all campaigns we see and the session ID we see them for
    all_campaigns_from_event_params_in_session_staging AS (
      SELECT
        a.user_pseudo_id AS ga_client_id,
        a.event_timestamp,
        (
          SELECT
            `value`
          FROM
            UNNEST(event_params)
          WHERE
            key = 'ga_session_id'
          LIMIT
            1
        ).int_value AS ga_session_id,
        (
          SELECT
            `value`
          FROM
            UNNEST(event_params)
          WHERE
            key = 'campaign'
          LIMIT
            1
        ).string_value AS campaign_from_event_params
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        distinct_ga_client_ids b
        ON a.user_pseudo_id = b.ga_client_id
    ),
    all_campaigns_from_event_params_in_session AS (
      SELECT
        ga_client_id,
        CAST(ga_session_id AS string) AS ga_session_id,
        campaign_from_event_params,
        event_timestamp
      FROM
        all_campaigns_from_event_params_in_session_staging
      WHERE
        ga_client_id IS NOT NULL
        AND ga_session_id IS NOT NULL
        AND campaign_from_event_params IS NOT NULL
    ),
    campaigns_by_session AS (
      SELECT
        ga_client_id,
        ga_session_id,
        ARRAY_AGG(DISTINCT campaign_from_event_params) AS distinct_campaigns_from_event_params,
        ARRAY_AGG(campaign_from_event_params ORDER BY event_timestamp ASC)[
          0
        ] AS first_campaign_from_event_params
      FROM
        all_campaigns_from_event_params_in_session
      GROUP BY
        ga_client_id,
        ga_session_id
    ),
    click_aggregate_stg AS (
      SELECT DISTINCT
        user_pseudo_id AS ga_client_id,
        event_timestamp,
        CAST(e.value.int_value AS string) AS ga_session_id,
        collected_traffic_source.gclid AS gclid
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        UNNEST(event_params) AS e
      JOIN
        all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days c
        ON a.user_pseudo_id = c.ga_client_id
        AND CAST(e.value.int_value AS string) = c.ga_session_id
      WHERE
        e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
        AND collected_traffic_source.gclid IS NOT NULL
    ),
    click_aggregate AS (
      SELECT
        ga_client_id,
        ga_session_id,
        ARRAY_AGG(DISTINCT gclid) AS gclid_array,
        ARRAY_AGG(gclid ORDER BY event_timestamp DESC)[
          0
        ] AS gclid --this is really just the last reported gclid
      FROM
        click_aggregate_stg
      GROUP BY
        ga_client_id,
        ga_session_id
    ),
    --get all the page views and min/max event timestamp and whether there was a product download for these session/clients of interest
    event_aggregates AS (
      SELECT
        user_pseudo_id AS ga_client_id,
        CAST(e.value.int_value AS string) AS ga_session_id,
        COUNTIF(event_name = 'page_view') AS pageviews,
        MIN(event_timestamp) AS min_event_timestamp,
        MAX(event_timestamp) AS max_event_timestamp,
        SUM(
          CASE
            WHEN (event_name = 'firefox_download' AND event_date >= '20240217')
              OR (
                event_name = 'product_download'
                AND event_date < '20240217'
                AND (
                  SELECT
                    `value`
                  FROM
                    UNNEST(event_params)
                  WHERE
                    key = 'product'
                  LIMIT
                    1
                ).string_value = 'firefox'
                AND (
                  SELECT
                    `value`
                  FROM
                    UNNEST(event_params)
                  WHERE
                    key = 'platform'
                  LIMIT
                    1
                ).string_value IN (
                  'win',
                  'win64',
                  'macos',
                  'linux64',
                  'win64-msi',
                  'linux',
                  'win-msi',
                  'win64-aarch64'
                ) --platform = desktop
              )
              THEN 1
            ELSE 0
          END
        ) AS firefox_desktop_downloads,
        CAST(
          MAX(
            CASE
              WHEN event_name IN (
                  'product_download',
                  'firefox_download',
                  'firefox_mobile_download',
                  'focus_download',
                  'klar_download'
                )
                THEN 1
              ELSE 0
            END
          ) AS boolean
        ) AS had_download_event
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        UNNEST(event_params) AS e
      JOIN
        all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days c
        ON a.user_pseudo_id = c.ga_client_id
        AND CAST(e.value.int_value AS string) = c.ga_session_id
      WHERE
        e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
      GROUP BY
        user_pseudo_id,
        CAST(e.value.int_value AS string)
    ),
    --get all the stub session IDs for the clients of interest
    stub_session_ids_staging AS (
      SELECT
        user_pseudo_id AS ga_client_id,
        event_timestamp,
        CAST(
          (
            SELECT
              `value`
            FROM
              UNNEST(event_params)
            WHERE
              key = 'ga_session_id'
            LIMIT
              1
          ).int_value AS string
        ) AS ga_session_id,
        CAST(e.value.int_value AS string) AS stub_session_id
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        UNNEST(event_params) AS e
      JOIN
        distinct_ga_client_ids c
        ON a.user_pseudo_id = c.ga_client_id
      WHERE
        event_name = 'stub_session_set'
        AND e.key = 'id'
        AND e.value.int_value IS NOT NULL
    ),
    --put the stub session IDs into an array
    all_stub_session_ids AS (
      SELECT
        ga_client_id,
        ga_session_id,
        ARRAY_AGG(stub_session_id) AS all_reported_stub_session_ids,
        ARRAY_AGG(stub_session_id ORDER BY event_timestamp DESC)[0] AS last_reported_stub_session_id
      FROM
        stub_session_ids_staging
      GROUP BY
        ga_client_id,
        ga_session_id
    ),
    --for each session, get the entrance page location
    landing_page_by_session_staging AS (
      SELECT
        user_pseudo_id AS ga_client_id,
        CAST(
          (
            SELECT
              `value`
            FROM
              UNNEST(event_params)
            WHERE
              key = 'ga_session_id'
            LIMIT
              1
          ).int_value AS string
        ) AS ga_session_id,
        SPLIT(
          (
            SELECT
              `value`
            FROM
              UNNEST(event_params)
            WHERE
              key = 'page_location'
            LIMIT
              1
          ).string_value,
          '?'
        )[OFFSET(0)] AS page_location,
        event_timestamp
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        UNNEST(event_params) AS e
      JOIN
        distinct_ga_client_ids c
        ON a.user_pseudo_id = c.ga_client_id
      WHERE
        e.key = 'entrances'
        AND e.value.int_value = 1
    ),
    --if there is ever for some reason more than 1 entrance in 1 session, just select the first one
    landing_page_by_session AS (
      SELECT
        ga_client_id,
        ga_session_id,
        page_location,
        event_timestamp
      FROM
        landing_page_by_session_staging
      QUALIFY
        ROW_NUMBER() OVER (
          PARTITION BY
            ga_client_id,
            ga_session_id
          ORDER BY
            event_timestamp ASC
        ) = 1
    ),
    --search for any download events (used to be called product download, but then was broken out into the 4 more detailed options)
    install_targets_staging AS (
      SELECT
        user_pseudo_id AS ga_client_id,
        CAST(e.value.int_value AS string) AS ga_session_id,
        event_timestamp,
        event_name AS install_event_name
      FROM
        `moz-fx-data-marketing-prod.analytics_313696158.events_2*` a
      JOIN
        UNNEST(event_params) AS e
      JOIN
        all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days c
        ON a.user_pseudo_id = c.ga_client_id
        AND CAST(e.value.int_value AS string) = c.ga_session_id
      WHERE
        e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
        AND event_name IN (
          'product_download',
          'firefox_download',
          'firefox_mobile_download',
          'focus_download',
          'klar_download'
        )
    ),
    --put these into an array for the session
    all_install_targets AS (
      SELECT
        ga_client_id,
        ga_session_id,
        ARRAY_AGG(install_event_name) AS all_reported_install_targets,
        ARRAY_AGG(install_event_name ORDER BY event_timestamp DESC)[
          SAFE_OFFSET(0)
        ] AS last_reported_install_target
      FROM
        install_targets_staging
      GROUP BY
        ga_client_id,
        ga_session_id
    )
    SELECT
      sess_strt.ga_client_id,
      sess_strt.ga_session_id,
      sess_strt.session_date,
      CASE
        WHEN sess_strt.ga_session_number = 1
          THEN TRUE
        ELSE FALSE
      END AS is_first_session,
      sess_strt.ga_session_number AS session_number,
      CAST(
        (evnt.max_event_timestamp - evnt.min_event_timestamp) / 1000000 AS int64
      ) AS time_on_site,
      evnt.pageviews,
      sess_strt.country,
      sess_strt.region,
      sess_strt.city,
      sess_strt.campaign_id,
      sess_strt.campaign,
      sess_strt.source,
      sess_strt.medium,
      sess_strt.term,
      sess_strt.content,
      clicks.gclid,
      clicks.gclid_array,
      sess_strt.device_category,
      sess_strt.mobile_device_model,
      sess_strt.mobile_device_string,
      sess_strt.os,
      sess_strt.os_version,
      sess_strt.language,
      sess_strt.browser,
      sess_strt.browser_version,
      evnt.had_download_event,
      evnt.firefox_desktop_downloads,
      installs.last_reported_install_target,
      installs.all_reported_install_targets,
      stub_sessn_ids.last_reported_stub_session_id,
      stub_sessn_ids.all_reported_stub_session_ids,
      lndg_pg.page_location AS landing_screen,
      campaigns.distinct_campaigns_from_event_params,
      campaigns.first_campaign_from_event_params
    FROM
      device_properties_at_session_start_event sess_strt
    JOIN
      all_ga_client_id_ga_session_ids_with_new_events_in_last_3_days sessions_to_update
      USING (ga_client_id, ga_session_id)
    LEFT JOIN
      event_aggregates evnt
      USING (ga_client_id, ga_session_id)
    LEFT JOIN
      all_stub_session_ids stub_sessn_ids
      USING (ga_client_id, ga_session_id)
    LEFT JOIN
      landing_page_by_session lndg_pg
      USING (ga_client_id, ga_session_id)
    LEFT JOIN
      all_install_targets installs
      USING (ga_client_id, ga_session_id)
    LEFT JOIN
      click_aggregate clicks
      USING (ga_client_id, ga_session_id)
    LEFT JOIN
      campaigns_by_session campaigns
      USING (ga_client_id, ga_session_id)
  ) S
  ON T.ga_client_id = S.ga_client_id
  AND T.ga_session_id = S.ga_session_id
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (
      ga_client_id,
      ga_session_id,
      session_date,
      is_first_session,
      session_number,
      time_on_site,
      pageviews,
      country,
      region,
      city,
      campaign_id,
      campaign,
      source,
      medium,
      term,
      content,
      gclid,
      gclid_array,
      device_category,
      mobile_device_model,
      mobile_device_string,
      os,
      os_version,
      `LANGUAGE`,
      browser,
      browser_version,
      had_download_event,
      last_reported_install_target,
      all_reported_install_targets,
      last_reported_stub_session_id,
      all_reported_stub_session_ids,
      landing_screen,
      distinct_campaigns_from_event_params,
      first_campaign_from_event_params
    )
  VALUES
    (
      S.ga_client_id,
      S.ga_session_id,
      S.session_date,
      S.is_first_session,
      S.session_number,
      S.time_on_site,
      S.pageviews,
      S.country,
      S.region,
      S.city,
      S.campaign_id,
      S.campaign,
      S.source,
      S.medium,
      S.term,
      S.content,
      S.gclid,
      S.gclid_array,
      S.device_category,
      S.mobile_device_model,
      S.mobile_device_string,
      S.os,
      S.os_version,
      S.language,
      S.browser,
      S.browser_version,
      S.had_download_event,
      S.last_reported_install_target,
      S.all_reported_install_targets,
      S.last_reported_stub_session_id,
      S.all_reported_stub_session_ids,
      S.landing_screen,
      S.distinct_campaigns_from_event_params,
      S.first_campaign_from_event_params
    )
  WHEN MATCHED
THEN
  UPDATE
    SET T.ga_client_id = S.ga_client_id,
    T.ga_session_id = S.ga_session_id,
    T.session_date = S.session_date,
    T.is_first_session = S.is_first_session,
    T.session_number = S.session_number,
    T.time_on_site = S.time_on_site,
    T.pageviews = S.pageviews,
    T.country = S.country,
    T.region = S.region,
    T.city = S.city,
    T.campaign_id = S.campaign_id,
    T.campaign = S.campaign,
    T.source = S.source,
    T.medium = S.medium,
    T.term = S.term,
    T.content = S.content,
    T.gclid = S.gclid,
    T.gclid_array = S.gclid_array,
    T.device_category = S.device_category,
    T.mobile_device_model = S.mobile_device_model,
    T.mobile_device_string = S.mobile_device_string,
    T.os = S.os,
    T.os_version = S.os_version,
    T.language = S.language,
    T.browser = S.browser,
    T.browser_version = S.browser_version,
    T.had_download_event = S.had_download_event,
    T.last_reported_install_target = S.last_reported_install_target,
    T.all_reported_install_targets = S.all_reported_install_targets,
    T.last_reported_stub_session_id = S.last_reported_stub_session_id,
    T.all_reported_stub_session_ids = S.all_reported_stub_session_ids,
    T.landing_screen = S.landing_screen,
    T.distinct_campaigns_from_event_params = S.distinct_campaigns_from_event_params,
    T.first_campaign_from_event_params = S.first_campaign_from_event_params
