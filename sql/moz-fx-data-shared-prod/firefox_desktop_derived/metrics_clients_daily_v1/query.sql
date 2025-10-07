SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  ANY_VALUE(metrics.uuid.legacy_telemetry_profile_group_id) AS profile_group_id,
  SUM(
    COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_withads_urlbar) kv),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_urlbar_searchmode) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_contextmenu) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_about_home) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_about_newtab) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_searchbar) kv
      ),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_withads_system) kv),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_webextension) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_tabhistory) kv
      ),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_withads_reload) kv),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_withads_unknown) kv),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_urlbar_handoff) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_withads_urlbar_persisted) kv
      ),
      0
    )
  ) AS search_with_ads_count_all,
  SUM(
    COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_about_home) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_about_newtab) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_contextmenu) kv
      ),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_content_reload) kv),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_searchbar) kv
      ),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_content_system) kv),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_tabhistory) kv
      ),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_content_unknown) kv),
      0
    ) + COALESCE(
      (SELECT SUM(kv.value) FROM UNNEST(metrics.labeled_counter.browser_search_content_urlbar) kv),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_urlbar_handoff) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_urlbar_persisted) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_urlbar_searchmode) kv
      ),
      0
    ) + COALESCE(
      (
        SELECT
          SUM(kv.value)
        FROM
          UNNEST(metrics.labeled_counter.browser_search_content_webextension) kv
      ),
      0
    )
  ) AS search_count_all
FROM
  `moz-fx-data-shared-prod.firefox_desktop.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
