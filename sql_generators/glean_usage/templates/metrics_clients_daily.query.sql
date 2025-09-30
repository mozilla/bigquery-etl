{% for (dataset, channel) in datasets -%}
  {% if not loop.first -%}
    UNION ALL
  {% endif -%}
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    sample_id,
    {% if app_name == "fenix" -%}
      mozfun.norm.fenix_app_info(
        "{{ dataset }}",
        client_info.app_build
      ).channel AS normalized_channel,
    {% else -%}
      "{{ channel }}" AS normalized_channel,
    {% endif -%}
    COUNT(*) AS n_metrics_ping,
    1 AS days_sent_metrics_ping_bits,
    {% if app_name in metrics -%}
      {% for metric in metrics[app_name] -%}
        {{ metrics[app_name][metric].sql }} AS {{ metric }},
      {% endfor -%}
    {% endif -%}
    {% if app_name == "firefox_desktop" -%}
      ANY_VALUE(metrics.uuid.legacy_telemetry_profile_group_id) AS profile_group_id,
      SUM(COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_urlbar) kv), 0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_urlbar_searchmode) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_contextmenu) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_about_home) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_about_newtab) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_searchbar) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_system) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_webextension) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_tabhistory) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_reload) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_unknown) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_urlbar_handoff) kv),0) + 
      COALESCE((SELECT SUM(kv.value) FROM unnest(metrics.labeled_counter.browser_search_withads_urlbar_persisted) kv),0)) AS search_with_ads_count_all
    {% endif -%}
  FROM
    `moz-fx-data-shared-prod.{{ dataset }}.metrics` AS m
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    client_id,
    sample_id,
    normalized_channel
{% endfor -%}
