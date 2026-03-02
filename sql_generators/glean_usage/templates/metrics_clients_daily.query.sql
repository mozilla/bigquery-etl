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
      SUM(
        COALESCE(
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_withads_urlbar) kv
          ),
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
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_withads_system) kv
          ),
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
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_withads_reload) kv
          ),
          0
        ) + COALESCE(
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_withads_unknown) kv
          ),
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
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_content_reload) kv
          ),
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
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_content_system) kv
          ),
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
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_content_unknown) kv
          ),
          0
        ) + COALESCE(
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(metrics.labeled_counter.browser_search_content_urlbar) kv
          ),
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
      ) AS search_count_all,
      SUM(
        COALESCE(
          (
            SELECT
              SUM(kv.value)
            FROM
              UNNEST(
                ARRAY_CONCAT(
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_about_home, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_about_newtab, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_contextmenu, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_reload, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_searchbar, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_system, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_tabhistory, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_unknown, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_urlbar, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_urlbar_handoff, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_urlbar_persisted, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_urlbar_searchmode, []),
                  IFNULL(metrics.labeled_counter.browser_search_adclicks_webextension, [])
                )
              ) AS kv
          ),
          0
        )
      ) AS ad_clicks_count_all,
      ANY_VALUE(metrics.string.system_apple_model_id) AS apple_model_id,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metrics.string.search_engine_default_engine_id ORDER BY submission_timestamp ASC)
      ) AS default_search_engine,
      ANY_VALUE(metrics.string.xpcom_abi) AS xpcom_abi,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_admin_user RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_admin_user,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_default_path RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_default_path,
      ARRAY_AGG(
        metrics.string.installation_first_seen_failure_reason RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_failure_reason,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_from_msi RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_from_msi,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_install_existed RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_install_existed,
      ARRAY_AGG(
        metrics.string.installation_first_seen_installer_type RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_installer_type,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_other_inst RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_other_inst,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_other_msix_inst RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_other_msix_inst,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_profdir_existed RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_profdir_existed,
      ARRAY_AGG(
        metrics.boolean.installation_first_seen_silent RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_silent,
      ARRAY_AGG(
        metrics.string.installation_first_seen_version RESPECT NULLS
        ORDER BY
          submission_timestamp
      )[SAFE_OFFSET(0)] AS installation_first_seen_version,
      mozfun.stats.mode_last(
        ARRAY_AGG(metrics.boolean.browser_backup_scheduler_enabled ORDER BY submission_timestamp)
      ) AS browser_backup_scheduler_enabled,
      mozfun.stats.mode_last(
        ARRAY_AGG(metrics.boolean.browser_backup_archive_enabled ORDER BY submission_timestamp)
      ) AS browser_backup_archive_enabled,
      mozfun.stats.mode_last(
        ARRAY_AGG(client_info.app_display_version ORDER BY submission_timestamp)
      ) AS app_display_version,
      mozfun.stats.mode_last(
        ARRAY_AGG(JSON_VALUE(metrics.object.addons_theme.id) ORDER BY submission_timestamp)
      ) AS addons_theme_id,
      mozfun.stats.mode_last(
        ARRAY_AGG(metadata.geo.country ORDER BY submission_timestamp)
      ) AS country_code
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
