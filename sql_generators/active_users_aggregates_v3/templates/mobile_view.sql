--- User-facing view for all mobile apps. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset_id }}.active_users_aggregates_mobile` AS
  {% for app_dataset_id in [
      fenix_dataset,
      firefox_ios_dataset,
      focus_ios_dataset,
      klar_ios_dataset,
      focus_android_dataset,
      klar_android_dataset
  ] %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
    -- As per DENG-8914, we want to use composite tables for focus products
    -- these fields do not exist in the composite view.
      {% if app_dataset_id in [
        focus_ios_dataset,
        focus_android_dataset
      ] %}
      activity_segment AS segment,
      CAST(NULL AS STRING) AS attribution_medium,
      CAST(NULL AS STRING) AS attribution_source,
      CAST(NULL AS BOOLEAN) AS attributed,
      CAST(NULL AS STRING) AS city,
      CAST(NULL AS STRING) AS locale,
      CAST(NULL AS STRING) AS adjust_network,
      CAST(NULL AS STRING) AS install_source,
      `mozfun.norm.os`(os) AS os_grouped,
      {% else %}
      segment,
      attribution_medium,
      attribution_source,
      attributed,
      city,
      locale,
      adjust_network,
      install_source,
      os_grouped,
      {% endif %}
      country,
      distribution_id,
      first_seen_year,
      is_default_browser,
      channel,
      os,
      os_version,
      os_version_major,
      os_version_minor,
      submission_date,
      daily_users,
      weekly_users,
      monthly_users,
      dau,
      wau,
      mau,
      app_name,
      app_version,
      app_version_major,
      app_version_minor,
      app_version_patch_revision,
      app_version_is_major_release,
    FROM
    -- As per DENG-8914, we want to use composite tables for focus products
    {% if app_dataset_id in [
      focus_ios_dataset,
      focus_android_dataset
    ] %}
      `{{ project_id }}.{{ app_dataset_id }}.composite_active_users_aggregates`
    {% else %}
      `{{ project_id }}.{{ app_dataset_id }}.active_users_aggregates`
    {% endif %}
  {% endfor %}
