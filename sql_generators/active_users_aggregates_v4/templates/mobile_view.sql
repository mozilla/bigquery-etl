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
      segment,
      attribution_medium,
      attribution_source,
      attributed,
      city,
      country,
      distribution_id,
      first_seen_year,
      is_default_browser,
      locale,
      channel,
      os,
      os_version,
      os_version_major,
      os_version_minor,
      submission_date,
      adjust_network,
      install_source,
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
      os_grouped,
      CASE 
        WHEN distribution_id LIKE "%vivo%"
          THEN "vivo"
        ELSE "other"
      END AS partnership,
    FROM
      `{{ project_id }}.{{ app_dataset_id }}.active_users_aggregates`
  {% endfor %}
