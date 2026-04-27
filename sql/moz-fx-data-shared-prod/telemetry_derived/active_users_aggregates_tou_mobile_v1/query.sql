{% for mobile_app_name in ["firefox_ios", "fenix"] %}
  SELECT
    TIMESTAMP(au.submission_date) AS submission_date,
    "{{ mobile_app_name }}" AS mobile_app_name,
    au.first_seen_year,
    au.channel,
    au.app_name,
    au.country,
    au.locale,
    au.os,
    au.os_version,
    au.os_version_major,
    au.os_version_minor,
    au.windows_build_number,
    au.app_version,
    au.app_version_major,
    au.app_version_minor,
    au.app_version_patch_revision,
    au.app_version_is_major_release,
    au.is_default_browser,
    au.distribution_id,
    au.activity_segment,
  /* Base DAU/WAU/MAU */
    COUNTIF(au.is_daily_user) AS daily_users,
    COUNTIF(au.is_weekly_user) AS weekly_users,
    COUNTIF(au.is_monthly_user) AS monthly_users,
    COUNTIF(au.is_dau) AS dau,
    COUNTIF(au.is_wau) AS wau,
    COUNTIF(au.is_mau) AS mau,
  /* ToU DAU/WAU/MAU (requires join) */
    COUNT(
      DISTINCT
      CASE
        WHEN tou.client_id IS NOT NULL
          AND au.is_dau
          THEN au.client_id
      END
    ) AS tou_daily_active_users,
    COUNT(
      DISTINCT
      CASE
        WHEN tou.client_id IS NOT NULL
          AND au.is_wau
          THEN au.client_id
      END
    ) AS tou_weekly_active_users,
    COUNT(
      DISTINCT
      CASE
        WHEN tou.client_id IS NOT NULL
          AND au.is_mau
          THEN au.client_id
      END
    ) AS tou_monthly_active_users
  FROM
    `moz-fx-data-shared-prod.{{ mobile_app_name }}.active_users` au
  LEFT JOIN
    `moz-fx-data-shared-prod.{{ mobile_app_name }}.terms_of_use_status` tou
    ON au.client_id = tou.client_id
    AND DATE(au.submission_date) >= DATE(tou.submission_date)
  WHERE
    au.is_mobile
    AND au.submission_date = @submission_date
  GROUP BY
    submission_date,
    mobile_app_name,
    au.first_seen_year,
    au.channel,
    au.app_name,
    au.country,
    au.locale,
    au.os,
    au.os_version,
    au.os_version_major,
    au.os_version_minor,
    au.windows_build_number,
    au.app_version,
    au.app_version_major,
    au.app_version_minor,
    au.app_version_patch_revision,
    au.app_version_is_major_release,
    au.is_default_browser,
    au.distribution_id,
    au.activity_segment
    {% if not loop.last %}
      UNION ALL
    {% endif %}
{% endfor %}
