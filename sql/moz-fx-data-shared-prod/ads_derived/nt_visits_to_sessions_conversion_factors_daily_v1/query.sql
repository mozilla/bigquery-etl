WITH activity_stream AS (
  SELECT
      DATE(submission_timestamp) AS submission_date,
      normalized_country_code  AS country,
      COUNT(*) AS as_sessions,
      COUNT(DISTINCT client_id ) AS as_clients
  FROM `mozdata.activity_stream.sessions`
  WHERE
    (metadata.geo.country ) IN (
      'AU',
      'BR',
      'CA',
      'DE',
      'ES',
      'FR',
      'GB',
      'IN',
      'IT',
      'JP',
      'MX',
      'US'
      )
    AND ((page ) IN ('about:home', 'about:newtab')
    AND (user_prefs ) >= 258)
    AND `mozfun`.norm.browser_version_info(version).major_version >= 91
    AND DATE(submission_timestamp) = @submission_date
  GROUP BY
      1, 2
),
newtab_visits AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_country_code AS country,
    COUNT(DISTINCT metrics.uuid.legacy_telemetry_client_id) AS newtab_clients,
    COUNT(DISTINCT mozfun.map.get_key(e.extra, "newtab_visit_id")) AS newtab_visits
  FROM
    `mozdata.firefox_desktop.newtab` nt
  CROSS JOIN UNNEST(nt.events) e
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND nt.metrics.boolean.topsites_enabled
    AND nt.metrics.boolean.topsites_sponsored_enabled
    AND normalized_country_code IN (
      'AU',
      'BR',
      'CA',
      'DE',
      'ES',
      'FR',
      'GB',
      'IN',
      'IT',
      'JP',
      'MX',
      'US'
      )
    AND
      (
        (
          mozfun.map.get_key(e.extra, "source") = 'about:home'
          AND nt.metrics.string.newtab_homepage_category = "enabled"
        )
        OR
        (
          mozfun.map.get_key(e.extra, "source") = 'about:newtab'
          AND nt.metrics.string.newtab_newtab_category = "enabled"
        )
      )
  GROUP BY 1, 2
)

SELECT
  a_s.submission_date,
  a_s.country,
  newtab_clients,
  as_clients,
  newtab_visits,
  as_sessions,
  ROUND(SAFE_DIVIDE(as_sessions, newtab_visits), 2) as conversion_factor
FROM newtab_visits n_s
INNER JOIN activity_stream a_s
  USING (submission_date, country)
ORDER BY 2, 1