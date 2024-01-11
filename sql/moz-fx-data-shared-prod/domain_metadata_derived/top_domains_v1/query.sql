WITH daily_tranco AS (
  SELECT
    rank,
    NET.REG_DOMAIN(domain) AS domain,
    NET.HOST(domain) AS host
  FROM
    `tranco.daily.daily`
  WHERE
    date IN (SELECT MAX(date) FROM `tranco.daily.daily`)
),
crux_country_summary AS (
  SELECT
    origin,
    NET.HOST(origin) AS host,
    NET.REG_DOMAIN(origin) AS domain,
    NET.PUBLIC_SUFFIX(origin) AS suffix,
    country_code
  FROM
    `chrome-ux-report.materialized.country_summary`
  WHERE
    yyyymm IN (SELECT MAX(yyyymm) FROM `chrome-ux-report.materialized.country_summary`)
    AND rank <= 1000
  GROUP BY
    origin,
    country_code
),
host_join AS (
  SELECT
    rank AS host_rank,
    NULL AS domain_rank,
    c.domain,
    host,
    origin,
    suffix,
    country_code
  FROM
    crux_country_summary c
  LEFT JOIN
    daily_tranco
    USING (host)
),
domain_join AS (
  SELECT
    NULL AS host_rank,
    rank AS domain_rank,
    domain,
    h.host,
    origin,
    suffix,
    country_code
  FROM
    (SELECT * FROM host_join WHERE host_rank IS NULL) h
  LEFT JOIN
    daily_tranco t
    USING (domain)
),
combined AS (
  SELECT
    *
  FROM
    host_join
  WHERE
    host_rank IS NOT NULL
  UNION ALL
  SELECT
    *
  FROM
    domain_join
)
SELECT
  DATE(@submission_date) AS submission_date,
  host_rank AS tranco_host_rank,
  domain_rank AS tranco_domain_rank,
  domain,
  host,
  origin,
  suffix,
  country_code
FROM
  combined
