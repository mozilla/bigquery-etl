-- We "compress" by allowing only the exact dimensions values that
-- are selectable in GUD to be broken out in the aggregates.
WITH compressed AS (
  SELECT
    submission_date,
    usage,
    id_bucket,
    new_profiles,
    IF(
      country IN (
        'US',
        'DE',
        'GB',
        'FR',
        'BR',
        'RU',
        'PL',
        'CN',
        'IN',
        'IT',
        'CA',
        'ES',
        'ID',
        'KE',
        'JP'
      ),
      country,
      NULL
    ) AS country,
    substr(locale, 0, 2) AS locale,
    IF(os IN ('Windows_NT', 'Darwin', 'Linux'), os, 'Other') AS os,
    channel,
    attributed,
  FROM
    smoot_usage_new_profiles_v2
)
SELECT
  submission_date,
  usage,
  id_bucket,
  country,
  locale,
  os,
  channel,
  attributed,
  SUM(new_profiles) AS new_profiles
FROM
  compressed
WHERE
  (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  usage,
  id_bucket,
  country,
  locale,
  os,
  channel,
  attributed
