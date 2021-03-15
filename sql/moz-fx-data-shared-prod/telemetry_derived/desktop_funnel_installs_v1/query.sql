CREATE TEMP FUNCTION get_ua_attribution(input STRING)
RETURNS STRING
LANGUAGE js
AS
  """
    if (input == null) {
      return 'Unknown';
    }

    try {    
      pt1 = input.split('26ua%3D')[1];
      pt2 = pt1.split('%')[0];
      return pt2;
    } catch {
        return 'Unknown';
    }
    """;

WITH distinct_countries AS (
  -- Some country codes appear multiple times as some countries have multiple names.
  -- Ensure that each code appears only once and go with name that appears first.
  SELECT
    code,
    name
  FROM
    (
      SELECT
        row_number() OVER (PARTITION BY code ORDER BY name) AS rn,
        code,
        name
      FROM
        `moz-fx-data-derived-datasets`.static.country_names_v1 country_names
    )
  WHERE
    rn = 1
)
SELECT
  DATE(submission_timestamp) AS date,
  country_names.name AS country_name,
  mozfun.norm.truncate_version(os_version, "minor") AS os_version,
  build_channel,
  build_id,
  silent,
  succeeded,
  get_ua_attribution(attribution) AS ua_attribution,
  COUNTIF(had_old_install = FALSE) AS successful_new_installs,
  COUNTIF(had_old_install = TRUE) AS successful_paveovers
FROM
  firefox_installer.install
LEFT JOIN
  distinct_countries country_names
ON
  (country_names.code = normalized_country_code)
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  date,
  os_version,
  build_channel,
  build_id,
  country_name,
  silent,
  succeeded,
  ua_attribution
