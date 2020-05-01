WITH top_cities_t AS (
  SELECT
    COUNT(client_id) AS dau,
    CONCAT(country, ":", geo_subdivision1, ":", geo_subdivision2, ":", city) AS geo
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date = "2020-03-01"
    AND city != "??"
  GROUP BY
    country,
    geo_subdivision1,
    geo_subdivision2,
    city
  ORDER BY
    dau DESC
  LIMIT
    1000
),
geo_t AS (
  SELECT
    client_id,
    geo
  FROM
    (
      SELECT
        client_id,
        CONCAT(country, ":", geo_subdivision1, ":", geo_subdivision2, ":", cd_t.city) AS geo,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date DESC) AS rn
      FROM
        `moz-fx-data-shared-prod.telemetry.clients_daily` AS cd_t
      INNER JOIN
        (SELECT geo FROM top_cities_t) AS top_cities_t
      ON
        CONCAT(country, ":", geo_subdivision1, ":", geo_subdivision2, ":", city) = top_cities_t.geo
      WHERE
        submission_date = @submission_date
    )
  WHERE
    rn = 1
)
SELECT
  submission_date AS date,
  AVG(active_hours_sum) AS value,
  COUNT(client_id) AS dau,
  geo_t.geo AS geo
FROM
  (
    SELECT
      active_hours_sum,
      client_id,
      submission_date,
    FROM
      `moz-fx-data-shared-prod.telemetry.clients_daily`
    WHERE
      submission_date = @submission_date
      AND attribution.content IS NOT NULL
  )
INNER JOIN
  geo_t
USING
  (client_id)
GROUP BY
  geo,
  submission_date
