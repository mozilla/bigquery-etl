WITH geo_t AS (
  SELECT
    client_id,
    geo
  FROM
    (
      SELECT
        client_id,
        country AS geo,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date DESC) AS rn
      FROM
        `moz-fx-data-shared-prod.telemetry.clients_daily` AS cd_t
      WHERE
        submission_date = @submission_date
    )
  WHERE
    rn = 1
)
SELECT
  submission_date AS date,
  COUNT(client_id) AS value,
  COUNT(client_id) AS dau,
  geo_t.geo AS geo
FROM
  (
    SELECT
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
