WITH grouped AS (
  SELECT
    addons.key AS addon_id,
    addons.value.name AS addon_name,
    COUNT(DISTINCT client_id) AS occurrences
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  CROSS JOIN
    UNNEST(environment.addons.active_addons) AS addons
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2
),
windowed AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY addon_id ORDER BY occurrences DESC) AS n_,
    *
  FROM
    grouped
)
SELECT
  * EXCEPT (n_)
FROM
  windowed
WHERE
  n_ = 1
  AND occurrences > 100
ORDER BY
  occurrences DESC
