  {{ header }}
WITH
  extracted AS (
  SELECT
    build.`target`.channel AS channel,
    MAX(mozfun.norm.extract_version(build.`target`.version,
        'major')) AS latest_version,
  FROM
    `moz-fx-data-shared-prod.telemetry.buildhub2`
  WHERE
    build.`source`.product = "firefox"
    AND build.`target`.channel = {{ app_id_channel }}
    AND DATE(build.build.date) <= @submission_date
  GROUP BY
    build.`target`.channel)
SELECT
  * EXCEPT (channel),
  "*" AS channel
FROM
  extracted
