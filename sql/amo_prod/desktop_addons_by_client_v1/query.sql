WITH per_client AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    ARRAY_CONCAT(
      -- active_addons
      ARRAY_CONCAT_AGG(
        ARRAY(
          SELECT AS STRUCT
            key AS id,
            value.version
          FROM
            UNNEST(environment.addons.active_addons)
        )
      ),
      -- themes
      ARRAY_AGG(STRUCT(environment.addons.theme.id, environment.addons.theme.version))
    ) AS addons,
    udf.mode_last(array_agg(application.version)) AS app_version,
    udf.mode_last(array_agg(normalized_country_code)) AS country,
    udf.mode_last(array_agg(environment.settings.locale)) AS locale,
    udf.mode_last(array_agg(normalized_os)) AS app_os,
  FROM
    telemetry.main
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
  GROUP BY
    submission_date,
    sample_id,
    client_id
)
SELECT
  * EXCEPT (addons),
  ARRAY(
    SELECT AS STRUCT
      addon.id,
      udf.mode_last(array_agg(addon.version)) AS version,
    FROM
      UNNEST(addons) AS addon
    GROUP BY
      addon.id
  ) AS addons
FROM
  per_client
