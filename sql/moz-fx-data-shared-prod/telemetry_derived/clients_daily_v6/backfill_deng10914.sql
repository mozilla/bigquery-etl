WITH recomputed_attribution AS (
  SELECT
    client_id,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          environment.settings.attribution IS NOT NULL,
          STRUCT(
            environment.settings.attribution.source,
            environment.settings.attribution.medium,
            environment.settings.attribution.campaign,
            environment.settings.attribution.content,
            environment.settings.attribution.experiment,
            environment.settings.attribution.variation,
            environment.settings.attribution.dltoken,
            environment.settings.attribution.dlsource,
            environment.settings.attribution.ua,
            environment.settings.attribution.msstoresignedin
          ),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ) AS attribution
  FROM
    `moz-fx-data-shared-prod`.telemetry_stable.main_v5
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_app_name = 'Firefox'
    AND document_id IS NOT NULL
  GROUP BY
    client_id
)
-- Only `attribution.msstoresignedin` is written; every other attribution sub-field is
-- kept exactly as prod via `cd.attribution.* REPLACE (...)`. To guarantee the written
-- value belongs to the attribution prod actually stored, msstoresignedin is grafted on
-- ONLY when the recomputed attribution's other 9 fields are identical to prod's. If the
-- struct-level mode_last shifted (so the sibling fields differ), we leave msstoresignedin
-- as prod has it rather than pairing it with a different attribution combination.
SELECT
  cd.* REPLACE (
    IF(
      -- Preserve a NULL attribution exactly; do not turn it into a struct of NULLs.
      cd.attribution IS NULL,
      NULL,
      (
        SELECT AS STRUCT
          cd.attribution.* REPLACE (
            -- Take the recomputed msstoresignedin only when the other 9 attribution
            -- fields match prod exactly (NULL-safe). Otherwise keep prod's value.
            IF(
              ra.client_id IS NOT NULL
              AND TO_JSON_STRING(
                (SELECT AS STRUCT ra.attribution.* EXCEPT (msstoresignedin))
              ) = TO_JSON_STRING((SELECT AS STRUCT cd.attribution.* EXCEPT (msstoresignedin))),
              ra.attribution.msstoresignedin,
              cd.attribution.msstoresignedin
            ) AS msstoresignedin
          )
      )
    ) AS attribution
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6` AS cd
LEFT JOIN
  recomputed_attribution AS ra
  ON cd.client_id = ra.client_id
WHERE
  cd.submission_date = @submission_date
