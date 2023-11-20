
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
  GROUP BY
    dl_token,
    ga_client_id,
    stub_session_id
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['dl_token', 'ga_client_id', 'stub_session_id'] to be unique.)"
    ),
    NULL
  );

#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1000) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 1000 rows"
      )
    ),
    NULL
  );
