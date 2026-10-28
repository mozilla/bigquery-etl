CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_clients_daily_glean`
AS
SELECT
  *,
  -- Why this row should be left out of SAP totals; NULL keeps it. One column for every rule, so a
  -- new rule adds a value rather than a column.
  --   sap_row_ge_10000: 10,000 or more SAP on one client, day, engine, partner_code and source
  --   row, which is runaway SAP. search_clients_daily_v8 drops rows with a count of 10,000 or
  --   more, so leaving these out matches legacy.
  CASE
    WHEN sap_counts_total >= 10000
      THEN 'sap_row_ge_10000'
  END AS sap_exclusion_reason
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
