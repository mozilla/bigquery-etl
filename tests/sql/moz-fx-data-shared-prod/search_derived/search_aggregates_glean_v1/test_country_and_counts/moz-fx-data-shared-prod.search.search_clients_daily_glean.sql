SELECT
  *,
  CASE
    WHEN sap_counts_total >= 10000
      THEN 'sap_row_ge_10000'
  END AS sap_exclusion_reason
FROM
  `{project}.{dataset}.moz_fx_data_shared_prod_search_derived_search_clients_daily_glean_v1`
