CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_clients_last_seen_v1`
AS
SELECT
  * EXCEPT (total_searches),
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) AS days_since_seen,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_searched_bytes) AS days_since_searched,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_tagged_searched_bytes
  ) AS days_since_tagged_searched,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_searched_with_ads_bytes
  ) AS days_since_searched_with_ads,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_clicked_ads_bytes
  ) AS days_since_clicked_ad,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_created_profile_bytes
  ) AS days_since_created_profile
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_last_seen_v1`
