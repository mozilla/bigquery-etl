CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_rfm`
AS
SELECT
  `moz-fx-data-shared-prod.udf.days_seen_bytes_to_rfm`(days_seen_bytes) AS days_seen,
  `moz-fx-data-shared-prod.udf.days_seen_bytes_to_rfm`(days_searched_bytes) AS days_searched,
  `moz-fx-data-shared-prod.udf.days_seen_bytes_to_rfm`(
    days_tagged_searched_bytes
  ) AS days_tagged_searched,
  `moz-fx-data-shared-prod.udf.days_seen_bytes_to_rfm`(
    days_searched_with_ads_bytes
  ) AS days_searched_with_ads,
  `moz-fx-data-shared-prod.udf.days_seen_bytes_to_rfm`(days_clicked_ads_bytes) AS days_clicked_ads,
  `moz-fx-data-shared-prod.udf.bits_to_days_since_first_seen`(
    days_created_profile_bytes
  ) AS days_since_created_profile,
  * EXCEPT (
    days_seen_bytes,
    days_searched_bytes,
    days_tagged_searched_bytes,
    days_searched_with_ads_bytes,
    days_clicked_ads_bytes,
    days_created_profile_bytes
  )
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_last_seen_v1`
