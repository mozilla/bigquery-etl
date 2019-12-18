CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_rfm` AS
SELECT
  STRUCT(
    udf.bits_to_days_seen_int(days_seen_bytes) AS frequency,
    udf.bits_to_first_seen_int(days_seen_bytes) AS T,
    udf.bits_to_first_seen_int(days_seen_bytes)
        - udf.bits_to_last_seen_int(days_seen_bytes) AS recency
  ) AS days_seen,
  STRUCT(
    udf.bits_to_days_seen_int(days_searched_bytes) AS frequency,
    udf.bits_to_first_seen_int(days_searched_bytes) AS T,
    udf.bits_to_first_seen_int(days_searched_bytes)
        - udf.bits_to_last_seen_int(days_searched_bytes) AS recency
  ) AS days_searched,
  STRUCT(
    udf.bits_to_days_seen_int(days_tagged_searched_bytes) AS frequency,
    udf.bits_to_first_seen_int(days_tagged_searched_bytes) AS T,
    udf.bits_to_first_seen_int(days_tagged_searched_bytes)
        - udf.bits_to_last_seen_int(days_tagged_searched_bytes) AS recency
  ) AS days_tagged_searched,
  STRUCT(
    udf.bits_to_days_seen_int(days_searched_with_ads_bytes) AS frequency,
    udf.bits_to_first_seen_int(days_searched_with_ads_bytes) AS T,
    udf.bits_to_first_seen_int(days_searched_with_ads_bytes)
        - udf.bits_to_last_seen_int(days_searched_with_ads_bytes) AS recency
  ) AS days_searched_with_ads,
  STRUCT(
    udf.bits_to_days_seen_int(days_clicked_ads_bytes) AS frequency,
    udf.bits_to_first_seen_int(days_clicked_ads_bytes) AS T,
    udf.bits_to_first_seen_int(days_clicked_ads_bytes)
        - udf.bits_to_last_seen_int(days_clicked_ads_bytes) AS recency
  ) AS days_clicked_ads,
  udf.bits_to_first_seen_int(days_created_profile_bytes) AS days_since_created_profile,
  * EXCEPT (
    days_seen_bytes,
    days_searched_bytes,
    days_tagged_searched_bytes,
    days_searched_with_ads_bytes,
    days_clicked_ads_bytes,
    days_created_profile_bytes    
  ),
  
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_last_seen_v1`
