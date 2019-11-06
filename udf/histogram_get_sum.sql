/*

Take a list of histograms of type STRUCT<key STRING, value STRING>,
extract the histogram of the given key, and return the sum value

 */
CREATE TEMP FUNCTION udf_histogram_get_sum(histogram_list ANY TYPE, target_key STRING) AS (
  (
    SELECT
      udf_json_extract_histogram(value).sum
    FROM
      UNNEST(histogram_list)
    WHERE
      key = target_key
    LIMIT
      1
  )
);
