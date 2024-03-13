-- TODO:
--    1 new view + 1 UDF
--    View to collect activity for all apps
--    UDF to calculate active user bits pattern.
--  Benefits:
--    No backfills required unless the view is materialized.
--    The definition lives in one file for all apps.
--  Trade-offs:
--    Introduces a non-standard way to calculate bits pattern, which is non standard and therefore harder to maintain.
--    Lower performance as data is calculated on every run and not materialized.
--    Higher cost: On average 30 new experiments run and would be re-calculating this data. See https://mozilla.cloud.looker.com/dashboards/1540?Date=90%20day
--    Reduced complexity of the lineage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.active_users_view`
AS
-- DECLARE period INT64 DEFAULT 6;

-- TODO: Function to UDF
CREATE TEMP FUNCTION get_active_user_bits(arr ARRAY<STRUCT< submission_date DATE, active_user BOOL>>)
RETURNS ARRAY<STRUCT< submission_date DATE, active_user BOOL, bit_string STRING, days_active_user_bits INT64>>
LANGUAGE js
  OPTIONS ()
AS r"""
function datediff(first, second) {
    return Math.round((second - first) / (1000 * 60 * 60 * 24));
}
const size = 5;
arr.sort((a, b) => b.submission_date - a.submission_date);

for (let i = arr.length-1; i >= 0; i--) {
    active_user_bit = arr[i].active_user === true ? 1 : 0;
    if (i === arr.length - 1) {
        arr[i].bit_string = "0".repeat(size-1).concat(active_user_bit);
    } else {
        padding = "0".repeat(Math.min(size, datediff(arr[i+1].submission_date, arr[i].submission_date)) - 1);
        arr[i].bit_string = arr[i+1].bit_string.concat(padding).concat(active_user_bit).slice(-size);
    }
    arr[i].days_active_user_bits = parseInt(arr[i].bit_string, 2)
}
return arr;
"""
;

WITH active_users AS
(
   SELECT
        DATE(submission_date) AS submission_date,
        client_id,
        normalized_app_id,
        CAST((durations > 0 AND isp != 'BrowserStack') AS INTEGER) AS is_dau -- TODO: This is where the definition of active would live.
    FROM
      `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
    WHERE
        DATE(submission_date) BETWEEN '2023-01-01' AND '2023-12-31'
)
,bits AS (
  SELECT client_id,
        get_active_user_bits(ARRAY_AGG(STRUCT(submission_date, is_dau = 1))) AS x
  FROM active_users
  GROUP BY client_id
)
SELECT client_id,
       submission_date,
       days_active_user_bits,
       mozfun.bits28.days_since_seen(days_active_user_bits) AS days_since_active,
       mozfun.bits28.days_since_seen(days_active_user_bits) = 0 AS is_dau,
       mozfun.bits28.days_since_seen(days_active_user_bits) < 7 AS is_wau,
       mozfun.bits28.days_since_seen(days_active_user_bits) < 28 AS is_mau
  FROM bits, UNNEST(x)
