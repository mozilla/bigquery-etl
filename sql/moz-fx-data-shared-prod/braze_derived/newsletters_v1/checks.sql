-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.newsletters_v1`,
    UNNEST(newsletters) AS newsletters
  WHERE
    newsletters.update_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 HOUR)
) > 0
AS
  "No new records in the braze_derived.newsletters_v1 table in the last 15 hours";

-- macro checks

#fail
{{ not_null(["external_id"]) }} -- to do: add array values

#fail
{{ is_unique(["external_id"]) }}

#fail
{{ min_row_count(1) }}
