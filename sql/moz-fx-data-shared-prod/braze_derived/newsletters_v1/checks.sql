-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  WITH extract_timestamp AS (
    SELECT
      TO_JSON_STRING(payload.newsletters_v1[0].update_timestamp) AS extracted_time
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_newsletters_sync_v1`
  ),
-- Retrieves the maximum newsletter updated timestamp from the last run to only
-- select recently changed records
  max_update AS (
    MAX(
      SELECT
        TIMESTAMP(mozfun.datetime_util.braze_parse_time(extracted_time))
    ) AS latest_newsletter_updated_at
    FROM
      extract_timestamp
  )
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.newsletters_v1`,
    UNNEST(newsletters) AS newsletters,
    max_update
  WHERE
    newsletters.update_timestamp > max_update.latest_newsletter_updated_at
) > 0;

-- macro checks

#fail
{{ not_null(["external_id"]) }} -- to do: add array values

#fail
{{ is_unique(["external_id"]) }}

#fail
{{ min_row_count(1) }}
