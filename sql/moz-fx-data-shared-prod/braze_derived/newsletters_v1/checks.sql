

#fail
ASSERT(
  WITH max_update AS (
    SELECT
      MAX(
        TIMESTAMP(JSON_VALUE(payload.newsletters_v1[0].update_timestamp, '$."$time"'))
      ) AS latest_newsletter_updated_at
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_newsletters_sync_v1`
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

