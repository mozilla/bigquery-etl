-- TODO:
--    2 new tables + 1 view:
--    table for mobile, table for Desktop, view to unify mobile + desktop.
--  Benefits:
--    DE standard processes
--    Easier to maintain.
--    Better performance as data is materialized.
--    Managed backfills process.
--    The definition of active user resides in  two tables.
--    Lower cost as data is calculated once instead of everytime is queried.
--  Trade-offs:
--    Requires backfills but we have the Managed Backfills process in development.
WITH _current AS (
    SELECT
        submission_date,
        client_id,
        normalized_app_id,
        CAST(durations > 0 AND isp != 'BrowserStack' AS INT64) AS days_active_user_bits -- TODO: This is where the definition of active would live.
    FROM
      `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
    WHERE
        {% if is_init() %}
          DATE(submission_timestamp) >= '2021-01-01'
        {% else %}
          DATE(submission_timestamp) = @submission_date
        {% endif %}
--     UNION ALL Other Mobile apps
)
, _previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fenix_derived.active_users_mobile_v1`
)
SELECT
    @submission_date AS submission_date,
    IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
        udf.combine_adjacent_days_28_bits(
            _previous.days_active_user_bits,
            _current.days_active_user_bits
        ) AS days_active_user_bits
    )
FROM
  _current
FULL JOIN
  _previous
USING (client_id)
